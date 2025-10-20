"""
SiliconFlow 嵌入服务客户端
支持 Qwen/Qwen3-Embedding-8B 模型，生成4096维向量
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict

import httpx
from asyncio_throttle import Throttler

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingResponse:
    """嵌入响应数据类"""
    embeddings: List[List[float]]
    model: str
    usage: Dict[str, int]
    total_tokens: int


@dataclass
class RerankResult:
    """重排结果数据类"""
    index: int
    relevance_score: float
    document: str


class SiliconFlowEmbeddingClient:
    """SiliconFlow 嵌入服务客户端"""

    def __init__(self, api_key: Optional[str] = None, api_base: Optional[str] = None, model: Optional[str] = None,
                 timeout: int = 30, max_retries: int = 8, rate_limit: int = 50):  # 每秒最大请求数
        """
        初始化客户端
        
        Args:
            api_key: API密钥，默认从配置获取
            api_base: API基础URL，默认从配置获取
            model: 模型名称，默认从配置获取
            timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
            rate_limit: 速率限制（每秒请求数）
        """
        self.api_key = api_key or settings.siliconflow_api_key
        self.api_base = api_base or settings.siliconflow_api_base
        self.model = model or settings.siliconflow_embedding_model
        self.timeout = timeout
        self.max_retries = max_retries

        # 速率限制器
        self.throttler = Throttler(rate_limit=rate_limit, period=1.0)

        # HTTP客户端配置，使用更细粒度的超时设置
        timeout_config = httpx.Timeout(connect=15.0,  # 连接超时 - 增加到15秒以应对网络波动
            read=self.timeout,  # 读取超时
            write=5.0,  # 写入超时
            pool=3.0  # 连接池超时
        )

        self.client = httpx.AsyncClient(timeout=timeout_config,
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=15))

        # 缓存 - 简单的内存缓存
        self._cache: Dict[str, List[float]] = {}
        self._cache_enabled = True

        logger.info(f"初始化SiliconFlow嵌入客户端 - 模型: {self.model}, 速率限制: {rate_limit}/s")
        logger.info(
            f"网络配置 - 连接超时: 15s, 读取超时: {self.timeout}s, 最大重试: {self.max_retries}次, keepalive: 15")

    async def embed_text(self, text: str, use_cache: bool = True) -> List[float]:
        """
        嵌入单个文本
        
        Args:
            text: 要嵌入的文本
            use_cache: 是否使用缓存
            
        Returns:
            4096维向量
        """
        # 检查缓存
        if use_cache and self._cache_enabled and text in self._cache:
            logger.debug(f"从缓存获取嵌入: {text[:50]}...")
            return self._cache[text]

        # 调用批量嵌入接口
        result = await self.embed_texts([text], use_cache=use_cache)
        return result.embeddings[0]

    async def embed_texts(self, texts: List[str], batch_size: int = 100, use_cache: bool = True) -> EmbeddingResponse:
        """
        批量嵌入文本
        
        Args:
            texts: 要嵌入的文本列表
            batch_size: 批次大小
            use_cache: 是否使用缓存
            
        Returns:
            嵌入响应对象
        """
        if not texts:
            return EmbeddingResponse(embeddings=[], model=self.model, usage={}, total_tokens=0)

        # 检查缓存
        cached_embeddings = []
        uncached_texts = []
        uncached_indices = []

        if use_cache and self._cache_enabled:
            for i, text in enumerate(texts):
                if text in self._cache:
                    cached_embeddings.append((i, self._cache[text]))
                else:
                    uncached_texts.append(text)
                    uncached_indices.append(i)
        else:
            uncached_texts = texts
            uncached_indices = list(range(len(texts)))

        # 如果全部从缓存获取
        if not uncached_texts:
            logger.debug(f"全部{len(texts)}个文本从缓存获取")
            embeddings = [None] * len(texts)
            for i, embedding in cached_embeddings:
                embeddings[i] = embedding
            return EmbeddingResponse(embeddings=embeddings, model=self.model, usage={"total_tokens": 0}, total_tokens=0)

        # 分批处理未缓存的文本
        all_embeddings = [None] * len(texts)
        total_usage = {"total_tokens": 0}

        # 填充缓存的结果
        for i, embedding in cached_embeddings:
            all_embeddings[i] = embedding

        # 分批处理未缓存的文本
        for i in range(0, len(uncached_texts), batch_size):
            batch_texts = uncached_texts[i:i + batch_size]
            batch_indices = uncached_indices[i:i + batch_size]

            logger.debug(f"处理批次 {i // batch_size + 1}: {len(batch_texts)} 个文本")

            batch_response = await self._embed_batch(batch_texts)

            # 填充结果
            for j, embedding in enumerate(batch_response.embeddings):
                original_index = batch_indices[j]
                all_embeddings[original_index] = embedding

                # 更新缓存
                if use_cache and self._cache_enabled:
                    self._cache[batch_texts[j]] = embedding

            # 累计使用量
            total_usage["total_tokens"] += batch_response.usage.get("total_tokens", 0)

        return EmbeddingResponse(embeddings=all_embeddings, model=self.model, usage=total_usage,
            total_tokens=total_usage["total_tokens"])

    async def _embed_batch(self, texts: List[str]) -> EmbeddingResponse:
        """
        处理单个批次的嵌入请求
        
        Args:
            texts: 文本列表
            
        Returns:
            嵌入响应
        """
        url = f"{self.api_base.rstrip('/')}/embeddings"

        payload = {"model": self.model, "input": texts, "encoding_format": "float"}

        for attempt in range(self.max_retries + 1):
            try:
                # 应用速率限制
                async with self.throttler:
                    response = await self.client.post(url, json=payload)
                    response.raise_for_status()

                    data = response.json()

                    # 解析响应
                    embeddings = []
                    for item in data["data"]:
                        embedding = item["embedding"]
                        # 验证维度
                        if len(embedding) != 4096:
                            raise ValueError(f"期望4096维向量，但得到{len(embedding)}维")
                        embeddings.append(embedding)

                    usage = data.get("usage", {})

                    logger.debug(f"成功嵌入 {len(texts)} 个文本，使用 {usage.get('total_tokens', 0)} tokens")

                    return EmbeddingResponse(embeddings=embeddings, model=data.get("model", self.model), usage=usage,
                        total_tokens=usage.get("total_tokens", 0))

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # 速率限制
                    wait_time = min(2 ** attempt, 60)  # 指数退避，最大60秒
                    logger.warning(f"遇到速率限制，等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                elif e.response.status_code >= 500:  # 服务器错误
                    wait_time = min(2 ** attempt, 30)
                    logger.warning(f"服务器错误 {e.response.status_code}，等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"HTTP错误 {e.response.status_code}: {e.response.text}")
                    raise

            except (httpx.RequestError, asyncio.TimeoutError) as e:
                wait_time = min(0.5 + attempt, 8)  # 更快的重试：0.5,1.5,2.5,3.5...最大8秒  
                error_detail = str(e) if str(e) else f"{type(e).__name__}"
                error_type = "连接失败" if isinstance(e, httpx.ConnectError) else "读取超时" if isinstance(e,
                                                                                                           asyncio.TimeoutError) else "网络错误"
                logger.warning(f"{error_type}({error_detail})，等待 {wait_time:.1f}s 后第 {attempt + 2} 次尝试...")
                await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"嵌入请求失败: {e}")
                raise

        raise Exception(f"嵌入请求失败，已重试 {self.max_retries} 次")

    async def rerank_documents(self, query: str, documents: List[str], model: str = "Qwen/Qwen3-Reranker-8B",
                               top_k: int = None) -> List[RerankResult]:
        """
        重排文档
        
        Args:
            query: 查询文本
            documents: 候选文档列表
            model: 重排模型名称
            top_k: 返回Top-K结果，None表示返回全部
            
        Returns:
            重排结果列表，按相关性分数降序排列
        """
        # 参数验证
        if not query or not query.strip():
            logger.warning("重排查询为空，跳过重排")
            return []

        if not documents:
            return []

        # 过滤空文档和None值，限制文档长度
        valid_documents = []
        for doc in documents:
            if doc and isinstance(doc, str) and doc.strip():
                cleaned_doc = doc.strip()
                # 限制单个文档长度，避免API错误
                if len(cleaned_doc) > 1500:
                    cleaned_doc = cleaned_doc[:1500] + "..."
                valid_documents.append(cleaned_doc)

        if not valid_documents:
            logger.warning("所有候选文档无效，跳过重排")
            return []

        # 验证top_k参数
        if top_k is not None and top_k <= 0:
            logger.warning(f"无效的top_k值: {top_k}，使用文档总数")
            top_k = len(valid_documents)

        url = f"{self.api_base.rstrip('/')}/rerank"

        payload = {"model": model, "query": query.strip(), "documents": valid_documents}

        # 只在需要时添加可选参数
        if top_k is not None and top_k < len(valid_documents):
            payload["top_k"] = top_k

        for attempt in range(self.max_retries + 1):
            try:
                # 应用速率限制
                async with self.throttler:
                    response = await self.client.post(url, json=payload)
                    response.raise_for_status()

                    data = response.json()

                    # 解析重排结果
                    results = []
                    for item in data.get("results", []):
                        result = RerankResult(index=item["index"], relevance_score=item["relevance_score"],
                            document=item.get("document", documents[item["index"]]))
                        results.append(result)

                    query_preview = query[:50] if len(query) > 50 else query
                    logger.debug(
                        f"重排完成: 查询='{query_preview}...', 文档数={len(documents)}, 返回={len(results)}个结果")
                    return results

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # 速率限制
                    wait_time = min(2 ** attempt, 60)
                    logger.warning(f"重排API遇到速率限制，等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                elif e.response.status_code == 400:  # 参数错误
                    error_msg = e.response.text
                    logger.error(f"重排API参数错误 400: {error_msg}")
                    # 对于400错误，尝试简化参数后重试一次
                    if attempt == 0 and len(documents) > 10:
                        logger.info("尝试减少文档数量后重试...")
                        documents = documents[:10]  # 减少文档数量
                        payload["documents"] = documents
                        continue
                    else:
                        # 400错误无法通过重试解决，直接返回空结果
                        logger.warning("重排API参数错误，返回空结果")
                        return []
                elif e.response.status_code >= 500:  # 服务器错误
                    wait_time = min(2 ** attempt, 30)
                    logger.warning(f"重排API服务器错误 {e.response.status_code}，等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"重排API HTTP错误 {e.response.status_code}: {e.response.text}")
                    raise

            except (httpx.RequestError, asyncio.TimeoutError) as e:
                wait_time = min(0.5 + attempt, 8)
                error_detail = str(e) if str(e) else f"{type(e).__name__}"
                error_type = "连接失败" if isinstance(e, httpx.ConnectError) else "读取超时" if isinstance(e,
                                                                                                           asyncio.TimeoutError) else "网络错误"
                logger.warning(
                    f"重排API {error_type}({error_detail})，等待 {wait_time:.1f}s 后第 {attempt + 2} 次尝试...")
                await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"重排请求失败: {e}")
                raise

        raise Exception(f"重排请求失败，已重试 {self.max_retries} 次")

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        logger.info("已清空嵌入缓存")

    def get_cache_size(self) -> int:
        """获取缓存大小"""
        return len(self._cache)

    def set_cache_enabled(self, enabled: bool):
        """设置缓存开关"""
        self._cache_enabled = enabled
        logger.info(f"嵌入缓存已{'启用' if enabled else '禁用'}")

    async def close(self):
        """关闭客户端"""
        await self.client.aclose()
        logger.info("SiliconFlow嵌入客户端已关闭")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# 全局客户端实例
_embedding_client: Optional[SiliconFlowEmbeddingClient] = None


async def get_embedding_client() -> SiliconFlowEmbeddingClient:
    """获取全局嵌入客户端实例"""
    global _embedding_client
    if _embedding_client is None:
        _embedding_client = SiliconFlowEmbeddingClient()
    return _embedding_client


async def embed_text(text: str) -> List[float]:
    """便捷函数：嵌入单个文本"""
    client = await get_embedding_client()
    return await client.embed_text(text)


async def embed_texts(texts: List[str]) -> List[List[float]]:
    """便捷函数：批量嵌入文本"""
    client = await get_embedding_client()
    response = await client.embed_texts(texts)
    return response.embeddings
