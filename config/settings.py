"""
系统配置管理模块

这个模块负责HKEX多Agent分析系统的所有配置管理。
使用Pydantic Settings提供类型安全的配置管理，支持：
- 环境变量自动加载（.env文件）
- 配置验证和类型转换
- 默认值设置
- 多种服务的连接配置

主要配置项：
- SiliconFlow API: LLM和嵌入模型服务
- ClickHouse: 时序数据存储
- Milvus: 向量数据库
- Redis: 缓存和会话存储
- API服务器: FastAPI配置
- Agent系统: 多Agent运行参数
- 数据路径: PDF和CSV数据位置

配置加载优先级：
1. 环境变量
2. .env文件
3. 默认值

作者: HKEX分析团队
版本: 2.0.0
"""

from pathlib import Path
from typing import Optional, List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    系统配置类 - 所有配置项的集中管理
    
    这个类继承自Pydantic BaseSettings，提供类型安全的配置管理。
    所有配置项都有合理的默认值，可以通过环境变量覆盖。
    
    配置分组：
    1. SiliconFlow API: LLM服务配置
    2. ClickHouse: 时序数据库配置  
    3. Milvus: 向量数据库配置
    4. Redis: 缓存数据库配置
    5. API: Web服务配置
    6. Agent: 多Agent系统配置
    7. 向量集合: 知识库配置
    8. 监控: 系统监控配置
    9. 数据路径: 文件存储配置
    
    Environment Variables:
        所有配置项都可以通过环境变量设置，变量名为大写的字段名
        例如：SILICONFLOW_API_KEY, CLICKHOUSE_HOST 等
        
    Note:
        生产环境应通过环境变量设置敏感信息如API密钥、数据库密码等
    """

    # SiliconFlow API 配置 - LLM和嵌入模型服务
    siliconflow_api_key: str = "sk-fxokmtmmiopvgzzpqixdlqbtqkrvpqfkocyqdlwcjadiipup"
    siliconflow_api_base: str = "https://api.siliconflow.cn/v1"
    siliconflow_embedding_model: str = "Qwen/Qwen3-Embedding-8B"
    siliconflow_chat_model: str = "deepseek-ai/DeepSeek-V3"

    # ClickHouse 配置 - 时序数据存储
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8124  # HTTP端口，与Docker映射配置一致
    clickhouse_database: str = "hkex_analysis"
    clickhouse_user: str = "root"  # 与Docker配置一致
    clickhouse_password: str = "123456"  # 与Docker配置一致

    # Milvus 配置 - 向量数据库
    milvus_host: str = "localhost"
    milvus_port: int = 19531  # 与Docker配置一致
    milvus_user: Optional[str] = None
    milvus_password: Optional[str] = None

    # Redis 配置 - 缓存和会话存储
    redis_host: str = "localhost"
    redis_port: int = 6380
    redis_password: Optional[str] = None
    redis_db: int = 0

    # CCASS 数据库配置 - MySQL数据库连接
    ccass_host: str = "192.168.6.207"
    ccass_port: int = 3306
    ccass_user: str = "ma1688"
    ccass_password: str = "ma1688"  # 通过环境变量设置: CCASS_PASSWORD
    ccass_database: str = "ccass"
    ccass_charset: str = "utf8mb4"

    # API 配置 - FastAPI服务器设置
    api_host: str = "0.0.0.0"
    api_port: int = 8168
    api_title: str = "HKEX Multi-Agent Analysis System"
    api_version: str = "2.0.0"

    # Agent 配置 - 多Agent系统运行参数
    max_agent_iterations: int = 5  # Agent最大迭代次数
    agent_timeout: int = 120  # Agent超时时间（秒）
    embedding_dimension: int = 4096  # 嵌入向量维度

    # 重排模型配置 - Reranking功能设置
    enable_reranking: bool = True  # 是否启用重排功能
    rerank_candidate_limit: int = 30  # 候选文档数量限制
    rerank_score_weight: float = 0.7  # 重排分数权重
    rerank_timeout: int = 10  # 重排请求超时时间（秒）
    rerank_model: str = "Qwen/Qwen3-Reranker-8B"  # 重排模型名称

    # 向量集合配置 - Milvus集合设置
    pdf_embeddings_collection: str = "pdf_embeddings_v3"

    # 监控配置 - 系统监控和日志
    enable_monitoring: bool = True
    metrics_port: int = 9090
    log_level: str = "INFO"

    # 数据路径配置 - 文件存储位置（遵循HKEX标准目录结构）
    pdf_data_path: str = "hkexann/HKEX"  # PDF文档存储路径（标准结构：hkexann/HKEX/{stockcode}/...）
    csv_data_path: str = "event_csv"  # CSV数据存储路径

    class Config:
        env_file = ".env"
        case_sensitive = False


    @property
    def pdf_data_full_path(self) -> Path:
        """
        获取PDF数据存储的完整路径对象
        
        将字符串路径转换为Path对象，便于路径操作和验证。
        
        Returns:
            Path: PDF数据存储路径的Path对象
            
        Note:
            返回的Path对象支持跨平台路径操作
        """
        return Path(self.pdf_data_path)

    @property
    def csv_data_full_path(self) -> Path:
        """
        获取CSV数据存储的完整路径对象
        
        将字符串路径转换为Path对象，便于路径操作和验证。
        
        Returns:
            Path: CSV数据存储路径的Path对象
            
        Note:
            返回的Path对象支持跨平台路径操作
        """
        return Path(self.csv_data_path)

    @property
    def clickhouse_url(self) -> str:
        """
        构建ClickHouse数据库连接URL
        
        根据配置参数生成标准的ClickHouse连接字符串，
        包含用户认证和数据库选择信息。
        
        Returns:
            str: 完整的ClickHouse连接URL
            
        Format:
            clickhouse://user:password@host:port/database
            
        Example:
            "clickhouse://default:@localhost:9010/hkex_analysis"
        """
        return f"clickhouse://{self.clickhouse_user}:{self.clickhouse_password}@{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_database}"

    @property
    def redis_url(self) -> str:
        """
        构建Redis数据库连接URL
        
        根据配置参数生成标准的Redis连接字符串，
        自动处理密码认证（如果提供）。
        
        Returns:
            str: 完整的Redis连接URL
            
        Format:
            - 无密码: redis://host:port/db
            - 有密码: redis://:password@host:port/db
            
        Example:
            "redis://localhost:6379/0" 或
            "redis://:mypassword@localhost:6379/0"
        """
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"


# 全局配置实例
settings = Settings()


def get_settings() -> Settings:
    """
    获取全局配置实例
    
    返回系统的全局配置对象，用于在应用的其他部分访问配置。
    这是推荐的配置访问方式，确保整个应用使用相同的配置实例。
    
    Returns:
        Settings: 全局配置实例
        
    Example:
        >>> config = get_settings()
        >>> print(config.clickhouse_host)
        "localhost"
    """
    return settings


def validate_settings():
    """
    验证系统配置的有效性
    
    执行全面的配置验证，检查：
    1. 文件路径的存在性
    2. API密钥的有效性 
    3. 数据库连接参数
    4. 向量维度配置
    
    在系统启动前调用此函数可以及早发现配置问题，
    避免运行时错误。
    
    Returns:
        bool: 验证通过时返回True
        
    Raises:
        ValueError: 当配置验证失败时抛出，包含详细的错误信息
        
    Example:
        >>> try:
        ...     validate_settings()
        ...     print("配置验证通过")
        ... except ValueError as e:
        ...     print(f"配置错误: {e}")
    """
    errors = []

    # 验证路径存在性
    if not settings.pdf_data_full_path.exists():
        errors.append(f"PDF数据路径不存在: {settings.pdf_data_full_path}")

    if not settings.csv_data_full_path.exists():
        errors.append(f"CSV数据路径不存在: {settings.csv_data_full_path}")

    # 验证API Key
    if not settings.siliconflow_api_key or settings.siliconflow_api_key == "your-api-key":
        errors.append("SiliconFlow API Key 未配置")

    # 验证向量维度
    if settings.embedding_dimension != 4096:
        errors.append("嵌入向量维度必须为4096")

    if errors:
        raise ValueError("配置验证失败:\n" + "\n".join(errors))

    return True


if __name__ == "__main__":
    """
    配置模块测试入口点
    
    当直接运行此模块时，执行配置加载和验证测试。
    展示配置的关键信息并验证系统是否已正确配置。
    
    测试内容：
    1. 配置验证
    2. 路径信息显示
    3. 连接URL生成
    4. 知识库集合解析
    
    Usage:
        python config/settings.py
    """
    # 测试配置加载
    try:
        validate_settings()
        print("✅ 配置验证通过")
        print(f"PDF数据路径: {settings.pdf_data_full_path}")
        print(f"CSV数据路径: {settings.csv_data_full_path}")
        print(f"ClickHouse URL: {settings.clickhouse_url}")
        print("通用文档处理系统配置完成")
    except Exception as e:
        print(f"❌ 配置验证失败: {e}")
