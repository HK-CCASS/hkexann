"""
Milvus服务模块
"""

from .collection_manager import MilvusCollectionManager, get_collection_manager

__all__ = [
    "MilvusCollectionManager",
    "get_collection_manager"
]
