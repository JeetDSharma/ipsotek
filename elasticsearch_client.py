"""
Elasticsearch client for data ingestion.
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, AsyncGenerator
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException, NotFoundError
from loguru import logger

from config import config
from models import ElasticsearchSearchResponse, ElasticsearchHit, ElasticsearchQuery


class ElasticsearchClient:
    """Client for interacting with Elasticsearch."""
    
    def __init__(self):
        self.client: Optional[AsyncElasticsearch] = None
        self.is_connected = False
    
    async def connect(self) -> bool:
        """Establish connection to Elasticsearch."""
        try:
            connection_params = {
                "hosts": [config.get_elasticsearch_url()],
                "verify_certs": config.elasticsearch.verify_certs,
                "ssl_show_warn": False,
            }
            
            # Add authentication if provided
            if config.elasticsearch.username and config.elasticsearch.password:
                connection_params["basic_auth"] = (
                    config.elasticsearch.username,
                    config.elasticsearch.password
                )
            
            self.client = AsyncElasticsearch(**connection_params)
            
            # Test connection
            await self.client.ping()
            self.is_connected = True
            logger.info(f"Connected to Elasticsearch at {config.get_elasticsearch_url()}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            self.is_connected = False
            return False
    
    async def disconnect(self):
        """Close connection to Elasticsearch."""
        if self.client:
            await self.client.close()
            self.is_connected = False
            logger.info("Disconnected from Elasticsearch")
    
    async def health_check(self) -> bool:
        """Check Elasticsearch cluster health."""
        try:
            if not self.client:
                return False
            
            health = await self.client.cluster.health()
            logger.debug(f"Elasticsearch health: {health['status']}")
            return health["status"] in ["green", "yellow"]
            
        except Exception as e:
            logger.error(f"Elasticsearch health check failed: {e}")
            return False
    
    async def get_index_info(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Get information about an index."""
        try:
            if not self.client:
                return None
            
            info = await self.client.indices.get(index=index_name)
            return info.get(index_name)
            
        except NotFoundError:
            logger.warning(f"Index '{index_name}' not found")
            return None
        except Exception as e:
            logger.error(f"Failed to get index info for '{index_name}': {e}")
            return None
    
    async def search_documents(
        self,
        query: ElasticsearchQuery,
        scroll_timeout: str = "1m"
    ) -> List[ElasticsearchHit]:
        """Search for documents in Elasticsearch."""
        try:
            if not self.client:
                raise Exception("Elasticsearch client not connected")
            
            search_params = {
                "index": query.index,
                "body": {
                    "query": query.query,
                    "size": query.size,
                }
            }
            
            if query.sort:
                search_params["body"]["sort"] = query.sort
            
            if query.scroll:
                search_params["scroll"] = query.scroll
            
            response = await self.client.search(**search_params)
            search_response = ElasticsearchSearchResponse(**response)
            
            return search_response.get_documents()
            
        except Exception as e:
            logger.error(f"Failed to search documents: {e}")
            raise
    
    async def scroll_documents(
        self,
        scroll_id: str,
        scroll_timeout: str = "1m"
    ) -> List[ElasticsearchHit]:
        """Continue scrolling through search results."""
        try:
            if not self.client:
                raise Exception("Elasticsearch client not connected")
            
            response = await self.client.scroll(
                scroll_id=scroll_id,
                scroll=scroll_timeout
            )
            
            search_response = ElasticsearchSearchResponse(**response)
            return search_response.get_documents()
            
        except Exception as e:
            logger.error(f"Failed to scroll documents: {e}")
            raise
    
    async def get_recent_documents(
        self,
        index_name: str,
        minutes_back: int = 5,
        batch_size: int = 100
    ) -> List[ElasticsearchHit]:
        """Get documents from the last N minutes."""
        try:
            # Calculate timestamp for N minutes ago
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
            timestamp_str = cutoff_time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Build query for recent documents
            query = ElasticsearchQuery(
                index=index_name,
                query={
                    "range": {
                        "@timestamp": {
                            "gte": timestamp_str,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                },
                size=batch_size,
                sort=[{"@timestamp": {"order": "desc"}}]
            )
            
            return await self.search_documents(query)
            
        except Exception as e:
            logger.error(f"Failed to get recent documents: {e}")
            raise
    
    async def get_all_documents_stream(
        self,
        index_name: str,
        batch_size: int = 100,
        scroll_timeout: str = "1m"
    ) -> AsyncGenerator[List[ElasticsearchHit], None]:
        """Stream all documents from an index using scroll API."""
        try:
            # Initial search with scroll
            query = ElasticsearchQuery(
                index=index_name,
                query={"match_all": {}},
                size=batch_size,
                scroll=scroll_timeout
            )
            
            documents = await self.search_documents(query)
            scroll_id = None
            
            # Extract scroll_id from the response
            if documents and hasattr(documents[0], '_scroll_id'):
                scroll_id = documents[0]._scroll_id
            
            yield documents
            
            # Continue scrolling
            while documents:
                if not scroll_id:
                    break
                
                documents = await self.scroll_documents(scroll_id, scroll_timeout)
                yield documents
                
                # Update scroll_id for next iteration
                if documents and hasattr(documents[0], '_scroll_id'):
                    scroll_id = documents[0]._scroll_id
                else:
                    break
            
        except Exception as e:
            logger.error(f"Failed to stream documents: {e}")
            raise
    
    async def count_documents(self, index_name: str, query: Dict[str, Any] = None) -> int:
        """Count documents in an index."""
        try:
            if not self.client:
                raise Exception("Elasticsearch client not connected")
            
            count_params = {"index": index_name}
            if query:
                count_params["body"] = {"query": query}
            
            response = await self.client.count(**count_params)
            return response["count"]
            
        except Exception as e:
            logger.error(f"Failed to count documents: {e}")
            raise
    
    async def test_connection(self) -> bool:
        """Test the Elasticsearch connection."""
        try:
            if not self.client:
                return False
            
            await self.client.ping()
            return True
            
        except Exception as e:
            logger.error(f"Elasticsearch connection test failed: {e}")
            return False


# Global Elasticsearch client instance
elasticsearch_client = ElasticsearchClient()
