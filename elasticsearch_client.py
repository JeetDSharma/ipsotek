"""
Elasticsearch client for data ingestion (synchronous version).
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError
from loguru import logger

from config import config
from models import ElasticsearchSearchResponse, ElasticsearchHit, ElasticsearchQuery


class ElasticsearchClient:
    """Client for interacting with Elasticsearch."""
    
    def __init__(self):
        self.client: Optional[Elasticsearch] = None
        self.is_connected = False
    
    def connect(self) -> bool:
        """Establish connection to Elasticsearch."""
        try:
            connection_params = {
                "hosts": [config.get_elasticsearch_url()],
                "verify_certs": config.elasticsearch.verify_certs,
                "ssl_show_warn": False,
                "timeout": 30,
                "max_retries": 3,
                "retry_on_timeout": True,
            }
            
            # Add authentication if provided
            if config.elasticsearch.username and config.elasticsearch.password:
                connection_params["basic_auth"] = (
                    config.elasticsearch.username,
                    config.elasticsearch.password
                )
            
            self.client = Elasticsearch(**connection_params)
            
            # Test connection with a simple operation
            info = self.client.info()
            self.is_connected = True
            logger.info(f"Connected to Elasticsearch at {config.get_elasticsearch_url()}")
            logger.info(f"Elasticsearch version: {info['version']['number']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Close connection to Elasticsearch."""
        if self.client:
            self.client.close()
            self.is_connected = False
            logger.info("Disconnected from Elasticsearch")
    
    def health_check(self) -> bool:
        """Check Elasticsearch cluster health."""
        try:
            if not self.client:
                return False
            
            health = self.client.cluster.health()
            logger.debug(f"Elasticsearch health: {health['status']}")
            return health["status"] in ["green", "yellow"]
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Elasticsearch health check failed: {e}")
            return False
    
    def get_index_info(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Get information about an index."""
        try:
            if not self.client:
                return None
            
            info = self.client.indices.get(index=index_name)
            return info.get(index_name)
            
        except NotFoundError:
            logger.warning(f"Index '{index_name}' not found")
            return None
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to get index info for '{index_name}': {e}")
            return None
    
    def search_documents(self, query: ElasticsearchQuery) -> List[ElasticsearchHit]:
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
            
            response = self.client.search(**search_params)
            search_response = ElasticsearchSearchResponse(**response)
            
            return search_response.get_documents()
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to search documents: {e}")
            raise
    
    def get_recent_documents(
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
            
            return self.search_documents(query)
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to get recent documents: {e}")
            raise
    
    def get_all_documents(
        self,
        index_name: str,
        batch_size: int = 100
    ) -> List[ElasticsearchHit]:
        """Get all documents from an index using scroll API."""
        try:
            # Initial search with scroll
            query = ElasticsearchQuery(
                index=index_name,
                query={"match_all": {}},
                size=batch_size
            )
            
            # Use scroll API for large datasets
            response = self.client.search(
                index=index_name,
                body={
                    "query": {"match_all": {}},
                    "size": batch_size
                },
                scroll="1m"
            )
            
            documents = []
            scroll_id = response.get("_scroll_id")
            
            # Process first batch
            for hit in response["hits"]["hits"]:
                documents.append(ElasticsearchHit(**hit))
            
            # Continue scrolling
            while scroll_id and len(response["hits"]["hits"]) > 0:
                response = self.client.scroll(
                    scroll_id=scroll_id,
                    scroll="1m"
                )
                
                for hit in response["hits"]["hits"]:
                    documents.append(ElasticsearchHit(**hit))
                
                scroll_id = response.get("_scroll_id")
            
            return documents
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to get all documents: {e}")
            raise
    
    def count_documents(self, index_name: str, query: Dict[str, Any] = None) -> int:
        """Count documents in an index."""
        try:
            if not self.client:
                raise Exception("Elasticsearch client not connected")
            
            count_params = {"index": index_name}
            if query:
                count_params["body"] = {"query": query}
            
            response = self.client.count(**count_params)
            return response["count"]
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to count documents: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the Elasticsearch connection."""
        try:
            if not self.client:
                return False
            
            self.client.ping()
            return True
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Elasticsearch connection test failed: {e}")
            return False


# Global Elasticsearch client instance
elasticsearch_client = ElasticsearchClient()