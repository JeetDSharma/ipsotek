from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError, RequestError
from loguru import logger

from config import config


class SimpleElasticsearchClient:
    """Simplified Elasticsearch client that works with raw dictionaries."""
    
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
    
    def get_recent_documents(
        self,
        index_name: str,
        minutes_back: int = 5,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Get documents from the last N minutes."""
        try:
            # Calculate timestamp for N minutes ago
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
            timestamp_str = cutoff_time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Build query for recent documents
            search_params = {
                "index": index_name,
                "body": {
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": timestamp_str,
                                "format": "yyyy-MM-dd'T'HH:mm:ss"
                            }
                        }
                    },
                    "size": batch_size,
                    "sort": [{"@timestamp": {"order": "desc"}}]
                }
            }
            
            response = self.client.search(**search_params)
            return response["hits"]["hits"]
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to get recent documents: {e}")
            raise
    
    def get_all_documents(
        self,
        index_name: str,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Get all documents from an index using scroll API."""
        try:
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
                documents.append(hit)
            
            # Continue scrolling
            while scroll_id and len(response["hits"]["hits"]) > 0:
                response = self.client.scroll(
                    scroll_id=scroll_id,
                    scroll="1m"
                )
                
                for hit in response["hits"]["hits"]:
                    documents.append(hit)
                
                scroll_id = response.get("_scroll_id")
            
            return documents
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to get all documents: {e}")
            raise
    
    def search_documents(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 100
    ) -> List[Dict[str, Any]]:
        """Search for documents in Elasticsearch."""
        try:
            if not self.client:
                raise Exception("Elasticsearch client not connected")
            
            search_params = {
                "index": index_name,
                "body": {
                    "query": query,
                    "size": size,
                }
            }
            
            response = self.client.search(**search_params)
            return response["hits"]["hits"]
            
        except (ConnectionError, RequestError) as e:
            logger.error(f"Failed to search documents: {e}")
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
elasticsearch_client = SimpleElasticsearchClient()
