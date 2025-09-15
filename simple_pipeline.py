"""
Simplified pipeline without Pydantic models - just raw dictionaries.
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger

from config import config
from simple_elasticsearch_client import elasticsearch_client
from simple_firebase_client import firebase_client


class SimpleDataPipeline:
    """Simplified pipeline that works with raw dictionaries."""
    
    def __init__(self):
        self.is_running = False
        self.stats = {
            "total_processed": 0,
            "total_successful": 0,
            "total_failed": 0,
            "last_run": None,
            "last_error": None,
            "processing_time_seconds": 0.0
        }
        
    async def initialize(self) -> bool:
        """Initialize the pipeline by connecting to both services."""
        try:
            logger.info("Initializing simplified data pipeline...")
            
            # Initialize Firebase
            if not firebase_client.initialize():
                logger.error("Failed to initialize Firebase client")
                return False
            
            # Connect to Elasticsearch
            if not elasticsearch_client.connect():
                logger.error("Failed to connect to Elasticsearch")
                return False
            
            # Test connections
            if not firebase_client.test_connection():
                logger.error("Firebase connection test failed")
                return False
            
            if not elasticsearch_client.test_connection():
                logger.error("Elasticsearch connection test failed")
                return False
            
            logger.info("Pipeline initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            logger.info("Cleaning up pipeline resources...")
            elasticsearch_client.disconnect()
            logger.info("Pipeline cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def process_recent_data(self, minutes_back: int = 5) -> int:
        """Process data from the last N minutes."""
        try:
            logger.info(f"Processing data from the last {minutes_back} minutes...")
            
            # Get recent documents from Elasticsearch
            documents = elasticsearch_client.get_recent_documents(
                index_name=config.elasticsearch.index,
                minutes_back=minutes_back,
                batch_size=config.pipeline.batch_size
            )
            
            if not documents:
                logger.info("No recent documents found")
                return 0
            
            logger.info(f"Found {len(documents)} recent documents")
            
            # Store documents in Firebase
            stored_count = firebase_client.store_documents_batch(
                documents=documents,
                collection_name=config.firebase.collection
            )
            
            # Update statistics
            self.stats["total_processed"] += 1
            if stored_count > 0:
                self.stats["total_successful"] += 1
                self.stats["last_error"] = None
            else:
                self.stats["total_failed"] += 1
                self.stats["last_error"] = "Failed to store documents"
            
            self.stats["last_run"] = datetime.utcnow()
            
            logger.info(f"Successfully processed {stored_count} documents")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error processing recent data: {e}")
            self.stats["total_failed"] += 1
            self.stats["last_error"] = str(e)
            return 0
    
    async def process_all_data(self) -> int:
        """Process all data from Elasticsearch."""
        try:
            logger.info("Starting to process all data from Elasticsearch...")
            
            # Get all documents from Elasticsearch
            documents = elasticsearch_client.get_all_documents(
                index_name=config.elasticsearch.index,
                batch_size=config.pipeline.batch_size
            )
            
            if not documents:
                logger.info("No documents found")
                return 0
            
            logger.info(f"Found {len(documents)} documents")
            
            # Store documents in Firebase
            stored_count = firebase_client.store_documents_batch(
                documents=documents,
                collection_name=config.firebase.collection
            )
            
            # Update statistics
            self.stats["total_processed"] += 1
            if stored_count > 0:
                self.stats["total_successful"] += 1
                self.stats["last_error"] = None
            else:
                self.stats["total_failed"] += 1
                self.stats["last_error"] = "Failed to store documents"
            
            self.stats["last_run"] = datetime.utcnow()
            
            logger.info(f"Completed processing all data. Total processed: {stored_count}")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error processing all data: {e}")
            self.stats["total_failed"] += 1
            self.stats["last_error"] = str(e)
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics."""
        success_rate = (
            self.stats["total_successful"] / self.stats["total_processed"] * 100
            if self.stats["total_processed"] > 0 else 0
        )
        
        return {
            **self.stats,
            "success_rate": success_rate,
            "is_running": self.is_running
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all components."""
        try:
            health_status = {
                "elasticsearch": elasticsearch_client.health_check(),
                "firebase": firebase_client.test_connection(),
                "pipeline": self.is_running,
                "timestamp": datetime.utcnow()
            }
            
            overall_health = all([
                health_status["elasticsearch"],
                health_status["firebase"]
            ])
            
            health_status["overall"] = overall_health
            
            return health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "elasticsearch": False,
                "firebase": False,
                "pipeline": False,
                "overall": False,
                "error": str(e),
                "timestamp": datetime.utcnow()
            }


# Global pipeline instance
data_pipeline = SimpleDataPipeline()
