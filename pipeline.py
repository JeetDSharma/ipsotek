"""
Core pipeline logic for Elasticsearch to Firebase data transfer.
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger

from config import config
from models import PipelineStats, ElasticsearchQuery, ElasticsearchHit
from elasticsearch_client import elasticsearch_client
from firebase_client import firebase_client


class DataPipeline:
    """Main pipeline class for transferring data from Elasticsearch to Firebase."""
    
    def __init__(self):
        self.stats = PipelineStats()
        self.is_running = False
        self.last_processed_timestamp: Optional[datetime] = None
        self.scroll_id: Optional[str] = None
        
    async def initialize(self) -> bool:
        """Initialize the pipeline by connecting to both services."""
        try:
            logger.info("Initializing data pipeline...")
            
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
            stored_count = await firebase_client.store_elasticsearch_hits(
                hits=documents,
                collection_name=config.firebase.collection
            )
            
            # Update statistics
            self.stats.increment_processed()
            if stored_count > 0:
                self.stats.increment_successful()
                self.stats.set_success()
            else:
                self.stats.increment_failed()
                self.stats.set_error("Failed to store documents")
            
            logger.info(f"Successfully processed {stored_count} documents")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error processing recent data: {e}")
            self.stats.increment_failed()
            self.stats.set_error(str(e))
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
            stored_count = await firebase_client.store_elasticsearch_hits(
                hits=documents,
                collection_name=config.firebase.collection
            )
            
            # Update statistics
            self.stats.increment_processed()
            if stored_count > 0:
                self.stats.increment_successful()
                self.stats.set_success()
            else:
                self.stats.increment_failed()
                self.stats.set_error("Failed to store documents")
            
            logger.info(f"Completed processing all data. Total processed: {stored_count}")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error processing all data: {e}")
            self.stats.increment_failed()
            self.stats.set_error(str(e))
            return 0
    
    async def process_custom_query(self, query: Dict[str, Any]) -> int:
        """Process data using a custom Elasticsearch query."""
        try:
            logger.info("Processing data with custom query...")
            
            elasticsearch_query = ElasticsearchQuery(
                index=config.elasticsearch.index,
                query=query,
                size=config.pipeline.batch_size
            )
            
            documents = elasticsearch_client.search_documents(elasticsearch_query)
            
            if not documents:
                logger.info("No documents found matching the query")
                return 0
            
            logger.info(f"Found {len(documents)} documents matching the query")
            
            # Store documents in Firebase
            stored_count = await firebase_client.store_elasticsearch_hits(
                hits=documents,
                collection_name=config.firebase.collection
            )
            
            # Update statistics
            self.stats.increment_processed()
            if stored_count > 0:
                self.stats.increment_successful()
                self.stats.set_success()
            else:
                self.stats.increment_failed()
                self.stats.set_error("Failed to store documents")
            
            logger.info(f"Successfully processed {stored_count} documents")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error processing custom query: {e}")
            self.stats.increment_failed()
            self.stats.set_error(str(e))
            return 0
    
    async def run_continuous_pipeline(self):
        """Run the pipeline continuously, processing data at regular intervals."""
        try:
            logger.info("Starting continuous pipeline...")
            self.is_running = True
            
            while self.is_running:
                start_time = time.time()
                
                try:
                    # Process recent data
                    processed_count = await self.process_recent_data(
                        minutes_back=config.pipeline.polling_interval_seconds // 60 + 1
                    )
                    
                    # Calculate processing time
                    processing_time = time.time() - start_time
                    self.stats.processing_time_seconds = processing_time
                    
                    logger.info(
                        f"Pipeline cycle completed. "
                        f"Processed: {processed_count}, "
                        f"Time: {processing_time:.2f}s"
                    )
                    
                except Exception as e:
                    logger.error(f"Error in pipeline cycle: {e}")
                    self.stats.increment_failed()
                    self.stats.set_error(str(e))
                
                # Wait for next cycle
                await asyncio.sleep(config.pipeline.polling_interval_seconds)
            
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            self.is_running = False
    
    def stop_pipeline(self):
        """Stop the continuous pipeline."""
        logger.info("Stopping pipeline...")
        self.is_running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics."""
        return {
            "total_processed": self.stats.total_processed,
            "total_successful": self.stats.total_successful,
            "total_failed": self.stats.total_failed,
            "success_rate": (
                self.stats.total_successful / self.stats.total_processed * 100
                if self.stats.total_processed > 0 else 0
            ),
            "last_run": self.stats.last_run,
            "last_error": self.stats.last_error,
            "processing_time_seconds": self.stats.processing_time_seconds,
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
data_pipeline = DataPipeline()
