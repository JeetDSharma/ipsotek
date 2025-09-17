import asyncio
import time
from datetime import datetime, timedelta
import requests
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from loguru import logger

from config import config
from elasticsearch_client import elasticsearch_client
from firebase_client import firebase_client
from notifications import notification_service
from event_statistics import event_statistics_service


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
    
    async def process_recent_data(self, minutes_back: int = 5, limit: Optional[int] = None) -> int:
        """Process data from the last N minutes."""
        try:
            logger.info(f"Processing data from the last {minutes_back} minutes...")
            
            # Get recent documents from Elasticsearch
            documents = elasticsearch_client.get_recent_documents(
                index_name=config.elasticsearch.index,
                minutes_back=minutes_back,
                batch_size=config.pipeline.batch_size
            )
            
            if limit is not None and limit > 0:
                documents = documents[:limit]

            if not documents:
                logger.info("No recent documents found")
                return 0
            
            logger.info(f"Found {len(documents)} recent documents")
            # Image Processing and incremental commit (also stores docs)
            try:
                token = self._fetch_image_bearer_token()
                stored_count = self._process_and_attach_images_with_incremental_commit(documents, token)
            except Exception as img_err:
                logger.error(f"Image processing failed: {img_err}")
                stored_count = 0
            
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
    
    async def process_all_data(self, limit: Optional[int] = None) -> int:
        """Process all data from Elasticsearch."""
        try:
            logger.info("Starting to process all data from Elasticsearch...")
            
            # Get all documents from Elasticsearch
            documents = elasticsearch_client.get_all_documents(
                index_name=config.elasticsearch.index,
                batch_size=config.pipeline.batch_size
            )
            
            if limit is not None and limit > 0:
                documents = documents[:limit]

            if not documents:
                logger.info("No documents found")
                return 0
            
            logger.info(f"Found {len(documents)} documents")
            # Image Processing and incremental commit (also stores docs)
            try:
                token = self._fetch_image_bearer_token()
                stored_count = self._process_and_attach_images_with_incremental_commit(documents, token)
            except Exception as img_err:
                logger.error(f"Image processing failed: {img_err}")
                stored_count = 0
            
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

    def _fetch_image_bearer_token(self) -> str:
        """Authenticate and get bearer token for image API."""
        url = config.pipeline.image_auth_url
        payload = {"username": config.pipeline.image_username, "password": config.pipeline.image_password}
        response = requests.post(url, json=payload, verify=False, timeout=15)
        response.raise_for_status()
        token = response.text.strip().strip('"')
        return token

    def _build_image_url(self, index_name: str, source_id: str) -> str:
        base_raw = config.pipeline.image_base_url or ""
        base = base_raw.splitlines()[0].strip().rstrip('/')
        encoded_index = quote(index_name, safe="")
        encoded_id = quote(source_id, safe="")
        return f"{base}/{encoded_index}/{encoded_id}?overlay=false"

    def _build_alt_image_url(self, index_name: str, source_id: str) -> str:
        """Alternative URL form that prefixes .ds- if needed."""
        if index_name.startswith('.ds-'):
            return self._build_image_url(index_name, source_id)
        return self._build_image_url(f".ds-{index_name}", source_id)

    def _process_and_attach_images(self, documents: List[Dict[str, Any]], token: str) -> None:
        """For each document, fetch its image and upload to Firebase Storage, then enrich doc."""
        headers = {"Authorization": f"Bearer {token}"}
        session = requests.Session()
        session.verify = False
        logger.info(f"Starting image processing for {len(documents)} documents")
        for doc in documents:
            try:
                index_name = doc.get("_index", "")
                source_id = doc.get("_id", "")
                if not index_name or not source_id:
                    continue
                image_url = self._build_image_url(index_name, source_id)
                resp = session.get(image_url, headers=headers, timeout=20)
                if resp.status_code == 404:
                    alt_url = self._build_alt_image_url(index_name, source_id)
                    logger.warning(f"Primary image URL 404, retrying with alt form: {alt_url}")
                    resp = session.get(alt_url, headers=headers, timeout=20)
                if resp.status_code != 200 or not resp.content:
                    logger.warning(f"No image for {index_name}/{source_id} (status {resp.status_code})")
                    continue
                content_type = resp.headers.get("Content-Type", "image/jpeg")
                ts = datetime.utcnow().strftime("%Y/%m/%d")
                dest_path = f"{config.pipeline.storage_prefix}/{ts}/{index_name}_{source_id}.jpg"
                
                # Get image_position from document source for BBOX processing
                src = doc.setdefault("_source", {})
                image_position = src.get("image_position")
                
                # Log BBOX processing info
                if image_position and "BBOX" in image_position.upper():
                    logger.info(f"Processing image with BBOX for {index_name}/{source_id}: {image_position}")
                else:
                    logger.info(f"No BBOX data found for {index_name}/{source_id}, processing original image")
                
                # Process image with BBOX rectangle if position data is available
                upload_meta = firebase_client.process_image_with_bbox(
                    resp.content, 
                    image_position, 
                    dest_path, 
                    content_type=content_type
                )
                
                if upload_meta:
                    media_url = (
                        upload_meta.get("media_url_with_token")
                        or upload_meta.get("media_url")
                    )
                    if media_url:
                        src["image_url"] = media_url
                        logger.info(f"Attached processed image URL to document {source_id}")
            except Exception as e:
                logger.error(f"Image handling failed for doc {doc.get('_id')}: {e}")
        logger.info("Image processing step complete")

    def _process_and_attach_images_with_incremental_commit(self, documents: List[Dict[str, Any]], token: str) -> int:
        batch_size = max(1, config.pipeline.batch_size)
        headers = {"Authorization": f"Bearer {token}"}
        session = requests.Session()
        session.verify = False
        logger.info(f"Starting image processing with incremental commit, batch size {batch_size}")
        staged: List[Dict[str, Any]] = []
        total_committed = 0
        batch_number = 0
        
        for doc in documents:
            try:
                index_name = doc.get("_index", "")
                source_id = doc.get("_id", "")
                if not index_name or not source_id:
                    continue
                image_url = self._build_image_url(index_name, source_id)
                logger.info(f"Fetching image for {index_name}/{source_id}")
                resp = session.get(image_url, headers=headers, timeout=20)
                if resp.status_code == 404:
                    alt_url = self._build_alt_image_url(index_name, source_id)
                    logger.warning(f"Primary image URL 404, retrying: {alt_url}")
                    resp = session.get(alt_url, headers=headers, timeout=20)
                if resp.status_code == 200 and resp.content:
                    content_type = resp.headers.get("Content-Type", "image/jpeg")
                    ts = datetime.utcnow().strftime("%Y/%m/%d")
                    dest_path = f"{config.pipeline.storage_prefix}/{ts}/{index_name}_{source_id}.jpg"
                    
                    # Get image_position from document source for BBOX processing
                    src = doc.setdefault("_source", {})
                    image_position = src.get("image_position")
                    
                    # Log BBOX processing info
                    if image_position and "BBOX" in image_position.upper():
                        logger.info(f"Processing image with BBOX for {index_name}/{source_id}: {image_position}")
                    else:
                        logger.info(f"No BBOX data found for {index_name}/{source_id}, processing original image")
                    
                    # Process image with BBOX rectangle if position data is available
                    upload_meta = firebase_client.process_image_with_bbox(
                        resp.content, 
                        image_position, 
                        dest_path, 
                        content_type=content_type
                    )
                    
                    if upload_meta:
                        media_url = (
                            upload_meta.get("media_url_with_token")
                            or upload_meta.get("media_url")
                        )
                        if media_url:
                            src["image_url"] = media_url
                            logger.info(f"Attached processed image URL to document {source_id}")
                    else:
                        logger.error(f"Upload failed for {index_name}/{source_id}")
                else:
                    logger.warning(f"No image for {index_name}/{source_id} (status {resp.status_code})")
                staged.append(doc)
                if len(staged) >= batch_size:
                    committed = firebase_client.store_documents_batch(staged, config.firebase.collection)
                    logger.info(f"Committed {len(staged)} documents to Firestore")
                    total_committed += committed
                    
                    # Send notification and update statistics after successful batch commit
                    if committed > 0:
                        batch_number += 1
                        self._send_batch_notification(committed, batch_number)
                        self._update_event_statistics()
                    
                    staged = []
            except Exception as e:
                logger.error(f"Image handling failed for doc {doc.get('_id')}: {e}")
        
        # Handle remaining staged documents
        if staged:
            committed = firebase_client.store_documents_batch(staged, config.firebase.collection)
            logger.info(f"Committed remaining {len(staged)} documents to Firestore")
            total_committed += committed
            
            # Send notification and update statistics for final batch if any documents were committed
            if committed > 0:
                batch_number += 1
                self._send_batch_notification(committed, batch_number)
                self._update_event_statistics()
        
        return total_committed
    
    def _send_batch_notification(self, batch_size: int, batch_number: int = None) -> bool:
        try:
            title = "Security Alert"
            body = f"{batch_size} new security events detected."
            
            # Send notification to online responders only
            result = notification_service.send_notification_to_responders(
                title=title,
                body=body,
                online_only=True  # Only send to online responders
            )
            
            if result.get("success_count", 0) > 0:
                logger.info(f"Sent batch notification to {result['success_count']} online responders")
                return True
            else:
                logger.warning(f"Failed to send batch notification: {result.get('error', 'No responders available')}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending batch notification: {e}")
            return False
    
    def _update_event_statistics(self) -> bool:
        """Update event statistics after new events are processed."""
        try:
            # Refresh current statistics
            stats = event_statistics_service.refresh_statistics()
            
            if stats:
                logger.info(f"Updated event statistics: {stats}")
                return True
            else:
                logger.warning("Failed to update event statistics")
                return False
                
        except Exception as e:
            logger.error(f"Error updating event statistics: {e}")
            return False
    
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
                    self.stats["processing_time_seconds"] = processing_time
                    
                    logger.info(
                        f"Pipeline cycle completed. "
                        f"Processed: {processed_count}, "
                        f"Time: {processing_time:.2f}s"
                    )
                    
                except Exception as e:
                    logger.error(f"Error in pipeline cycle: {e}")
                    self.stats["total_failed"] += 1
                    self.stats["last_error"] = str(e)
                
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
