import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from loguru import logger
from firebase_admin import firestore
from firebase_client import firebase_client


class EventStatisticsService:
    
    def __init__(self):
        self.firebase_client = firebase_client
        self.events_collection = "event" 
        self.stats_collection = "event_statistics" 
    
    def calculate_event_statistics(self, date_filter: Optional[str] = None) -> Dict[str, Any]:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return {}
            
            query = self.firebase_client.db.collection(self.events_collection)
            
            # Apply date filter if provided
            if date_filter:
                query = self._apply_date_filter(query, date_filter)
            
            # Get all documents
            docs = query.stream()
            
            # Initialize counters
            stats = {
                'total': 0,
                'pending': 0,
                'accepted': 0,
                'rejected': 0,
                'done': 0,
                'last_updated': datetime.utcnow(),
                'date_filter': date_filter or 'all'
            }
            
            # Count events by status
            for doc in docs:
                data = doc.to_dict() or {}
                stats['total'] += 1
                
                status = data.get('status', 'pending').lower()
                if status in stats:
                    stats[status] += 1
                else:
                    # Handle unknown statuses
                    stats['pending'] += 1
            
            # logger.info(f"Calculated event statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate event statistics: {e}")
            return {}
    
    def _apply_date_filter(self, query, date_filter: str):
        try:
            now = datetime.utcnow()
            
            if date_filter == "today":
                start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            elif date_filter == "week":
                start_date = now - timedelta(days=7)
            elif date_filter == "month":
                start_date = now - timedelta(days=30)
            else:
                return query  # No filter applied
            
            return query.where(
                filter=firestore.FieldFilter("created_at", ">=", start_date)
            )
            
        except Exception as e:
            logger.error(f"Failed to apply date filter: {e}")
            return query
    
    def store_statistics(self, stats: Dict[str, Any], stats_id: str = "current") -> bool:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return False
            
            # Add metadata
            stats['stored_at'] = datetime.utcnow()
            stats['collection'] = self.events_collection
            
            # Store in Firestore
            doc_ref = self.firebase_client.db.collection(self.stats_collection).document(stats_id)
            doc_ref.set(stats)
            
            logger.info(f"Stored event statistics with ID: {stats_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store event statistics: {e}")
            return False
    
    def get_statistics(self, stats_id: str = "current") -> Dict[str, Any]:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return {}
            
            doc_ref = self.firebase_client.db.collection(self.stats_collection).document(stats_id)
            doc = doc_ref.get()
            
            if doc.exists:
                data = doc.to_dict()
                logger.info(f"Retrieved event statistics: {data}")
                return data
            else:
                logger.warning(f"No statistics found with ID: {stats_id}")
                return {}
                
        except Exception as e:
            logger.error(f"Failed to retrieve event statistics: {e}")
            return {}
    
    def update_event_status(self, event_id: str, new_status: str) -> bool:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return False
            
            valid_statuses = ['pending', 'accepted', 'rejected', 'done']
            if new_status.lower() not in valid_statuses:
                logger.error(f"Invalid status: {new_status}. Valid statuses: {valid_statuses}")
                return False
            
            # Update the event
            doc_ref = self.firebase_client.db.collection(self.events_collection).document(event_id)
            doc_ref.update({
                'status': new_status.lower(),
                'status_updated_at': datetime.utcnow()
            })
            
            logger.info(f"Updated event {event_id} status to: {new_status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update event status: {e}")
            return False
    
    def refresh_statistics(self, date_filter: Optional[str] = None, stats_id: str = "current") -> Dict[str, Any]:

        try:
            # Calculate current statistics
            stats = self.calculate_event_statistics(date_filter)
            
            if stats:
                # Store the statistics
                if self.store_statistics(stats, stats_id):
                    logger.info(f"Successfully refreshed statistics for {stats_id}")
                else:
                    logger.error(f"Failed to store refreshed statistics for {stats_id}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to refresh statistics: {e}")
            return {}
    
    def get_daily_statistics(self, days: int = 7) -> Dict[str, Any]:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return {}
            
            daily_stats = {}
            
            for i in range(days):
                date = datetime.utcnow() - timedelta(days=i)
                date_str = date.strftime("%Y-%m-%d")
                
                # Calculate stats for this specific day
                start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_day = start_of_day + timedelta(days=1)
                
                query = self.firebase_client.db.collection(self.events_collection).where(
                    filter=firestore.FieldFilter("created_at", ">=", start_of_day)
                ).where(
                    filter=firestore.FieldFilter("created_at", "<", end_of_day)
                )
                
                docs = query.stream()
                day_stats = {
                    'total': 0,
                    'pending': 0,
                    'accepted': 0,
                    'rejected': 0,
                    'done': 0
                }
                
                for doc in docs:
                    data = doc.to_dict() or {}
                    day_stats['total'] += 1
                    status = data.get('status', 'pending').lower()
                    if status in day_stats:
                        day_stats[status] += 1
                
                daily_stats[date_str] = day_stats
            
            logger.info(f"Retrieved daily statistics for {days} days")
            return daily_stats
            
        except Exception as e:
            logger.error(f"Failed to get daily statistics: {e}")
            return {}


# Global event statistics service instance
event_statistics_service = EventStatisticsService()
