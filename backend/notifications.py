import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
from firebase_admin import firestore
from firebase_client import firebase_client


class NotificationService:
    """Service for sending Firebase Cloud Messaging notifications."""
    
    def __init__(self):
        self.firebase_client = firebase_client
    
    def get_responder_tokens(
        self,
        where_equal: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        online_only: bool = True
    ) -> List[str]:

        try:
            if not self.firebase_client.is_initialized:
                logger.error("Firebase client not initialized")
                return []
            
            query = self.firebase_client.db.collection("responders")
            
            if online_only:
                query = query.where(filter=firestore.FieldFilter("status", "==", "online"))
            
            if where_equal:
                for key, value in where_equal.items():
                    query = query.where(filter=firestore.FieldFilter(key, "==", value))
            
            if limit and limit > 0:
                query = query.limit(limit)
            
            snapshots = query.stream()
            tokens: List[str] = []
            
            for snap in snapshots:
                data = snap.to_dict() or {}
                token = data.get("notification_token")
                
                if isinstance(token, str) and token.strip():
                    tokens.append(token.strip())
            
            unique_tokens = list(dict.fromkeys(tokens))
            status_filter = "online " if online_only else ""
            logger.info(f"Fetched {len(unique_tokens)} {status_filter}responder tokens")
            return unique_tokens
            
        except Exception as e:
            logger.error(f"Failed to fetch responder tokens: {e}")
            return []

    def send_notification_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str
    ) -> Dict[str, Any]:
        try:
            from firebase_admin import messaging
            
            if not self.firebase_client.is_initialized:
                raise Exception("Firebase client not initialized")
            
            # Filter valid tokens
            valid_tokens = [t for t in tokens if isinstance(t, str) and t.strip()]
            
            if not valid_tokens:
                logger.warning("No valid tokens provided")
                return {"success_count": 0, "failure_count": 0, "responses": []}
            
            success_count = 0
            failure_count = 0
            responses = []
            
            for token in valid_tokens:
                try:
                    message = messaging.Message(
                        notification=messaging.Notification(title=title, body=body),
                        token=token
                    )
                    
                    # Send notification
                    message_id = messaging.send(message)
                    success_count += 1
                    responses.append({
                        "token": token[:20] + "...",  
                        "message_id": message_id
                    })
                    logger.debug(f"Sent notification to token {token[:20]}... successfully")
                    
                except Exception as token_error:
                    failure_count += 1
                    error_type = getattr(token_error, "__class__", type("Error", (), {})).__name__
                    responses.append({
                        "token": token[:20] + "...", 
                        "error": error_type,
                        "detail": str(token_error)
                    })
                    logger.warning(f"Failed to send to token {token[:20]}...: {token_error}")
                    
                    # If token is invalid, mark it for cleanup
                    if "not found" in str(token_error).lower() or "invalid" in str(token_error).lower():
                        self._mark_token_for_cleanup(token)
            
            result = {
                "success_count": success_count,
                "failure_count": failure_count,
                "responses": responses
            }
            
            logger.info(f"FCM notification sent: {success_count} success, {failure_count} failure")
            return result
            
        except Exception as e:
            logger.error(f"Failed to send FCM notifications: {e}")
            return {"success_count": 0, "failure_count": 0, "error": str(e)}

    def send_notification_to_topic(
        self,
        topic: str,
        title: str,
        body: str
    ) -> Dict[str, Any]:
        try:
            from firebase_admin import messaging
            
            if not self.firebase_client.is_initialized:
                raise Exception("Firebase client not initialized")
            
            message = messaging.Message(
                topic=topic,
                notification=messaging.Notification(title=title, body=body)
            )
            
            message_id = messaging.send(message)
            logger.info(f"Sent topic notification to '{topic}' with id {message_id}")
            return {"message_id": message_id}
            
        except Exception as e:
            logger.error(f"Failed to send topic notification: {e}")
            return {"error": str(e)}

    def send_notification_to_responders(
        self,
        title: str,
        body: str,
        batch_size: int = 500,
        where_equal: Optional[Dict[str, Any]] = None,
        online_only: bool = True
    ) -> Dict[str, Any]:
        try:
            all_tokens = self.get_responder_tokens(where_equal=where_equal, online_only=online_only)
            
            if not all_tokens:
                status_msg = "online " if online_only else ""
                logger.warning(f"No {status_msg}responder tokens found")
                return {"success_count": 0, "failure_count": 0, "responses": []}
            
            total_success = 0
            total_failure = 0
            all_responses: List[Dict[str, Any]] = []
            
            # Send in batches
            for i in range(0, len(all_tokens), max(1, batch_size)):
                chunk = all_tokens[i:i + max(1, batch_size)]
                res = self.send_notification_to_tokens(chunk, title, body)
                
                total_success += int(res.get("success_count", 0))
                total_failure += int(res.get("failure_count", 0))
                
                if isinstance(res.get("responses"), list):
                    all_responses.extend(res["responses"])
                
                # Small delay between batches to avoid overwhelming FCM
                if i + batch_size < len(all_tokens):
                    time.sleep(0.1)
            
            summary = {
                "success_count": total_success,
                "failure_count": total_failure,
                "total_tokens": len(all_tokens),
                "responses": all_responses
            }
            
            status_msg = "online " if online_only else ""
            logger.info(f"Responder notification summary: {total_success} success, {total_failure} failure out of {len(all_tokens)} {status_msg}tokens")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to send notifications to responders: {e}")
            return {"success_count": 0, "failure_count": 0, "error": str(e)}

    def _mark_token_for_cleanup(self, invalid_token: str):
        """Mark a responder with invalid token for cleanup by setting status to offline."""
        try:
            if not self.firebase_client.is_initialized:
                return
            
            # Find responder with this token and mark as offline
            docs = self.firebase_client.db.collection("responders").where(
                filter=firestore.FieldFilter("notification_token", "==", invalid_token)
            ).stream()
            
            for doc in docs:
                doc.reference.update({
                    "status": "offline",
                    "token_invalid": True,
                    "last_token_error": datetime.utcnow()
                })
                logger.info(f"Marked responder {doc.id} as offline due to invalid token")
                
        except Exception as e:
            logger.error(f"Failed to mark token for cleanup: {e}")

    def cleanup_invalid_tokens(self) -> int:
        """Manually clean up all responders with invalid tokens."""
        try:
            if not self.firebase_client.is_initialized:
                return 0
            
            # Find all responders marked with invalid tokens
            docs = self.firebase_client.db.collection("responders").where(
                filter=firestore.FieldFilter("token_invalid", "==", True)
            ).stream()
            
            cleaned_count = 0
            for doc in docs:
                doc.reference.update({
                    "status": "offline",
                    "notification_token": None,
                    "token_invalid": False,
                    "last_cleanup": datetime.utcnow()
                })
                cleaned_count += 1
                logger.info(f"Cleaned up invalid token for responder {doc.id}")
            
            logger.info(f"Cleaned up {cleaned_count} invalid tokens")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup invalid tokens: {e}")
            return 0


# Global notification service instance
notification_service = NotificationService()
