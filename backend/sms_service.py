from typing import Dict, Any, Optional
from loguru import logger
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from config import config


class SMSService:
    """Service for sending SMS notifications via Twilio."""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self._initialized = False
    
    def initialize(self) -> bool:
        """Initialize the Twilio client."""
        try:
            if not config.twilio.enabled:
                logger.info("Twilio SMS service is disabled")
                return False
                
            if not config.twilio.account_sid or not config.twilio.auth_token:
                logger.error("Twilio credentials not configured")
                return False
            
            self.client = Client(config.twilio.account_sid, config.twilio.auth_token)
            self._initialized = True
            logger.info("Twilio SMS service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Twilio SMS service: {e}")
            return False
    
    def send_sms(self, message: str, to_phone: Optional[str] = None) -> Dict[str, Any]:
        """Send an SMS message."""
        try:
            if not self._initialized:
                if not self.initialize():
                    return {"success": False, "error": "SMS service not initialized"}
            
            if not self.client:
                return {"success": False, "error": "Twilio client not available"}
            
            # Use provided phone number or default from config
            recipient = to_phone or config.twilio.to_phone
            if not recipient:
                return {"success": False, "error": "No recipient phone number configured"}
            
            # Ensure phone number starts with +
            if not recipient.startswith('+'):
                recipient = '+' + recipient
            
            # Send SMS
            message_obj = self.client.messages.create(
                body=message,
                from_=config.twilio.from_phone,
                to=recipient
            )
            
            logger.info(f"SMS sent successfully. SID: {message_obj.sid}")
            return {
                "success": True,
                "message_sid": message_obj.sid,
                "to": recipient,
                "status": message_obj.status
            }
            
        except TwilioRestException as e:
            logger.error(f"Twilio error sending SMS: {e.msg} (Code: {e.code})")
            return {
                "success": False,
                "error": f"Twilio error: {e.msg}",
                "error_code": e.code
            }
        except Exception as e:
            logger.error(f"Unexpected error sending SMS: {e}")
            return {"success": False, "error": str(e)}
    
    def send_event_alert(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send an SMS alert for a specific event."""
        try:
            # Extract event information
            event_source = event_data.get("_source", {})
            event_id = event_data.get("_id", "Unknown")
            event_index = event_data.get("_index", "Unknown")
            
            # Extract relevant fields from the event
            timestamp = event_source.get("@timestamp", "Unknown time")
            location = event_source.get("location", "Unknown location")
            event_type = event_source.get("event_type", "Security Event")
            description = event_source.get("description", "Event detected")
            
            # Create alert message
            message = f"🚨 SECURITY ALERT 🚨\n"
            message += f"Type: {event_type}\n"
            message += f"Time: {timestamp}\n"
            message += f"Location: {location}\n"
            message += f"Description: {description}\n"
            message += f"Event ID: {event_id}"
            
            # Truncate message if too long (SMS limit is 1600 characters)
            if len(message) > 1500:
                message = message[:1500] + "..."
            
            result = self.send_sms(message)
            
            if result.get("success"):
                logger.info(f"Event alert SMS sent for event {event_id}")
            else:
                logger.error(f"Failed to send event alert SMS for event {event_id}: {result.get('error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating event alert SMS: {e}")
            return {"success": False, "error": str(e)}
    
    def send_batch_alert(self, event_count: int, batch_number: Optional[int] = None) -> Dict[str, Any]:
        """Send an SMS alert for a batch of events."""
        try:
            message = f"🚨 SECURITY BATCH ALERT 🚨\n"
            message += f"{event_count} new security events detected"
            
            if batch_number:
                message += f" (Batch #{batch_number})"
            
            message += f"\nTime: {logger._core.handlers[0]._sink._write.__self__.name if hasattr(logger, '_core') else 'Now'}"
            message += "\nCheck the system for details."
            
            result = self.send_sms(message)
            
            if result.get("success"):
                logger.info(f"Batch alert SMS sent for {event_count} events")
            else:
                logger.error(f"Failed to send batch alert SMS: {result.get('error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating batch alert SMS: {e}")
            return {"success": False, "error": str(e)}
    
    def test_connection(self) -> bool:
        """Test the Twilio connection by sending a test message."""
        try:
            if not self._initialized:
                if not self.initialize():
                    return False
            
            test_message = "Test message from your security monitoring system. SMS alerts are working correctly."
            result = self.send_sms(test_message)
            
            return result.get("success", False)
            
        except Exception as e:
            logger.error(f"SMS connection test failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the SMS service."""
        return {
            "initialized": self._initialized,
            "enabled": config.twilio.enabled,
            "has_credentials": bool(config.twilio.account_sid and config.twilio.auth_token),
            "configured_recipient": config.twilio.to_phone or "Not configured"
        }


# Global SMS service instance
sms_service = SMSService()
