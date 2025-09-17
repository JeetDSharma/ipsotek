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
            # Try to get message SID if the message was created but failed
            message_sid = getattr(e, 'sid', None) or getattr(e, 'message_sid', None)
            if message_sid:
                logger.error(f"Twilio error sending SMS: {e.msg} (Code: {e.code}, Message SID: {message_sid})")
            else:
                logger.error(f"Twilio error sending SMS: {e.msg} (Code: {e.code})")
            
            return {
                "success": False,
                "error": f"Twilio error: {e.msg}",
                "error_code": e.code,
                "message_sid": message_sid
            }
        except Exception as e:
            logger.error(f"Unexpected error sending SMS: {e}")
            return {"success": False, "error": str(e)}
    
    def _format_location_with_maps(self, location: Any) -> str:
        """Format location data and include Google Maps URL if coordinates are available."""
        try:
            if isinstance(location, dict):
                lat = location.get("lat")
                lon = location.get("lon")
                
                if lat is not None and lon is not None:
                    # Create Google Maps URL
                    maps_url = f"https://maps.google.com/maps?q={lat},{lon}"
                    return f"📍 Coordinates: {lat:.6f}, {lon:.6f}\n🗺️ Map: {maps_url}"
                else:
                    return f"📍 Location: {location}"
            elif isinstance(location, str):
                return f"📍 Location: {location}"
            else:
                return f"📍 Location: {location}"
        except Exception as e:
            logger.warning(f"Error formatting location: {e}")
            return f"📍 Location: {location}"
    
    def _extract_event_name(self, event_source: Dict[str, Any]) -> str:
        """Extract the most specific event name/type from the event data."""
        # Try multiple possible field names for event type/name
        possible_fields = [
            "event_name",
            "event_type", 
            "alert_type",
            "detection_type",
            "incident_type",
            "alarm_type",
            "category",
            "type",
            "name",
            "title"
        ]
        
        for field in possible_fields:
            value = event_source.get(field)
            if value and isinstance(value, str) and value.strip():
                return value.strip()
        
        # If no specific event type found, try to infer from other fields
        if "crowd" in str(event_source).lower():
            return "Crowd Management Alert"
        elif "intrusion" in str(event_source).lower():
            return "Intrusion Detection Alert"
        elif "fire" in str(event_source).lower():
            return "Fire Detection Alert"
        elif "motion" in str(event_source).lower():
            return "Motion Detection Alert"
        
        # Default fallback
        return "Security Event"

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
            event_type = self._extract_event_name(event_source)
            
            # Format location with Google Maps URL if coordinates available
            formatted_location = self._format_location_with_maps(location)
            
            # Create alert message
            message = f"🚨 SECURITY ALERT 🚨\n"
            message += f"📋 Type: {event_type}\n"
            message += f"⏰ Time: {timestamp}\n"
            message += f"{formatted_location}"
            
            # Add camera info if available
            camera_info = event_source.get("camera_name") or event_source.get("camera_id") or event_source.get("source")
            if camera_info:
                message += f"\n📹 Camera: {camera_info}"
            
            # Truncate message if too long (SMS limit is 1600 characters)
            if len(message) > 1500:
                message = message[:1500] + "..."
            
            result = self.send_sms(message)
            
            if result.get("success"):
                logger.info(f"Event alert SMS sent for event {event_id}")
            else:
                error_msg = f"Failed to send event alert SMS for event {event_id}: {result.get('error')}"
                message_sid = result.get("message_sid")
                if message_sid:
                    error_msg += f" (Message SID: {message_sid})"
                logger.error(error_msg)
            
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
                error_msg = f"Failed to send batch alert SMS: {result.get('error')}"
                message_sid = result.get("message_sid")
                if message_sid:
                    error_msg += f" (Message SID: {message_sid})"
                logger.error(error_msg)
            
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
