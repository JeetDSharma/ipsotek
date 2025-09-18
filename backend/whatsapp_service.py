from typing import Dict, Any, Optional
from loguru import logger
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from config import config
import json


class WhatsAppService:
    """Service for sending WhatsApp notifications via Twilio."""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self._initialized = False
    
    def initialize(self) -> bool:
        """Initialize the Twilio client for WhatsApp."""
        try:
            if not config.whatsapp.enabled:
                logger.info("WhatsApp service is disabled")
                return False
                
            if not config.whatsapp.account_sid or not config.whatsapp.auth_token:
                logger.error("Twilio credentials not configured")
                return False
            
            if not config.whatsapp.content_sid:
                logger.error("WhatsApp content SID not configured")
                return False
            
            self.client = Client(config.whatsapp.account_sid, config.whatsapp.auth_token)
            self._initialized = True
            logger.info("WhatsApp service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize WhatsApp service: {e}")
            return False
    
    def send_whatsapp_message(self, content_variables: Dict[str, str], to_number: Optional[str] = None) -> Dict[str, Any]:
        """Send a WhatsApp message using Twilio Content API."""
        try:
            if not self._initialized:
                if not self.initialize():
                    return {"success": False, "error": "WhatsApp service not initialized"}
            
            if not self.client:
                return {"success": False, "error": "Twilio client not available"}
            
            # Use provided phone number or default from config
            recipient = to_number or config.whatsapp.to_number
            if not recipient:
                return {"success": False, "error": "No recipient WhatsApp number configured"}
            
            # Ensure WhatsApp number format
            if not recipient.startswith('whatsapp:'):
                if recipient.startswith('+'):
                    recipient = f'whatsapp:{recipient}'
                else:
                    recipient = f'whatsapp:+{recipient}'
            
            # Convert content variables to JSON string
            content_vars_json = json.dumps(content_variables)
            
            # Send WhatsApp message
            message = self.client.messages.create(
                from_=config.whatsapp.from_number,
                content_sid=config.whatsapp.content_sid,
                content_variables=content_vars_json,
                to=recipient
            )
            
            logger.info(f"WhatsApp message sent successfully. SID: {message.sid}")
            return {
                "success": True,
                "message_sid": message.sid,
                "to": recipient,
                "status": message.status
            }
            
        except TwilioRestException as e:
            # Try to get message SID if the message was created but failed
            message_sid = getattr(e, 'sid', None) or getattr(e, 'message_sid', None)
            if message_sid:
                logger.error(f"Twilio error sending WhatsApp: {e.msg} (Code: {e.code}, Message SID: {message_sid})")
            else:
                logger.error(f"Twilio error sending WhatsApp: {e.msg} (Code: {e.code})")
            
            return {
                "success": False,
                "error": f"Twilio error: {e.msg}",
                "error_code": e.code,
                "message_sid": message_sid
            }
        except Exception as e:
            logger.error(f"Unexpected error sending WhatsApp: {e}")
            return {"success": False, "error": str(e)}
    
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
            return "Crowd Detection"
        elif "intrusion" in str(event_source).lower():
            return "Intrusion Detection"
        elif "fire" in str(event_source).lower():
            return "Fire Detection"
        elif "motion" in str(event_source).lower():
            return "Motion Detection"
        
        # Default fallback
        return "Security Event"

    def _format_location_url(self, location: Any) -> str:
        """Format location data and return Google Maps URL if coordinates are available."""
        try:
            if isinstance(location, dict):
                lat = location.get("lat")
                lon = location.get("lon")
                
                if lat is not None and lon is not None:
                    # Create Google Maps URL
                    return f"https://maps.google.com/maps?q={lat},{lon}"
                else:
                    return ""
            else:
                return ""
        except Exception as e:
            logger.warning(f"Error formatting location URL: {e}")
            return ""

    def send_event_alert(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send a WhatsApp alert for a specific event using the configured template."""
        try:
            # Extract event information
            event_source = event_data.get("_source", {})
            event_id = event_data.get("_id", "Unknown")
            
            # Extract relevant fields from the event
            timestamp = event_source.get("@timestamp", "Unknown time")
            location = event_source.get("location", "Unknown location")
            event_type = self._extract_event_name(event_source)
            
            # Format location URL
            location_url = self._format_location_url(location)
            
            # Get camera info if available
            camera_info = event_source.get("camera_name") or event_source.get("camera_id") or event_source.get("source") or "Unknown"
            
            # Based on your template example, prepare content variables
            # You'll need to adjust these variable numbers (1, 2, 3, 4) based on your actual WhatsApp template
            content_variables = {
                "1": event_type,        # {{1}} - Event type
                "2": location_url,      # {{2}} - Location URL  
                "3": timestamp,         # {{3}} - Timestamp
                "4": camera_info        # {{4}} - Camera info
            }
            
            result = self.send_whatsapp_message(content_variables)
            
            if result.get("success"):
                logger.info(f"Event alert WhatsApp sent for event {event_id}")
            else:
                error_msg = f"Failed to send event alert WhatsApp for event {event_id}: {result.get('error')}"
                message_sid = result.get("message_sid")
                if message_sid:
                    error_msg += f" (Message SID: {message_sid})"
                logger.error(error_msg)
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating event alert WhatsApp: {e}")
            return {"success": False, "error": str(e)}
    
    def send_batch_alert(self, event_count: int, batch_number: Optional[int] = None) -> Dict[str, Any]:
        """Send a WhatsApp alert for a batch of events."""
        try:
            # Prepare variables for batch alert template
            # You may need a different template for batch alerts
            batch_info = f"Batch #{batch_number}" if batch_number else "New batch"
            timestamp = "Now"  # You can format this better if needed
            
            content_variables = {
                "1": str(event_count),  # Number of events
                "2": batch_info         # Batch information
            }
            
            result = self.send_whatsapp_message(content_variables)
            
            if result.get("success"):
                logger.info(f"Batch alert WhatsApp sent for {event_count} events")
            else:
                error_msg = f"Failed to send batch alert WhatsApp: {result.get('error')}"
                message_sid = result.get("message_sid")
                if message_sid:
                    error_msg += f" (Message SID: {message_sid})"
                logger.error(error_msg)
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating batch alert WhatsApp: {e}")
            return {"success": False, "error": str(e)}
    
    def test_connection(self) -> bool:
        """Test the WhatsApp connection by sending a test message."""
        try:
            if not self._initialized:
                if not self.initialize():
                    return False
            
            # Test with simple variables
            test_variables = {
                "1": "Test Event",
                "2": "https://maps.google.com/maps?q=0,0",
                "3": "2025-09-18 Test Time",
                "4": "Test Camera"
            }
            
            result = self.send_whatsapp_message(test_variables)
            return result.get("success", False)
            
        except Exception as e:
            logger.error(f"WhatsApp connection test failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the WhatsApp service."""
        return {
            "initialized": self._initialized,
            "enabled": config.whatsapp.enabled,
            "has_credentials": bool(config.whatsapp.account_sid and config.whatsapp.auth_token),
            "has_content_sid": bool(config.whatsapp.content_sid),
            "configured_recipient": config.whatsapp.to_number or "Not configured",
            "from_number": config.whatsapp.from_number or "Not configured"
        }


# Global WhatsApp service instance
whatsapp_service = WhatsAppService()
