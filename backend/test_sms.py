#!/usr/bin/env python3
"""
Test script for Twilio SMS functionality.
Run this script to test if your Twilio SMS setup is working correctly.
"""

import sys
from datetime import datetime
from loguru import logger
from config import config
from sms_service import sms_service

def test_sms_configuration():
    """Test SMS configuration and connection."""
    print("=" * 60)
    print("TWILIO SMS CONFIGURATION TEST")
    print("=" * 60)
    
    # Check configuration
    print(f"SMS Enabled: {config.twilio.enabled}")
    print(f"Account SID: {config.twilio.account_sid[:10]}..." if config.twilio.account_sid else "Account SID: Not configured")
    print(f"Auth Token: {'*' * 10}..." if config.twilio.auth_token else "Auth Token: Not configured")
    print(f"From Phone: {config.twilio.from_phone}" if hasattr(config.twilio, 'from_phone') else "From Phone: Not configured")
    print(f"To Phone: {config.twilio.to_phone}")
    print()
    
    if not config.twilio.enabled:
        print("❌ SMS is disabled. Set TWILIO_SMS_ENABLED=true in your .env file")
        return False
    
    if not config.twilio.account_sid or not config.twilio.auth_token:
        print("❌ Twilio credentials not configured. Check your .env file")
        return False
    
    if not config.twilio.to_phone:
        print("❌ Recipient phone number not configured. Set TWILIO_TO_PHONE in your .env file")
        return False
    
    print("✅ Configuration looks good!")
    return True

def test_sms_service():
    """Test SMS service initialization and connection."""
    print("\n" + "=" * 60)
    print("TWILIO SMS SERVICE TEST")
    print("=" * 60)
    
    # Test initialization
    print("Initializing SMS service...")
    if not sms_service.initialize():
        print("❌ Failed to initialize SMS service")
        return False
    
    print("✅ SMS service initialized successfully")
    
    # Test connection
    print("Testing SMS connection...")
    if not sms_service.test_connection():
        print("❌ SMS connection test failed")
        return False
    
    print("✅ SMS connection test passed")
    return True

def test_event_alert():
    """Test sending an event alert SMS."""
    print("\n" + "=" * 60)
    print("EVENT ALERT SMS TEST")
    print("=" * 60)
    
    # Create a sample event with coordinates (like your actual data)
    sample_event = {
        "_id": "test_event_123",
        "_index": "security_events",
        "_source": {
            "@timestamp": datetime.utcnow().isoformat(),
            "event_name": "Crowd Management Alert",  # More specific event name
            "location": {
                "lat": 19.01247551946565,
                "lon": 72.83084797793317
            },
            "description": "Large crowd detected in restricted area",
            "severity": "HIGH",
            "camera_name": "Main Entrance - Camera 01"
        }
    }
    
    print("Sending sample event alert...")
    result = sms_service.send_event_alert(sample_event)
    
    if result.get("success"):
        print("✅ Event alert SMS sent successfully!")
        print(f"   Message SID: {result.get('message_sid')}")
        print(f"   Recipient: {result.get('to')}")
        print(f"   Status: {result.get('status')}")
        return True
    else:
        print("❌ Failed to send event alert SMS")
        print(f"   Error: {result.get('error')}")
        return False

def test_batch_alert():
    """Test sending a batch alert SMS."""
    print("\n" + "=" * 60)
    print("BATCH ALERT SMS TEST")
    print("=" * 60)
    
    print("Sending sample batch alert...")
    result = sms_service.send_batch_alert(5, 1)
    
    if result.get("success"):
        print("✅ Batch alert SMS sent successfully!")
        print(f"   Message SID: {result.get('message_sid')}")
        print(f"   Recipient: {result.get('to')}")
        print(f"   Status: {result.get('status')}")
        return True
    else:
        print("❌ Failed to send batch alert SMS")
        print(f"   Error: {result.get('error')}")
        return False

def main():
    """Main test function."""
    print("Starting Twilio SMS Integration Tests...")
    print(f"Test started at: {datetime.now()}")
    
    # Configure logging
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")
    
    tests = [
        ("Configuration Test", test_sms_configuration),
        ("SMS Service Test", test_sms_service),
        ("Event Alert Test", test_event_alert),
        ("Batch Alert Test", test_batch_alert),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your Twilio SMS integration is working correctly.")
        print("\nYour pipeline will now send SMS alerts for each security event detected.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check your configuration.")
        print("\nRefer to env.example.twilio for configuration help.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
