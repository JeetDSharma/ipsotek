#!/usr/bin/env python3
"""
Test script for Twilio WhatsApp functionality.
Run this script to test if your WhatsApp setup is working correctly.
"""

import sys
from datetime import datetime
from loguru import logger
from config import config
from whatsapp_service import whatsapp_service


def test_whatsapp_configuration():
    """Test WhatsApp configuration and connection."""
    print("=" * 60)
    print("TWILIO WHATSAPP CONFIGURATION TEST")
    print("=" * 60)
    
    # Check configuration
    print(f"WhatsApp Enabled: {config.whatsapp.enabled}")
    print(f"Account SID: {config.whatsapp.account_sid[:10]}..." if config.whatsapp.account_sid else "Account SID: Not configured")
    print(f"Auth Token: {'*' * 10}..." if config.whatsapp.auth_token else "Auth Token: Not configured")
    print(f"From Number: {config.whatsapp.from_number}")
    print(f"To Number: {config.whatsapp.to_number}")
    print(f"Content SID: {config.whatsapp.content_sid}")
    print()
    
    if not config.whatsapp.enabled:
        print("❌ WhatsApp is disabled. Set WHATSAPP_ENABLED=true in your .env file")
        return False
    
    if not config.whatsapp.account_sid or not config.whatsapp.auth_token:
        print("❌ Twilio credentials not configured. Check your .env file")
        return False
    
    if not config.whatsapp.from_number:
        print("❌ WhatsApp from number not configured. Set WHATSAPP_FROM_NUMBER in your .env file")
        return False
        
    if not config.whatsapp.to_number:
        print("❌ WhatsApp to number not configured. Set WHATSAPP_TO_NUMBER in your .env file")
        return False
    
    if not config.whatsapp.content_sid:
        print("❌ WhatsApp content SID not configured. Set WHATSAPP_CONTENT_SID in your .env file")
        return False
    
    print("✅ Configuration looks good!")
    return True


def test_whatsapp_service():
    """Test WhatsApp service initialization and connection."""
    print("=" * 60)
    print("WHATSAPP SERVICE TEST")
    print("=" * 60)
    
    try:
        # Test service initialization
        print("Testing WhatsApp service initialization...")
        if whatsapp_service.initialize():
            print("✅ WhatsApp service initialized successfully")
        else:
            print("❌ WhatsApp service initialization failed")
            return False
        
        # Get service status
        status = whatsapp_service.get_status()
        print(f"Service Status: {status}")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ WhatsApp service test failed: {e}")
        return False


def test_whatsapp_connection():
    """Test actual WhatsApp message sending."""
    print("=" * 60)
    print("WHATSAPP CONNECTION TEST")
    print("=" * 60)
    
    try:
        print("Testing WhatsApp connection by sending a test message...")
        
        # Test connection
        if whatsapp_service.test_connection():
            print("✅ WhatsApp test message sent successfully!")
            print("Check your WhatsApp to confirm you received the test message.")
            return True
        else:
            print("❌ WhatsApp test message failed")
            return False
            
    except Exception as e:
        print(f"❌ WhatsApp connection test failed: {e}")
        return False


def test_event_alert():
    """Test sending an event alert via WhatsApp."""
    print("=" * 60)
    print("WHATSAPP EVENT ALERT TEST")
    print("=" * 60)
    
    try:
        # Create a sample event
        sample_event = {
            "_id": "test-event-123",
            "_index": "test-index",
            "_source": {
                "@timestamp": datetime.now().isoformat(),
                "event_type": "Crowd Detection",
                "location": {
                    "lat": 19.012470,
                    "lon": 72.830835
                },
                "camera_name": "CAM-001",
                "description": "Test event for WhatsApp integration"
            }
        }
        
        print("Sending test event alert...")
        result = whatsapp_service.send_event_alert(sample_event)
        
        if result.get("success"):
            print(f"✅ Event alert sent successfully!")
            print(f"Message SID: {result.get('message_sid')}")
            print(f"Status: {result.get('status')}")
            return True
        else:
            print(f"❌ Event alert failed: {result.get('error')}")
            if result.get('message_sid'):
                print(f"Message SID: {result.get('message_sid')}")
            return False
            
    except Exception as e:
        print(f"❌ Event alert test failed: {e}")
        return False


def test_batch_alert():
    """Test sending a batch alert via WhatsApp."""
    print("=" * 60)
    print("WHATSAPP BATCH ALERT TEST")
    print("=" * 60)
    
    try:
        print("Sending test batch alert...")
        result = whatsapp_service.send_batch_alert(5, 1)
        
        if result.get("success"):
            print(f"✅ Batch alert sent successfully!")
            print(f"Message SID: {result.get('message_sid')}")
            print(f"Status: {result.get('status')}")
            return True
        else:
            print(f"❌ Batch alert failed: {result.get('error')}")
            if result.get('message_sid'):
                print(f"Message SID: {result.get('message_sid')}")
            return False
            
    except Exception as e:
        print(f"❌ Batch alert test failed: {e}")
        return False


def main():
    """Run all WhatsApp tests."""
    print("🔧 WHATSAPP TESTING TOOL")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests = [
        ("Configuration Check", test_whatsapp_configuration),
        ("Service Initialization", test_whatsapp_service),
        ("Connection Test", test_whatsapp_connection),
        ("Event Alert Test", test_event_alert),
        ("Batch Alert Test", test_batch_alert)
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        print(f"Running {test_name}...")
        try:
            if test_func():
                passed_tests += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")
        print()
    
    print("=" * 60)
    print(f"TEST RESULTS: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! Your WhatsApp integration is working correctly.")
    else:
        print("❌ SOME TESTS FAILED. Please check the configuration and errors above.")
        print()
        print("📋 TROUBLESHOOTING TIPS:")
        print("1. Verify your WhatsApp template is approved in Twilio Console")
        print("2. Check that your WhatsApp numbers are in the correct format")
        print("3. Ensure your Content SID matches your approved template")
        print("4. Make sure your template variables match what the service is sending")
        print("5. Check Twilio Console logs for detailed error information")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
