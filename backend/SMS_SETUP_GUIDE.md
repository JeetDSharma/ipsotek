# Twilio SMS Integration Setup Guide

This guide will help you set up Twilio SMS notifications for your security event monitoring system.

## Overview

The SMS integration sends real-time alerts via SMS for every security event detected by your monitoring system. When running in continuous mode, you'll receive SMS notifications like:

```
🚨 SECURITY ALERT 🚨
Type: Crowd Management Alert
Time: 2024-01-15T10:30:45Z
Location: Main Entrance - Camera 01
Description: Large crowd detected in restricted area
Event ID: event_123456
```

## Prerequisites

1. **Twilio Account**: Sign up at [https://www.twilio.com](https://www.twilio.com)
2. **Twilio Phone Number**: Purchase a phone number from Twilio Console
3. **Account Credentials**: Get your Account SID and Auth Token from Twilio Console

## Setup Steps

### Step 1: Install Dependencies

The Twilio dependency has already been added to `requirements.txt`. Install it:

```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables

Add these variables to your `.env` file:

```bash
# Twilio Configuration
TWILIO_ACCOUNT_SID=your_account_sid_here
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_FROM_PHONE=+1234567890  # Your Twilio phone number
TWILIO_TO_PHONE=+917045660366   # Phone number to receive alerts
TWILIO_SMS_ENABLED=true
```

**Important Notes:**
- Replace the example values with your actual Twilio credentials
- Phone numbers must include country code (e.g., +1 for US, +91 for India)
- `TWILIO_FROM_PHONE` must be a verified phone number in your Twilio account

### Step 3: Test Your Setup

Run the test script to verify everything is working:

```bash
python test_sms.py
```

This will:
- ✅ Check your configuration
- ✅ Test Twilio connection
- ✅ Send a sample event alert
- ✅ Send a sample batch alert

### Step 4: Run Your Pipeline

Start your pipeline in continuous mode:

```bash
python main.py --mode continuous
```

Now you'll receive SMS alerts for every security event detected!

## Configuration Options

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `TWILIO_ACCOUNT_SID` | Your Twilio Account SID | Yes | - |
| `TWILIO_AUTH_TOKEN` | Your Twilio Auth Token | Yes | - |
| `TWILIO_FROM_PHONE` | Twilio phone number (sender) | Yes | - |
| `TWILIO_TO_PHONE` | Your phone number (recipient) | Yes | - |
| `TWILIO_SMS_ENABLED` | Enable/disable SMS alerts | No | true |

## How It Works

### Event Processing Flow

1. **Event Detection**: Pipeline detects new events from Elasticsearch
2. **Batch Processing**: Events are processed in batches (default: 50 events)
3. **SMS Alerts**: For each event in the batch, an SMS alert is sent
4. **Error Handling**: Failed SMS attempts are logged but don't stop processing

### SMS Alert Types

#### Individual Event Alerts
Each security event triggers a detailed SMS with:
- Event type (e.g., "Crowd Management Alert")
- Timestamp
- Location/camera information
- Event description
- Unique event ID

#### Batch Alerts (Optional)
Summary alerts for batches of events:
```
🚨 SECURITY BATCH ALERT 🚨
5 new security events detected (Batch #1)
Check the system for details.
```

## Troubleshooting

### Common Issues

1. **"SMS service initialization failed"**
   - Check your Twilio credentials in `.env`
   - Verify your Account SID and Auth Token are correct

2. **"No recipient phone number configured"**
   - Set `TWILIO_TO_PHONE` in your `.env` file
   - Include country code (e.g., +917045660366)

3. **"Twilio error: The number +XXXXX is unverified"**
   - For trial accounts, verify the recipient number in Twilio Console
   - Or upgrade to a paid Twilio account

4. **SMS not received**
   - Check your phone's spam/blocked messages
   - Verify the recipient number is correct
   - Check Twilio Console logs for delivery status

### Debugging

Enable debug logging by setting `LOG_LEVEL=DEBUG` in your `.env` file:

```bash
LOG_LEVEL=DEBUG
```

This will show detailed SMS sending logs.

## SMS Costs

- Twilio charges per SMS sent
- Typical cost: $0.0075 per SMS in the US
- Monitor your usage in Twilio Console
- Set up usage alerts to avoid unexpected charges

## Security Considerations

1. **Credentials**: Keep your Twilio credentials secure
2. **Rate Limiting**: Be aware of SMS rate limits (1 message/second for trial accounts)
3. **Phone Numbers**: Don't log or expose recipient phone numbers

## Advanced Configuration

### Disabling SMS for Testing

Set `TWILIO_SMS_ENABLED=false` in your `.env` file to disable SMS while keeping other notifications active.

### Custom Message Format

You can modify the SMS message format by editing the `send_event_alert` method in `sms_service.py`.

### Multiple Recipients

Currently supports one recipient. To add multiple recipients, you can:
1. Modify `sms_service.py` to accept a list of phone numbers
2. Call the SMS service multiple times with different numbers

## Health Checks

The pipeline health check now includes SMS service status:

```bash
python main.py --mode health-check
```

This will show:
- ✅ SMS service status
- ✅ Twilio connection status
- ✅ Configuration validation

## Integration with Existing Notifications

SMS alerts work alongside your existing Firebase push notifications:
- Firebase notifications go to app users
- SMS alerts go to specified phone number
- Both systems work independently

## Support

If you encounter issues:
1. Run `python test_sms.py` for diagnostics
2. Check the pipeline logs for error details
3. Verify your Twilio account status and balance
4. Review Twilio Console for delivery logs
