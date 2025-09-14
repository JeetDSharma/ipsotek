# Elasticsearch to Firebase Data Pipeline

This project creates a real-time data pipeline that ingests data from Elasticsearch and stores it in Firebase Firestore. It's designed for continuous data synchronization with robust error handling and monitoring capabilities.

## Features

- **Real-time Data Streaming**: Continuous ingestion from Elasticsearch with configurable polling intervals
- **Automatic Data Transformation**: Seamless conversion between Elasticsearch and Firebase data formats
- **Firebase Firestore Integration**: Efficient batch operations and document management
- **Robust Error Handling**: Comprehensive retry logic and graceful failure handling
- **Comprehensive Logging**: Detailed logging with file rotation and structured output
- **Multiple Execution Modes**: Continuous, single-run, full-sync, and custom query modes
- **Health Monitoring**: Built-in health checks and monitoring utilities
- **Configurable Processing**: Flexible batch sizes, intervals, and retry policies

## Quick Start

### 1. Installation

```bash
# Clone or download the project
cd ipsotek

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy the example configuration
cp env.example .env

# Edit .env with your actual credentials
nano .env
```

### 3. Run the Pipeline

```bash
# Continuous mode (default)
python main.py

# Single execution
python main.py --mode single

# Full synchronization
python main.py --mode full-sync

# Health check
python main.py --mode health-check
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

#### Elasticsearch Configuration
```bash
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_USERNAME=your-username
ELASTICSEARCH_PASSWORD=your-password
ELASTICSEARCH_USE_SSL=false
ELASTICSEARCH_VERIFY_CERTS=false
ELASTICSEARCH_INDEX=your-index-name
```

#### Firebase Configuration
```bash
FIREBASE_PROJECT_ID=your-project-id
FIREBASE_PRIVATE_KEY_ID=your-private-key-id
FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nyour-private-key\n-----END PRIVATE KEY-----"
FIREBASE_CLIENT_EMAIL=your-client-email@your-project.iam.gserviceaccount.com
FIREBASE_CLIENT_ID=your-client-id
FIREBASE_COLLECTION=your-collection-name
```

#### Pipeline Configuration
```bash
POLLING_INTERVAL_SECONDS=30
BATCH_SIZE=100
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5
LOG_LEVEL=INFO
LOG_FILE=pipeline.log
```

### Firebase Setup

1. **Create a Firebase Project**:
   - Go to [Firebase Console](https://console.firebase.google.com/)
   - Create a new project or select existing one

2. **Enable Firestore**:
   - Go to Firestore Database
   - Create database in production mode

3. **Generate Service Account Key**:
   - Go to Project Settings > Service Accounts
   - Click "Generate new private key"
   - Download the JSON file
   - Extract the required fields for your `.env` file

### Elasticsearch Setup

Ensure your Elasticsearch instance is running and accessible. The pipeline supports:
- Basic authentication
- SSL/TLS connections
- Custom indices and queries

## Usage Examples

### Continuous Data Pipeline
```bash
# Run continuously, processing new data every 30 seconds
python main.py --mode continuous
```

### One-time Data Sync
```bash
# Process data from the last 5 minutes once
python main.py --mode single
```

### Full Historical Sync
```bash
# Sync all historical data from Elasticsearch to Firebase
python main.py --mode full-sync
```

### Custom Query Processing
```bash
# Process data matching a specific query
python main.py --mode custom --query '{"range":{"@timestamp":{"gte":"2024-01-01"}}}'
```

### Health Check
```bash
# Check the health of all components
python main.py --mode health-check
```

### Configuration Check
```bash
# Verify configuration without running the pipeline
python main.py --config-check
```

## Monitoring and Testing

### Run Comprehensive Tests
```bash
# Test all components and data flow
python monitor.py
```

### Monitor Pipeline Statistics
The pipeline provides detailed statistics including:
- Total documents processed
- Success/failure rates
- Processing times
- Last run timestamps
- Error messages

## Architecture

### Core Components

- **`main.py`**: Entry point with CLI interface and execution modes
- **`pipeline.py`**: Core pipeline logic and orchestration
- **`elasticsearch_client.py`**: Elasticsearch data fetching and querying
- **`firebase_client.py`**: Firebase Firestore data storage
- **`config.py`**: Configuration management with environment variables
- **`models.py`**: Data models and validation schemas
- **`monitor.py`**: Testing and monitoring utilities

### Data Flow

1. **Elasticsearch Query**: Fetch documents using scroll API or time-based queries
2. **Data Transformation**: Convert Elasticsearch hits to Firebase-compatible documents
3. **Batch Processing**: Group documents for efficient Firebase operations
4. **Error Handling**: Retry failed operations with exponential backoff
5. **Logging**: Comprehensive logging of all operations and errors

### Execution Modes

- **Continuous**: Runs indefinitely, processing new data at regular intervals
- **Single**: Processes recent data once and exits
- **Full Sync**: Processes all historical data and exits
- **Health Check**: Tests all components and reports status

## Error Handling

The pipeline includes comprehensive error handling:

- **Connection Failures**: Automatic retry with exponential backoff
- **Data Validation**: Pydantic models ensure data integrity
- **Batch Failures**: Individual document retry for partial failures
- **Graceful Shutdown**: Proper cleanup on interruption signals
- **Detailed Logging**: All errors logged with context and stack traces

## Performance Considerations

- **Batch Operations**: Configurable batch sizes for optimal performance
- **Async Processing**: Non-blocking I/O operations
- **Memory Management**: Streaming large datasets without loading everything into memory
- **Connection Pooling**: Efficient connection management for both services

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify Elasticsearch is running and accessible
   - Check Firebase credentials and project ID
   - Ensure network connectivity

2. **Authentication Failures**:
   - Verify Elasticsearch username/password
   - Check Firebase service account key format
   - Ensure proper JSON escaping in `.env` file

3. **Data Format Issues**:
   - Check Elasticsearch index mapping
   - Verify Firebase collection permissions
   - Review data transformation logic

### Debug Mode

Enable debug logging:
```bash
# Set in .env file
LOG_LEVEL=DEBUG
```

### Log Files

- **Console Output**: Real-time pipeline status
- **Log File**: Detailed logs with rotation (`pipeline.log`)
- **Error Tracking**: Comprehensive error context and stack traces

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
