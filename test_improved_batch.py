#!/usr/bin/env python3
"""
Test script to verify the improved Firebase batch processing with a limited dataset.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from loguru import logger
from simple_elasticsearch_client import elasticsearch_client
from simple_firebase_client import firebase_client
from config import config


def test_limited_sync():
    """Test the pipeline with a limited number of documents."""
    try:
        logger.info("=" * 60)
        logger.info("Testing Improved Firebase Batch Processing")
        logger.info("=" * 60)
        
        # Initialize clients
        logger.info("1. Initializing clients...")
        if not elasticsearch_client.connect():
            logger.error("Failed to connect to Elasticsearch")
            return False
        
        if not firebase_client.initialize():
            logger.error("Failed to initialize Firebase")
            return False
        
        # Test Elasticsearch connection
        logger.info("2. Testing Elasticsearch connection...")
        if not elasticsearch_client.test_connection():
            logger.error("Elasticsearch connection test failed")
            return False
        logger.info("✅ Elasticsearch connection successful")
        
        # Test Firebase connection
        logger.info("3. Testing Firebase connection...")
        if not firebase_client.test_connection():
            logger.error("Firebase connection test failed")
            return False
        logger.info("✅ Firebase connection successful")
        
        # Get limited data from Elasticsearch
        logger.info("4. Fetching limited data from Elasticsearch...")
        index_name = config.elasticsearch.index_name
        
        # Search for limited number of documents
        response = elasticsearch_client.client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "size": 200  # Limit to 200 documents for testing
            }
        )
        
        hits = response.get('hits', {}).get('hits', [])
        logger.info(f"Found {len(hits)} documents to process")
        
        if not hits:
            logger.warning("No documents found in Elasticsearch")
            return False
        
        # Process and store in Firebase
        logger.info("5. Processing and storing documents in Firebase...")
        successful_stores = firebase_client.store_documents_batch(
            documents=hits,
            collection_name=config.firebase.collection_name
        )
        
        logger.info(f"✅ Successfully stored {successful_stores}/{len(hits)} documents")
        
        # Cleanup
        logger.info("6. Cleaning up...")
        elasticsearch_client.disconnect()
        
        logger.info("=" * 60)
        logger.info("Test completed successfully!")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_limited_sync()
    sys.exit(0 if success else 1)


