"""
Test script to process existing Elasticsearch data (not just recent data).
"""
import asyncio
from datetime import datetime
from loguru import logger

from config import config
from pipeline import data_pipeline


async def test_with_existing_data():
    """Test the pipeline with existing data from Elasticsearch."""
    logger.info("Testing pipeline with existing data...")
    
    # Initialize the pipeline
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        # Process data using a custom query to get any recent data
        # Let's try to get data from the last 24 hours instead of 5 minutes
        processed_count = await data_pipeline.process_recent_data(minutes_back=1440)  # 24 hours
        
        if processed_count > 0:
            logger.info(f"✅ Successfully processed {processed_count} documents!")
            
            # Print statistics
            stats = data_pipeline.get_stats()
            logger.info(f"Pipeline statistics: {stats}")
            
            return True
        else:
            logger.info("No data found in the last 24 hours. Let's try a different approach...")
            
            # Try with a simple match_all query to get any data
            from elasticsearch_client import elasticsearch_client
            from firebase_client import firebase_client
            
            # Get a few documents using match_all
            documents = elasticsearch_client.search_documents({
                "index": config.elasticsearch.index,
                "query": {"match_all": {}},
                "size": 5  # Just get 5 documents for testing
            })
            
            if documents:
                logger.info(f"Found {len(documents)} documents with match_all query")
                
                # Store them in Firebase
                stored_count = await firebase_client.store_elasticsearch_hits(
                    hits=documents,
                    collection_name=config.firebase.collection
                )
                
                logger.info(f"✅ Successfully stored {stored_count} documents in Firebase!")
                
                # Print sample document info
                if documents:
                    sample_doc = documents[0]
                    logger.info(f"Sample document ID: {sample_doc._id}")
                    logger.info(f"Sample event: {sample_doc._source.get('event_name', 'N/A')}")
                    logger.info(f"Sample timestamp: {sample_doc._source.get('@timestamp', 'N/A')}")
                
                return True
            else:
                logger.error("No documents found even with match_all query")
                return False
    
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await data_pipeline.cleanup()


async def main():
    """Main function."""
    print("=" * 60)
    print("Testing Pipeline with Existing Data")
    print("=" * 60)
    
    success = await test_with_existing_data()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ Test completed successfully!")
        print("Check your Firebase console to see the data.")
    else:
        print("❌ Test failed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
