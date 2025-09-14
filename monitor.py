"""
Utility scripts for testing and monitoring the pipeline.
"""
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from loguru import logger

from config import config
from elasticsearch_client import elasticsearch_client
from firebase_client import firebase_client
from pipeline import data_pipeline


class PipelineMonitor:
    """Monitor and test the pipeline components."""
    
    async def test_elasticsearch_connection(self) -> Dict[str, Any]:
        """Test Elasticsearch connection and basic operations."""
        logger.info("Testing Elasticsearch connection...")
        
        try:
            # Connect
            connected = await elasticsearch_client.connect()
            if not connected:
                return {"status": "failed", "error": "Failed to connect"}
            
            # Test ping
            ping_result = await elasticsearch_client.test_connection()
            
            # Get cluster health
            health_result = await elasticsearch_client.health_check()
            
            # Get index info
            index_info = await elasticsearch_client.get_index_info(config.elasticsearch.index)
            
            # Count documents
            doc_count = await elasticsearch_client.count_documents(config.elasticsearch.index)
            
            await elasticsearch_client.disconnect()
            
            return {
                "status": "success",
                "ping": ping_result,
                "health": health_result,
                "index_exists": index_info is not None,
                "document_count": doc_count,
                "index_info": index_info
            }
            
        except Exception as e:
            logger.error(f"Elasticsearch test failed: {e}")
            return {"status": "failed", "error": str(e)}
    
    async def test_firebase_connection(self) -> Dict[str, Any]:
        """Test Firebase connection and basic operations."""
        logger.info("Testing Firebase connection...")
        
        try:
            # Initialize
            initialized = firebase_client.initialize()
            if not initialized:
                return {"status": "failed", "error": "Failed to initialize"}
            
            # Test connection
            connection_result = firebase_client.test_connection()
            
            # Get collection stats
            stats = await firebase_client.get_collection_stats(config.firebase.collection)
            
            return {
                "status": "success",
                "connection": connection_result,
                "collection_stats": stats
            }
            
        except Exception as e:
            logger.error(f"Firebase test failed: {e}")
            return {"status": "failed", "error": str(e)}
    
    async def test_data_transfer(self, limit: int = 10) -> Dict[str, Any]:
        """Test transferring a small amount of data."""
        logger.info(f"Testing data transfer with {limit} documents...")
        
        try:
            # Initialize pipeline
            if not await data_pipeline.initialize():
                return {"status": "failed", "error": "Failed to initialize pipeline"}
            
            # Get a few documents from Elasticsearch
            query = {
                "match_all": {}
            }
            
            elasticsearch_query = {
                "index": config.elasticsearch.index,
                "query": query,
                "size": limit
            }
            
            documents = await elasticsearch_client.search_documents(
                elasticsearch_query
            )
            
            if not documents:
                return {"status": "success", "message": "No documents found to transfer"}
            
            # Store in Firebase
            stored_count = await firebase_client.store_elasticsearch_hits(
                hits=documents,
                collection_name=f"{config.firebase.collection}_test"
            )
            
            await data_pipeline.cleanup()
            
            return {
                "status": "success",
                "documents_found": len(documents),
                "documents_stored": stored_count,
                "test_collection": f"{config.firebase.collection}_test"
            }
            
        except Exception as e:
            logger.error(f"Data transfer test failed: {e}")
            return {"status": "failed", "error": str(e)}
    
    async def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        try:
            if not await data_pipeline.initialize():
                return {"status": "failed", "error": "Failed to initialize pipeline"}
            
            # Get pipeline stats
            stats = data_pipeline.get_stats()
            
            # Get health status
            health = await data_pipeline.health_check()
            
            # Get Elasticsearch document count
            es_count = await elasticsearch_client.count_documents(config.elasticsearch.index)
            
            # Get Firebase collection stats
            fb_stats = await firebase_client.get_collection_stats(config.firebase.collection)
            
            await data_pipeline.cleanup()
            
            return {
                "status": "success",
                "pipeline_stats": stats,
                "health": health,
                "elasticsearch_document_count": es_count,
                "firebase_stats": fb_stats,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get pipeline stats: {e}")
            return {"status": "failed", "error": str(e)}
    
    async def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run a comprehensive test of all components."""
        logger.info("Running comprehensive pipeline test...")
        
        results = {
            "timestamp": datetime.utcnow(),
            "tests": {}
        }
        
        # Test Elasticsearch
        results["tests"]["elasticsearch"] = await self.test_elasticsearch_connection()
        
        # Test Firebase
        results["tests"]["firebase"] = await self.test_firebase_connection()
        
        # Test data transfer
        results["tests"]["data_transfer"] = await self.test_data_transfer()
        
        # Get overall stats
        results["tests"]["pipeline_stats"] = await self.get_pipeline_stats()
        
        # Determine overall status
        all_passed = all(
            test.get("status") == "success" 
            for test in results["tests"].values()
        )
        
        results["overall_status"] = "success" if all_passed else "failed"
        
        return results


async def main():
    """Main function for running tests."""
    monitor = PipelineMonitor()
    
    print("=" * 60)
    print("Pipeline Monitor and Test Suite")
    print("=" * 60)
    
    # Run comprehensive test
    results = await monitor.run_comprehensive_test()
    
    # Print results
    print("\nTest Results:")
    print(json.dumps(results, indent=2, default=str))
    
    # Print summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"Overall Status: {results['overall_status']}")
    
    for test_name, test_result in results["tests"].items():
        status = test_result.get("status", "unknown")
        print(f"{test_name}: {status}")
    
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
