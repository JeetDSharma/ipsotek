"""
Example usage script for the Elasticsearch to Firebase pipeline.
"""
import asyncio
import json
from datetime import datetime
from loguru import logger

from config import config
from pipeline import data_pipeline


async def example_continuous_pipeline():
    """Example of running the pipeline continuously."""
    logger.info("Starting continuous pipeline example...")
    
    # Initialize the pipeline
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return
    
    try:
        # Run for a few cycles to demonstrate
        for i in range(3):
            logger.info(f"Pipeline cycle {i + 1}")
            
            # Process recent data
            processed_count = await data_pipeline.process_recent_data(minutes_back=5)
            logger.info(f"Processed {processed_count} documents")
            
            # Get and display stats
            stats = data_pipeline.get_stats()
            logger.info(f"Pipeline stats: {stats}")
            
            # Wait before next cycle
            await asyncio.sleep(10)
    
    finally:
        await data_pipeline.cleanup()


async def example_custom_query():
    """Example of using custom queries."""
    logger.info("Starting custom query example...")
    
    # Initialize the pipeline
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return
    
    try:
        # Example 1: Query for specific time range
        time_query = {
            "range": {
                "@timestamp": {
                    "gte": "2024-01-01T00:00:00",
                    "lte": "2024-01-31T23:59:59"
                }
            }
        }
        
        processed_count = await data_pipeline.process_custom_query(time_query)
        logger.info(f"Time range query processed {processed_count} documents")
        
        # Example 2: Query for specific field values
        field_query = {
            "term": {
                "status": "active"
            }
        }
        
        processed_count = await data_pipeline.process_custom_query(field_query)
        logger.info(f"Field query processed {processed_count} documents")
        
        # Example 3: Complex query
        complex_query = {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": "now-1h"}}},
                    {"term": {"level": "error"}}
                ],
                "should": [
                    {"wildcard": {"message": "*error*"}},
                    {"wildcard": {"message": "*exception*"}}
                ]
            }
        }
        
        processed_count = await data_pipeline.process_custom_query(complex_query)
        logger.info(f"Complex query processed {processed_count} documents")
    
    finally:
        await data_pipeline.cleanup()


async def example_monitoring():
    """Example of monitoring the pipeline."""
    logger.info("Starting monitoring example...")
    
    # Initialize the pipeline
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return
    
    try:
        # Get health status
        health = await data_pipeline.health_check()
        logger.info(f"Health check: {json.dumps(health, indent=2, default=str)}")
        
        # Get pipeline statistics
        stats = data_pipeline.get_stats()
        logger.info(f"Pipeline statistics: {json.dumps(stats, indent=2, default=str)}")
        
        # Process some data and monitor
        processed_count = await data_pipeline.process_recent_data(minutes_back=1)
        logger.info(f"Processed {processed_count} documents")
        
        # Get updated stats
        updated_stats = data_pipeline.get_stats()
        logger.info(f"Updated statistics: {json.dumps(updated_stats, indent=2, default=str)}")
    
    finally:
        await data_pipeline.cleanup()


def main():
    """Main function to run examples."""
    print("=" * 60)
    print("Elasticsearch to Firebase Pipeline Examples")
    print("=" * 60)
    
    # Configure logging
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )
    
    # Run examples
    examples = [
        ("Continuous Pipeline", example_continuous_pipeline),
        ("Custom Queries", example_custom_query),
        ("Monitoring", example_monitoring),
    ]
    
    for name, example_func in examples:
        print(f"\n--- {name} ---")
        try:
            asyncio.run(example_func())
        except Exception as e:
            logger.error(f"Example '{name}' failed: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
