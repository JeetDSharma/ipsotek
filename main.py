"""
Main entry point for the Elasticsearch to Firebase data pipeline.
"""
import asyncio
import signal
import sys
from datetime import datetime
from loguru import logger
import argparse

from config import config
from pipeline import data_pipeline


def setup_logging():
    """Configure logging for the application."""
    # Remove default logger
    logger.remove()
    
    # Add console logging
    logger.add(
        sys.stdout,
        level=config.logging.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # Add file logging
    logger.add(
        config.logging.log_file,
        level=config.logging.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="7 days",
        compression="zip"
    )


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    data_pipeline.stop_pipeline()


async def run_continuous_mode():
    """Run the pipeline in continuous mode."""
    logger.info("Starting pipeline in continuous mode...")
    
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        await data_pipeline.run_continuous_pipeline()
    finally:
        await data_pipeline.cleanup()
    
    return True


async def run_single_execution():
    """Run the pipeline once."""
    logger.info("Starting single pipeline execution...")
    
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        # Process recent data
        processed_count = await data_pipeline.process_recent_data(minutes_back=5)
        logger.info(f"Single execution completed. Processed {processed_count} documents")
        
        # Print statistics
        stats = data_pipeline.get_stats()
        logger.info(f"Pipeline statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"Single execution failed: {e}")
        return False
    finally:
        await data_pipeline.cleanup()


async def run_full_sync():
    """Run a full synchronization of all data."""
    logger.info("Starting full data synchronization...")
    
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        # Process all data
        processed_count = await data_pipeline.process_all_data()
        logger.info(f"Full sync completed. Processed {processed_count} documents")
        
        # Print statistics
        stats = data_pipeline.get_stats()
        logger.info(f"Pipeline statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"Full sync failed: {e}")
        return False
    finally:
        await data_pipeline.cleanup()


async def run_custom_query(query_dict):
    """Run pipeline with a custom query."""
    logger.info(f"Starting custom query execution: {query_dict}")
    
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        # Process data with custom query
        processed_count = await data_pipeline.process_custom_query(query_dict)
        logger.info(f"Custom query execution completed. Processed {processed_count} documents")
        
        # Print statistics
        stats = data_pipeline.get_stats()
        logger.info(f"Pipeline statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"Custom query execution failed: {e}")
        return False
    finally:
        await data_pipeline.cleanup()


async def health_check():
    """Perform health check on all components."""
    logger.info("Performing health check...")
    
    if not await data_pipeline.initialize():
        logger.error("Failed to initialize pipeline")
        return False
    
    try:
        health = await data_pipeline.health_check()
        logger.info(f"Health check results: {health}")
        
        if health["overall"]:
            logger.info("All systems healthy")
        else:
            logger.warning("Some systems are unhealthy")
        
        return health["overall"]
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False
    finally:
        await data_pipeline.cleanup()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Elasticsearch to Firebase Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["continuous", "single", "full-sync", "health-check"],
        default="continuous",
        help="Pipeline execution mode"
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Custom Elasticsearch query as JSON string (for custom query mode)"
    )
    parser.add_argument(
        "--config-check",
        action="store_true",
        help="Check configuration and exit"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("Elasticsearch to Firebase Data Pipeline")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("=" * 60)
    
    # Configuration check
    if args.config_check:
        logger.info("Configuration check:")
        logger.info(f"Elasticsearch: {config.get_elasticsearch_url()}")
        logger.info(f"Firebase Project: {config.firebase.project_id}")
        logger.info(f"Index: {config.elasticsearch.index}")
        logger.info(f"Collection: {config.firebase.collection}")
        logger.info(f"Polling Interval: {config.pipeline.polling_interval_seconds}s")
        logger.info(f"Batch Size: {config.pipeline.batch_size}")
        return
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Run the appropriate mode
        if args.mode == "continuous":
            success = asyncio.run(run_continuous_mode())
        elif args.mode == "single":
            success = asyncio.run(run_single_execution())
        elif args.mode == "full-sync":
            success = asyncio.run(run_full_sync())
        elif args.mode == "health-check":
            success = asyncio.run(health_check())
        else:
            logger.error(f"Unknown mode: {args.mode}")
            success = False
        
        if success:
            logger.info("Pipeline execution completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline execution failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
