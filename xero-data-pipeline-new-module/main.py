import asyncio
import signal
import sys
from xero_data_pipeline import XeroDataPipeline
from logger import LoggerConfig
import config
from datetime import datetime

# Get a configured logger for this module
logger = LoggerConfig.get_logger_from_config(config, __name__)

def handle_sigterm(signum, frame):
    """Handle SIGTERM signal gracefully."""
    logger.info("Received SIGTERM signal. Starting graceful shutdown...")
    sys.exit(0)

async def main():
    """Main function to run the Xero data pipeline."""
    start_time = datetime.now()
    
    try:
        logger.info("Starting Xero Data Pipeline in Cloud Run job")
        
        # Initialize and run the pipeline
        pipeline = XeroDataPipeline()
        success = await pipeline.run_pipeline()
        
        # Calculate total duration
        total_duration = (datetime.now() - start_time).total_seconds()
        
        if success:
            logger.info(f"Pipeline completed successfully - Total Duration: {total_duration:.2f}s")
            return 0
        else:
            logger.error(f"Pipeline failed - Total Duration: {total_duration:.2f}s")
            return 1
            
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed after {total_duration:.2f}s")
        return 1

if __name__ == "__main__":
    # Register signal handler for graceful termination
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # Run the pipeline
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)