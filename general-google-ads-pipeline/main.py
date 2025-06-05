import asyncio
import signal
import sys
from google_ads_pipeline import GoogleAdsPipeline, setup_logging
from google_ads_api import GoogleAdsAPI
from datetime import datetime

logger = setup_logging()

def handle_sigterm(signum, frame):
    """Handle SIGTERM signal gracefully."""
    logger.info("Received SIGTERM signal. Starting graceful shutdown...")
    sys.exit(0)

async def main():
    """Main function to run the pipeline."""
    start_time = datetime.now()
    pipeline = None
    AdsAPI = None
    
    try:
        logger.info("Starting Google Ads Pipeline in Cloud Run job")
        pipeline = GoogleAdsPipeline()
        # AdsAPI = GoogleAdsAPI()
        
        # Initialize necessary components
        pipeline.create_big_query_table_if_not_exists()

        testing_mode = False  # Set to True for testing, False for production
        test_account = "7011193260"
        
        if testing_mode:

            customer_ids = [test_account]
            logger.info(f"TEST MODE: Only processing account {test_account}")
        else:
            # Get all customer IDs
            customer_ids = await pipeline.get_customer_ids()
        
        total_customers = len(customer_ids)
        logger.info(f"Retrieved {total_customers} customer accounts")
        
        # Process all customers
        all_data = []
        successful_customers = 0
        failed_customers = 0
        
        for i, customer_id in enumerate(customer_ids, 1):
            # if i > 5:
            #     break
            customer_start_time = datetime.now()
            try:
                logger.info(f"Processing customer {i}/{total_customers} (ID: {customer_id})")
                customer_data = await pipeline.fetch_campaign_data(customer_id)
                all_data.extend(customer_data)
                
                customer_duration = (datetime.now() - customer_start_time).total_seconds()
                logger.info(
                    f"Successfully processed customer {i}/{total_customers} "
                    f"(ID: {customer_id}) - Records: {len(customer_data)} - "
                    f"Duration: {customer_duration:.2f}s"
                )
                successful_customers += 1
                
            except Exception as e:
                failed_customers += 1
                logger.error(f"Error processing customer {customer_id}: {str(e)}")
                pipeline.metrics.errors.append(f"Customer {customer_id}: {str(e)}")
                continue
        
        # Only proceed with data upload if we have data
        if all_data:
            try:
                logger.info("Starting data upload to BigQuery...")
                
                # Delete existing partitions for the date range
                pipeline.delete_partitions()
                
                # Append new data to BigQuery
                pipeline.append_data_to_bigquery(all_data)
                
                # Log final metrics
                total_duration = (datetime.now() - start_time).total_seconds()
                pipeline.metrics.log_metrics()
                logger.info(
                    f"Pipeline completed successfully - Total Duration: {total_duration:.2f}s - "
                    f"Processed {successful_customers}/{total_customers} customers "
                    f"({failed_customers} failed) - Total records: {len(all_data)}"
                )
                return 0
                
            except Exception as e:
                logger.error(f"Failed during data upload: {str(e)}")
                pipeline.metrics.errors.append(str(e))
                return 1
        else:
            logger.warning("No data was collected from any customer. Skipping BigQuery upload.")
            pipeline.metrics.log_metrics()
            return 1

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        if pipeline:
            pipeline.metrics.errors.append(str(e))
            pipeline.metrics.log_metrics()
        return 1

if __name__ == "__main__":
    # Register signal handler
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
