import asyncio
import signal
import sys
from google_ads_activites_pipeline import GoogleAdsPipeline, setup_logging
from google_ads_api import GoogleAdsAPI
from datetime import datetime
import json
import os

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
        
        # Get all customer IDs
        all_accounts = await pipeline.get_customer_ids()
        total_customers = len(all_accounts)
        logger.info(f"Retrieved {total_customers} customer accounts")

        test_mode = False  # Set to True for testing, False for production
        test_account = "3892625774"
        
        if test_mode:
            # Only process the test account
            if test_account in all_accounts:
                customer_ids = [test_account]
                print(customer_ids)
                logger.info(f"TEST MODE: Only processing account {test_account}")
            else:
                logger.warning(f"TEST MODE: Test account {test_account} not found in available accounts")
                customer_ids = []
        else:
            # # Normal processing - use all accounts
            # if custome_accounts:
            #     customer_ids = all_accounts
            #     logger.info(f"Processing only custom accounts: {all_accounts}")
            # else:
            customer_ids = all_accounts

        # Process all customers
        all_data = []
        successful_customers = 0
        failed_customers = 0
        
        for i, customer_id in enumerate(customer_ids, 1):
            # if i > 1:
            #     break
            customer_start_time = datetime.now()
            try:
                logger.info(f"Processing customer {i}/{total_customers} (ID: {customer_id})")
                activity_data = await pipeline.fetch_activity_data(customer_id)
                # print(activity_data)
                all_data.extend(activity_data)
                
                customer_duration = (datetime.now() - customer_start_time).total_seconds()
                logger.info(
                    f"Successfully processed customer {i}/{total_customers} "
                    f"(ID: {customer_id}) - Records: {len(activity_data)} - "
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
                # print(all_data)
                
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
