import asyncio
import signal
import sys
from google_ads_pipeline import GoogleAdsPipeline
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
    """Main function to run the pipeline."""
    start_time = datetime.now()
    pipeline = None
    
    try:
        logger.info("Starting Google Ads Pipeline in Cloud Run job")
        pipeline = GoogleAdsPipeline()
        
        pipeline.create_big_query_dataset_if_not_exists()

        # Initialize necessary components
        pipeline.create_big_query_table_if_not_exists()

        
        # Get all customer IDs
        all_accounts = await pipeline.get_customer_ids()
        total_customers = len(all_accounts)
        logger.info(f"Retrieved {total_customers} customer accounts")

        custome_accounts = pipeline.custome_accounts

        test_mode = False  # Set to True for testing, False for production
        test_account = "3892625774"
        
        if test_mode:
            # Only process the test account
            if test_account in all_accounts:
                customer_ids = [test_account]
                logger.info(f"TEST MODE: Only processing account {test_account}")
            else:
                logger.warning(f"TEST MODE: Test account {test_account} not found in available accounts")
                customer_ids = []
        else:
            # Normal processing - use all accounts
            if custome_accounts:
                customer_ids = custome_accounts
                logger.info(f"Processing only custom accounts: {custome_accounts} accounts")
            else:
                customer_ids = all_accounts
        
        # Process all customers
        all_data = []
        successful_customers = 0
        failed_customers = 0
        
        for i, customer_id in enumerate(customer_ids, 1):
            customer_start_time = datetime.now()
            try:
                logger.info(f"Processing customer {i}/{total_customers} (ID: {customer_id})")
                customer_data = await pipeline.fetch_campaign_data(customer_id)
                # print(customer_data)
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
                
                # Check if there's a date field in column config
                date_field = next((col["name"] for col in pipeline.column_config if col["type"] == "DATE"), None)
                
                if date_field:
                    # Delete existing partitions using the date range from the data for partitioned tables
                    logger.info(f"Found DATE field '{date_field}'. Deleting partitions before appending data.")
                    pipeline.delete_partitions()
                else:
                    logger.info("No DATE field found in column configuration. Using regular table without partitioning.")
                
                # Append new data to BigQuery in all cases
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