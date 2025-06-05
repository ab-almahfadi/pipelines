import asyncio
import signal
import sys
from meta_ads_pipeline import MetaAdsPipeline, logger
from datetime import datetime
from logger import LoggerConfig
import config

# Get a configured logger for this module
logger = LoggerConfig.get_logger_from_config(config, __name__)
def handle_sigterm(signum, frame):
    """Handle SIGTERM signal gracefully."""
    logger.info("Received SIGTERM signal. Starting graceful shutdown...")
    sys.exit(0)

async def main():
    """Main function to run the pipeline with enhanced batch processing and metrics."""
    try:
        logger.info("Starting Meta Ads Pipeline in Cloud Run job")
        pipeline = MetaAdsPipeline()

        # Update Meta Access Token
        pipeline.update_meta_access_token()

        # Create BigQuery dataset if it doesn't exist
        pipeline.create_big_query_dataset_if_not_exists()
        
        # Initialize necessary components
        pipeline.create_big_query_table_if_not_exists()

        custome_accounts = pipeline.custome_accounts
        
        # Get all accounts and calculate batch information
        
        # Filter for testing - only process account 369912827
        test_mode = False  # Set to True for testing, False for production
        test_account = "act_229691921275637"
        
        if test_mode:
            # Only process the test account
            if test_account:
                accounts = [test_account]
                logger.info(f"TEST MODE: Only processing account {test_account}")
            else:
                logger.warning(f"TEST MODE: Test account {test_account} not found in available accounts")
                accounts = []
        else:
            # Normal processing - use all accounts
            if custome_accounts:
                accounts = custome_accounts
                logger.info(f"Processing only custom accounts: {accounts}")
            else:
                all_accounts = await pipeline.get_facebook_accounts()
                accounts = all_accounts
        
        # print(accounts)
        total_accounts = len(accounts)
        batch_size = pipeline.batch_size
        total_batches = (total_accounts + batch_size - 1) // batch_size
        
        logger.info(f"Retrieved {total_accounts} Facebook accounts - Will process in {total_batches} batches")


        # if pipeline.endpoints_level == "account_id":
        # Process accounts in batches with detailed tracking
        for i in range(0, total_accounts, batch_size):
            batch = accounts[i:i + batch_size]
            current_batch = i // batch_size + 1
            batch_start_time = datetime.now()
            
            logger.info(
                f"Starting batch {current_batch}/{total_batches} "
                f"(Processing accounts {i+1}-{min(i+batch_size, total_accounts)} of {total_accounts})"
            )
            
            try:
                await pipeline.process_batch(batch)
                batch_duration = (datetime.now() - batch_start_time).total_seconds()
                logger.info(
                    f"Successfully completed batch {current_batch}/{total_batches} "
                    f"Duration: {batch_duration:.2f}s"
                )
                
            except Exception as batch_error:
                logger.error(
                    f"Error in batch {current_batch}/{total_batches}: {str(batch_error)}"
                )
                raise
        
        # Delete existing partitions and append new data
        logger.info("All batches completed. Starting data upload to BigQuery...")
        df = pipeline.prepare_dataframe()
        
        if df is None or df.empty:
            logger.warning("No data to process. Skipping BigQuery upload.")
            return 0
              # Find the date column name dynamically instead of hardcoding 'date'
        # First, check for columns with is_date_range flag
        date_column = next((col['name'] for col in pipeline.column_definitions 
                           if col.get('is_date_range', False)), None)
          # Append data to BigQuery in all cases
        if date_column and date_column in df.columns:
            # If we have a date column, we can delete partitions
            min_date = df[date_column].min().strftime('%Y-%m-%d')
            max_date = df[date_column].max().strftime('%Y-%m-%d')
            
            # Delete existing partitions using the date range from the data
            pipeline.delete_partitions(min_date, max_date)
            
            # Append to BigQuery
            pipeline.append_data_to_bigquery(df)
            
            # Log final metrics
            pipeline.metrics.log_metrics()
            logger.info(f"Pipeline completed successfully - from: {min_date} to: {max_date}")
        else:
            # For non-partitioned tables, just append the data without deleting partitions
            if not date_column:
                logger.info("No DATE column found in column definitions. Using regular table without partitioning.")
            elif date_column not in df.columns:
                logger.warning(f"Date column '{date_column}' not found in DataFrame. Using regular table without partitioning.")
            
            # Delete all data from the table instead of using date-based partitioning
            pipeline.delete_partitions(None, None)  # The method will handle this case
            
            # Append to BigQuery
            pipeline.append_data_to_bigquery(df)

            
            # Log final metrics
            pipeline.metrics.log_metrics()
            logger.info("Pipeline completed successfully - data appended to regular table (no partitioning)")
        
        return 0


    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        try:
            # In case pipeline object was created
            pipeline.metrics.errors.append(str(e))
            pipeline.metrics.log_metrics()
        except:
            # In case pipeline creation itself failed
            logger.error("Could not log metrics due to pipeline initialization failure")
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