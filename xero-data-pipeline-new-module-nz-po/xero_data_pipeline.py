import asyncio
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import base64
import os
import logging
import logging.handlers
import json
import functions_framework
from google.cloud import bigquery
from google.cloud import secretmanager
import pandas as pd
import aiohttp
import requests
from concurrent.futures import ThreadPoolExecutor
import threading
import config
import traceback  # For detailed stack traces
from google.cloud import run_v2
from dateutil.parser import parse as parse_date
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from logger import LoggerConfig


# Get a configured logger for this module
logger = LoggerConfig.get_logger_from_config(config, __name__)

class XeroPipelineMetrics:
    """Class to track pipeline metrics and performance."""
    def __init__(self):
        self.start_time = datetime.now()
        self.api_calls = 0
        self.successful_api_calls = 0
        self.failed_api_calls = 0
        self.records_processed = 0
        self.bigquery_operations = 0
        self.errors = []

    def log_metrics(self):
        """Log current metrics."""
        duration = datetime.now() - self.start_time
        metrics = {
            "duration_seconds": duration.total_seconds(),
            "api_calls_total": self.api_calls,
            "api_calls_successful": self.successful_api_calls,
            "api_calls_failed": self.failed_api_calls,
            "records_processed": self.records_processed,
            "bigquery_operations": self.bigquery_operations,
            "error_count": len(self.errors)
        }
        logger.info(f"Pipeline metrics: {json.dumps(metrics, indent=2)}")

class XeroDataPipeline:
    """Main pipeline class for fetching and processing Xero accounting data."""
    
    def __init__(self):
        """Initialize the pipeline with necessary clients and configurations."""
        # Load configuration from environment or config file
        self.client_id = os.environ.get("XERO_CLIENT_ID", config.XERO_CLIENT_ID)
        self.client_secret = os.environ.get("XERO_CLIENT_SECRET", config.XERO_CLIENT_SECRET)
        self.tenant_id = os.environ.get("XERO_TENANT_ID", config.XERO_TENANT_ID)
        self.redirect_uri = os.environ.get("XERO_REDIRECT_URI", config.XERO_REDIRECT_URI)
        self.scopes = os.environ.get("XERO_SCOPES", config.XERO_SCOPES)
        
        self.project_id = os.environ.get("GCP_PROJECT_ID", config.GCP_PROJECT_ID)
        self.dataset_id = os.environ.get("BIGQUERY_DATASET_ID", config.BIGQUERY_DATASET_ID)
        self.PurchaseOrders_table_id = os.environ.get("BIGQUERY_TABLE_ID_PurchaseOrders", config.BIGQUERY_TABLE_ID_PurchaseOrders)
        self.credit_notes_table_id = os.environ.get("BIGQUERY_TABLE_ID_CREDIT_NOTES", config.BIGQUERY_TABLE_ID_CREDIT_NOTES)
        self.profit_loss_table_id = os.environ.get("BIGQUERY_TABLE_ID_PROFIT_LOSS", config.BIGQUERY_TABLE_ID_PROFIT_LOSS)
        
        self.job_name = os.environ.get("CLOUD_RUN_JOB_NAME", config.CLOUD_RUN_JOB_NAME)
        self.start_date = os.environ.get("REFRESH_WINDOW_START_DATE", config.REFRESH_WINDOW_START_DATE)
        self.batch_size = int(os.environ.get("BATCH_SIZE", config.BATCH_SIZE))
        self.max_retries = int(os.environ.get("MAX_RETRIES", config.MAX_RETRIES))
        self.rate_limit_delay = int(os.environ.get("RATE_LIMIT_DELAY", config.RATE_LIMIT_DELAY))
        
        # Load column definitions
        self.PurchaseOrders_columns = config.PurchaseOrders_COLUMN_DEFINITIONS
        self.credit_notes_columns = config.CREDIT_NOTES_COLUMN_DEFINITIONS
        self.profit_loss_columns = config.PROFIT_LOSS_COLUMN_DEFINITIONS
        
        # Product quantity multipliers
        self.product_multipliers = config.PRODUCT_QUANTITY_MULTIPLIERS
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # Initialize metrics tracker
        self.metrics = XeroPipelineMetrics()
        
        # Initialize OAuth token storage
        self.token = None
        
        # Generate secret name for this tenant
        self.secret_name = os.environ.get("XERO_SECRET_NAME", config.XERO_SECRET_NAME)
        # if not self.secret_name:
        #     self.secret_name = self.generate_secret_name()
        #     logger.info(f"Generated secret name: {self.secret_name}")
        #     # Store the secret name for future reference
        #     os.environ["XERO_SECRET_NAME"] = self.secret_name
        # else:
        #     logger.info(f"Using configured secret name: {self.secret_name}")
        
        # Try to get refresh token from Secret Manager first
        self.refresh_token = self.get_refresh_token()
        
        # # If not found in Secret Manager, fall back to environment or config
        # if not self.refresh_token:
        #     self.refresh_token = os.environ.get("XERO_REFRESH_TOKEN", config.XERO_REFRESH_TOKEN)
        #     # If we have a refresh token from config/env, store it in Secret Manager
        #     if self.refresh_token and self.refresh_token != "YOUR_REFRESH_TOKEN":
        #         logger.info("Storing initial refresh token in Secret Manager")
        #         self.store_refresh_token_in_secret_manager(self.secret_name, self.refresh_token)
        
        # Table references
        self.PurchaseOrders_table_ref = f"{self.project_id}.{self.dataset_id}.{self.PurchaseOrders_table_id}"
        self.credit_notes_table_ref = f"{self.project_id}.{self.dataset_id}.{self.credit_notes_table_id}"
        self.profit_loss_table_ref = f"{self.project_id}.{self.dataset_id}.{self.profit_loss_table_id}"
        
        # Data storage
        self.PurchaseOrders = []
        self.credit_notes_data = []
        self.profit_loss_data = []
        
        logger.info(
            f"Initialized Xero pipeline with start date: {self.start_date} "
            f"(Job: {self.job_name})"
        )

    def update_xero_access_token(self):
        client_id = self.client_id
        client_secret = self.client_secret
        refresh_token = self.refresh_token
        cloud_run_job_name = self.job_name
        project_id = self.project_id
        region = "us-central1"

        if not all([client_id, client_secret, refresh_token, project_id]):
            raise ValueError("Missing required environment variables")

        # Use the refresh token to get a new access token from Xero
        token_url = "https://identity.xero.com/connect/token"
            
        # Create Authorization header with Base64 encoded client credentials
        auth_header = 'Basic ' + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            
        headers = {
            'Authorization': auth_header,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
            
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret
        }
            
        response = requests.post(token_url, headers=headers, data=payload)

        if response.status_code == 200:
            response_data = response.json()
            new_access_token = response_data['access_token']
            new_refresh_token = response_data['refresh_token']
            expires_in = response_data['expires_in']
            
            if not new_access_token or not new_refresh_token:
                raise ValueError("Failed to extract new tokens.")

            print("Xero access and refresh tokens fetched successfully.")
            self.save_refresh_token(new_refresh_token)
            
            # Update Cloud Run job with new tokens
            client = run_v2.JobsClient()
            job_name = f"projects/{project_id}/locations/{region}/jobs/{cloud_run_job_name}"

            job = client.get_job(name=job_name)

            env_vars = job.template.template.containers[0].env

            # Remove the old tokens
            for env_var in env_vars[:]: 
                if env_var.name == "XERO_ACCESS_TOKEN" or env_var.name == "XERO_REFRESH_TOKEN":
                    env_vars.remove(env_var)  

            client.update_job(job=job)
            print("Removed old XERO tokens successfully.")

            # Get the job again after update
            job = client.get_job(name=job_name)

            # Add the new tokens
            job.template.template.containers[0].env.append(
                {"name": "XERO_ACCESS_TOKEN", "value": new_access_token}
            )
            job.template.template.containers[0].env.append(
                {"name": "XERO_REFRESH_TOKEN", "value": new_refresh_token}
            )

            updated_job = client.update_job(job=job)
            print("Updated Cloud Run job successfully with new XERO tokens.")
            
            # Update local token values
            self.token = {
                'access_token': new_access_token,
                'refresh_token': new_refresh_token,
                'expires_in': expires_in,
                'expires_at': int(datetime.now().timestamp()) + expires_in
            }
            self.refresh_token = new_refresh_token
            
            # Update the refresh token in Secret Manager
            # self.store_refresh_token_in_secret_manager(self.secret_name, new_refresh_token)
            
            return True
        else:
            raise Exception(f"Failed to refresh token: {response.status_code}, {response.text}")
           
    async def make_api_request(self, url: str, method: str = "GET", params: Dict = None, retries: int = 0) -> Dict:
        """
        Make an authenticated request to the Xero API with retry logic.
        
        Args:
            url: The API endpoint URL
            method: HTTP method (GET, POST, etc.)
            params: Query parameters
            retries: Current retry count
            
        Returns:
            The JSON response from the API
        """
        # Ensure we have a valid token
        # self.refresh_token()
        # print(f"Token: {self.token}")
        
        headers = {
            "Authorization": f"Bearer {self.token['access_token']}",
            "Accept": "application/json",
            "Xero-tenant-id": self.tenant_id
        }
        
        self.metrics.api_calls += 1
        start_time = datetime.now()
        # print(f"Start Time: {headers}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method, url, headers=headers, params=params
                ) as response:
                    duration = (datetime.now() - start_time).total_seconds()
                    
                    # Handle successful response
                    if response.status == 200:
                        self.metrics.successful_api_calls += 1
                        data = await response.json()
                        logger.info(f"API call successful - URL: {url} - Duration: {duration:.2f}s")
                        return data
                    # Handle rate limiting
                    elif response.status == 429:
                        error_content = await response.text()
                        self.metrics.failed_api_calls += 1
                        logger.warning(f"Rate limited by Xero API. Waiting {self.rate_limit_delay} {error_content} seconds...")
                        await asyncio.sleep(self.rate_limit_delay)
                        
                        if retries < self.max_retries:
                            logger.info(f"Retrying request ({retries + 1}/{self.max_retries})")
                            return await self.make_api_request(url, method, params, retries + 1)
                        else:
                            logger.error(f"Max retries exceeded for URL: {url}")
                            raise Exception(f"Failed after {self.max_retries} retries due to rate limiting")
                    
                    # Handle other errors
                    else:
                        self.metrics.failed_api_calls += 1
                        error_content = await response.text()
                        error_msg = f"API call failed - Status: {response.status} - {error_content}"
                        
                        if retries < self.max_retries:
                            wait_time = 2 ** retries  # Exponential backoff
                            logger.warning(f"{error_msg} - Retrying in {wait_time}s ({retries + 1}/{self.max_retries})")
                            await asyncio.sleep(wait_time)
                            return await self.make_api_request(url, method, params, retries + 1)
                        
                        logger.error(f"{error_msg} - Max retries exceeded")
                        raise Exception(f"API request failed: {error_msg}")
                        
        except aiohttp.ClientError as e:
            self.metrics.failed_api_calls += 1
            logger.error(f"Request error: {str(e)}")
            
            if retries < self.max_retries:
                wait_time = 2 ** retries  # Exponential backoff
                logger.warning(f"Connection error - Retrying in {wait_time}s ({retries + 1}/{self.max_retries})")
                await asyncio.sleep(wait_time)
                return await self.make_api_request(url, method, params, retries + 1)
            
            raise Exception(f"Connection failed after {self.max_retries} retries: {str(e)}")

    def adjust_quantity_for_product(self, item_name: str, original_quantity: float) -> float:
        """
        Adjusts product quantity based on packaging multipliers.
        Replicates the logic from the App Script.
        
        Args:
            item_name: Product name
            original_quantity: Original quantity value
            
        Returns:
            Adjusted quantity
        """
        if item_name in self.product_multipliers:
            return original_quantity * self.product_multipliers[item_name]
        return original_quantity

    async def fetch_PurchaseOrders(self) -> List[Dict]:
        """
        Fetch PurchaseOrders from Xero API with pagination.
        Processes PurchaseOrders in batches to avoid memory issues.
        
        Returns:
            List of processed invoice records
        """
        logger.info(f"Fetching PurchaseOrders from {self.start_date}")
        api_url = "https://api.xero.com/api.xro/2.0/PurchaseOrders"
        
        # Prepare filter for PurchaseOrders since start date
        date_filter = f"Date>=DateTime({self.start_date})"
        
        page = 1
        has_more_pages = True
        processed_count = 0
        
        print(f"Response: {date_filter}")
        while has_more_pages:
            try:
                params = {
                    "page": page,
                    "where": date_filter
                }
                
                print(f"Response: {params}")
                response = await self.make_api_request(api_url, params=params)
                
                if response and "PurchaseOrders" in response:
                    PurchaseOrders_batch = response["PurchaseOrders"]
                    batch_count = len(PurchaseOrders_batch)
                    
                    if batch_count > 0:
                        # Process this batch of PurchaseOrders
                        processed_records = self.process_purchase_orders(PurchaseOrders_batch)
                        self.PurchaseOrders.extend(processed_records)
                        
                        processed_count += batch_count
                        logger.info(f"Processed {batch_count} PurchaseOrders from page {page} (Total: {processed_count})")
                        
                        # Check if we should continue to the next page
                        has_more_pages = batch_count == 100  # Default page size from Xero API
                        page += 1
                    else:
                        has_more_pages = False
                else:
                    logger.warning(f"Unexpected response format from Xero API: {response}")
                    has_more_pages = False
                    
            except Exception as e:
                logger.error(f"Error fetching PurchaseOrders page {page}: {str(e)}")
                self.metrics.errors.append(f"Invoice fetch error (page {page}): {str(e)}")
                raise
        
        logger.info(f"Completed fetching PurchaseOrders. Total records: {processed_count}")
        self.metrics.records_processed += processed_count
        return self.PurchaseOrders

    def process_purchase_orders(self, purchase_orders: List[Dict]) -> List[List]:
        """
        Process purchase order records from Xero API into a format ready for BigQuery.
        Handles line items and applies product quantity adjustments if needed.
        
        Args:
            purchase_orders: List of purchase order objects from Xero API
            
        Returns:
            List of processed records with flattened line items
        """
        processed_records = []
        
        for po in purchase_orders:
            # Extract basic purchase order data that's shared across line items
            base_po_data = {
                "PurchaseOrderID": po.get("PurchaseOrderID", ""),
                "PurchaseOrderNumber": po.get("PurchaseOrderNumber", ""),
                "DateString": po.get("DateString", ""),
                "DeliveryDateString": po.get("DeliveryDateString", ""),
                "Status": po.get("Status", ""),
                "UpdatedDateUTC": po.get("UpdatedDateUTC", ""),
                "Contact": {
                    "Name": po.get("Contact", {}).get("Name", "No Name Provided"),
                    "ContactStatus": po.get("Contact", {}).get("ContactStatus", "")
                }
            }
            
            # Process each line item as a separate record
            line_items = po.get("LineItems", [])
            
            for line_item in line_items:
                # Create a copy of the PO with this specific line item
                record = base_po_data.copy()
                
                # Extract line item details
                line_item_data = {
                    "ItemCode": line_item.get("ItemCode", "No Item Code"),
                    "Description": line_item.get("Description", ""),
                    "UnitAmount": line_item.get("UnitAmount", 0),
                    "TaxType": line_item.get("TaxType", ""),
                    "TaxAmount": line_item.get("TaxAmount", 0),
                    "LineAmount": line_item.get("LineAmount", 0),
                    "Quantity": line_item.get("Quantity", 0),
                    "LineItemID": line_item.get("LineItemID", "")
                }
                
                # Add the line item to the record
                record["LineItems"] = line_item_data
                
                # Process the record using our column definitions
                processed_record = self.extract_record_values(record, self.PurchaseOrders_columns)
                processed_records.append(processed_record)
        
        return processed_records

    def extract_record_values(self, record: Dict, column_definitions: List[Dict]) -> List:
        """
        Extract values from a record based on column definitions.
        Handles nested fields and auto-generated fields.
        
        Args:
            record: Record to extract values from
            column_definitions: List of column definitions
            
        Returns:
            List of values in the order defined by column definitions
        """
        values = []
        
        for col_def in column_definitions:
            col_name = col_def["name"]
            source_field = col_def.get("source_field")
            is_nested = col_def.get("is_nested", False)
            auto_generate = col_def.get("auto_generate", False)
            
            # Handle auto-generated fields
            if auto_generate:
                if col_name == "processed_at":
                    values.append(datetime.now())
                else:
                    values.append(None)
                continue
            
            # Handle nested fields
            if is_nested:
                value = self.extract_nested_value(record, source_field)
                values.append(value)
            # Handle direct fields
            else:
                value = record.get(source_field)
                values.append(value)
        
        return values

    def extract_nested_value(self, record: Dict, field_path: str) -> Any:
        """
        Extract a value from a nested structure using a dot-notation path.
        Handles special syntax for array index access.
        
        Args:
            record: Record to extract value from
            field_path: Path to the value (e.g., "Contact.Name" or "Cells[0].Value")
            
        Returns:
            The extracted value or None if not found
        """
        # Handle array index syntax [n]
        if "[" in field_path and "]" in field_path:
            # Split into parts
            parts = field_path.split(".")
            current = record
            
            for part in parts:
                # Check if this part has an array index
                if "[" in part and "]" in part:
                    # Split into field name and index
                    field_name = part.split("[")[0]
                    index_str = part.split("[")[1].split("]")[0]
                    
                    try:
                        index = int(index_str)
                        if current and field_name in current and isinstance(current[field_name], list) and len(current[field_name]) > index:
                            current = current[field_name][index]
                        else:
                            return None
                    except (ValueError, TypeError, IndexError):
                        return None
                else:
                    # Regular field access
                    if current and isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return None
            
            return current
        
        # Regular dot notation
        else:
            parts = field_path.split(".")
            current = record
            
            for part in parts:
                if current and isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            
            return current

    def create_big_query_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist."""
        start_time = datetime.now()
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        logger.info(f"Checking/creating BigQuery dataset: {dataset_ref}")
        
        try:
            # Check if dataset exists
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
            return
        except Exception:
            try:
                # Create dataset
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Default location
                
                # Create the dataset
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Created dataset {dataset_ref} - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                
            except Exception as e:
                logger.error(f"Failed to create BigQuery dataset: {str(e)}")
                self.metrics.errors.append(str(e))
                raise

    def create_big_query_table_if_not_exists(self, table_ref: str, column_definitions: List[Dict]):
        """
        Create BigQuery table if it doesn't exist.
        
        Args:
            table_ref: Full table reference (project.dataset.table)
            column_definitions: List of column definitions
        """
        start_time = datetime.now()
        logger.info(f"Checking/creating BigQuery table: {table_ref}")
        
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
            return
        except Exception:
            try:
                # Create schema based on column definitions
                schema = []
                
                for col_def in column_definitions:
                    field_name = col_def['name']
                    field_type = col_def['type']
                    # All fields are NULLABLE by default
                    schema.append(bigquery.SchemaField(field_name, field_type, mode="NULLABLE"))
                
                table = bigquery.Table(table_ref, schema=schema)

                # Find the date column for partitioning
                date_column = next((col['name'] for col in column_definitions 
                                  if col['type'] == 'DATE'), None)
                
                if date_column:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=date_column
                    )
                
                # Set clustering fields if applicable
                clustering_fields = []
                
                if date_column:
                    clustering_fields.append(date_column)
                
                if "type" in [col['name'] for col in column_definitions]:
                    clustering_fields.append("type")
                
                if clustering_fields:
                    table.clustering_fields = clustering_fields[:4]  # Max 4 clustering fields
                
                self.bq_client.create_table(table)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Created table {table_ref} - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                
            except Exception as e:
                logger.error(f"Failed to create BigQuery table: {str(e)}")
                self.metrics.errors.append(str(e))
                raise

    def delete_partitions(self, table_ref: str, date_column: str, min_date: str, max_date: str):
        """
        Delete partitions within a date range.
        
        Args:
            table_ref: Full table reference (project.dataset.table)
            date_column: Date column to use for filtering
            min_date: Start date in YYYY-MM-DD format
            max_date: End date in YYYY-MM-DD format
        """
        start_time = datetime.now()
        logger.info(f"Deleting partitions for date range: {min_date} to {max_date} in {table_ref}")
        
        try:
            # Verify the date column exists in the table
            table = self.bq_client.get_table(table_ref)
            table_schema_fields = [field.name for field in table.schema]
            
            if date_column not in table_schema_fields:
                raise ValueError(f"Column '{date_column}' not found in table schema. Available columns: {', '.join(table_schema_fields)}")
            
            logger.info(f"Using column '{date_column}' for partition deletion")
            
            query = f"""
            DELETE FROM `{table_ref}`
            WHERE DATE({date_column}) BETWEEN "{min_date}" AND "{max_date}"
            """
            
            logger.info(f"Executing deletion query: {query}")
            query_job = self.bq_client.query(query)
            result = query_job.result()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Deleted partitions successfully - Duration: {duration:.2f}s")
            self.metrics.bigquery_operations += 1
            
        except Exception as e:
            logger.error(f"Failed to delete partitions: {str(e)}")
            self.metrics.errors.append(str(e))
            raise Exception(f"Failed to delete partitions: {str(e)} - Check if your date column exists and is correctly named in your table schema.") from e

    def append_data_to_bigquery(self, table_ref: str, data: List[List], column_names: List[str]):
        """
        Append data to BigQuery table.
        
        Args:
            table_ref: Full table reference (project.dataset.table)
            data: List of records to append
            column_names: List of column names
        """
        if not data:
            logger.warning(f"No data to append to {table_ref}")
            return
            
        start_time = datetime.now()
        logger.info(f"Appending {len(data)} rows to {table_ref}")
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=column_names)
            
            # Convert data types based on column definitions
            for col_def in self.PurchaseOrders_columns:
                col_name = col_def['name']
                col_type = col_def['type']
                
                if col_name in df.columns:
                    # Handle date and timestamp conversions
                    if col_type == 'DATE' or col_type == 'TIMESTAMP':
                        df[col_name] = pd.to_datetime(df[col_name], utc=True)
                    
                    # Handle numeric conversions
                    elif col_type == 'INTEGER' or col_type == 'FLOAT64':
                        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            
            # Load the DataFrame into BigQuery
            job = self.bq_client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            
            result = job.result()  # Wait for the job to complete
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(
                f"Successfully appended {len(df)} rows to {table_ref} - "
                f"Duration: {duration:.2f}s"
            )
            self.metrics.bigquery_operations += 1
            
        except Exception as e:
            logger.error(f"Failed to append data to BigQuery: {str(e)}")
            self.metrics.errors.append(str(e))
            raise

    # Constants for GCS bucket storage
    BUCKET_NAME = 'pah-xero-refresh-token-bucket'
    TOKEN_FILE_NAME = 'refresh_token.txt'

    def get_refresh_token_from_gcs(self) -> str:
        """
        Get refresh token from Google Cloud Storage bucket.
        Replicates getRefreshTokenFromGCS function from App Script.
        
        Returns:
            The refresh token as string or None if not found
        """
        try:
            from google.cloud import storage
            
            logger.info(f"Getting refresh token from GCS bucket: {self.BUCKET_NAME}/{self.TOKEN_FILE_NAME}")
            storage_client = storage.Client(project=self.project_id)
            bucket = storage_client.bucket(self.BUCKET_NAME)
            blob = bucket.blob(self.TOKEN_FILE_NAME)
            
            if blob.exists():
                token = blob.download_as_text()
                logger.info("Retrieved refresh token from GCS bucket")
                return token
            else:
                logger.warning(f"Refresh token file not found in GCS bucket: {self.TOKEN_FILE_NAME}")
                return None
                
        except Exception as e:
            logger.error(f"Error reading refresh token from GCS: {str(e)}")
            logger.error(f"Full exception: {traceback.format_exc()}")
            self.metrics.errors.append(f"GCS token retrieval error: {str(e)}")
            return None

    def save_refresh_token_to_gcs(self, token_value: str) -> bool:
        """
        Save refresh token to Google Cloud Storage bucket.
        Replicates saveRefreshTokenToGCS function from App Script.
        
        Args:
            token_value: The refresh token to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from google.cloud import storage
            
            logger.info(f"Saving refresh token to GCS bucket: {self.BUCKET_NAME}/{self.TOKEN_FILE_NAME}")
            storage_client = storage.Client(project=self.project_id)
            bucket = storage_client.bucket(self.BUCKET_NAME)
            blob = bucket.blob(self.TOKEN_FILE_NAME)
            
            blob.upload_from_string(token_value, content_type="text/plain")
            logger.info("Saved refresh token to GCS bucket successfully")
            return True
                
        except Exception as e:
            logger.error(f"Error saving refresh token to GCS: {str(e)}")
            logger.error(f"Full exception: {traceback.format_exc()}")
            self.metrics.errors.append(f"GCS token save error: {str(e)}")
            return False

    def get_refresh_token(self) -> str:
        """
        Get the refresh token from various sources with fallbacks.
        First tries GCS bucket, then falls back to environment variable.
        Replicates getRefreshToken function from App Script.
        
        Returns:
            The refresh token or None if not found
        """
        # First try to get from GCS
        token_from_gcs = self.get_refresh_token_from_gcs()
        if token_from_gcs:
            logger.info("Using refresh token from GCS")
            return token_from_gcs
        
        # Fall back to environment variable
        token_from_env = os.environ.get("XERO_REFRESH_TOKEN", config.XERO_REFRESH_TOKEN)
        if token_from_env and token_from_env != "YOUR_REFRESH_TOKEN":
            logger.info("Using refresh token from environment")
            return token_from_env
        
        logger.warning("Refresh token not found in any source")
        return None

    def save_refresh_token(self, token_value: str) -> bool:
        """
        Save refresh token to GCS and optionally to Secret Manager.
        Replicates saveRefreshToken function from App Script.
        
        Args:
            token_value: The refresh token to save
            
        Returns:
            True if saved to at least one location successfully
        """
        success = False
        
        # Save to GCS
        gcs_success = self.save_refresh_token_to_gcs(token_value)
        if gcs_success:
            success = True
        
        # # Also save to Secret Manager if configured
        # if self.secret_name:
        #     secret_success = self.store_refresh_token_in_secret_manager(self.secret_name, token_value)
        #     if secret_success:
        #         success = True
        
        return success

    async def run_pipeline(self):
        """Execute the full pipeline for all data types."""
        logger.info("Starting Xero Data Pipeline")
        
        try:
            self.update_xero_access_token()
            # Authenticate with Xero
            # self.refresh_access_token()
            
            # Create BigQuery dataset and tables if needed
            self.create_big_query_dataset_if_not_exists()
            self.create_big_query_table_if_not_exists(self.PurchaseOrders_table_ref, self.PurchaseOrders_columns)
            # self.create_big_query_table_if_not_exists(self.credit_notes_table_ref, self.credit_notes_columns)
            # self.create_big_query_table_if_not_exists(self.profit_loss_table_ref, self.profit_loss_columns)
            
            # Fetch PurchaseOrders
            await self.fetch_PurchaseOrders()
            
            # Fetch credit notes
            # await self.fetch_credit_notes()
            
            # # Fetch profit and loss reports for different date ranges
            # for report_type, date_range in config.REPORT_DATE_RANGES.items():
            #     await self.fetch_profit_and_loss(date_range["from_date"], date_range["to_date"])
            
            # Only delete partitions and upload if we have data
            if self.PurchaseOrders:
                # Get min and max dates for partitioning
                date_column = next((col['name'] for col in self.PurchaseOrders_columns if col['name'] == 'date'), None)
                if date_column:
                    # Find min and max dates for deleting partitions
                    dates = []
                    for record in self.PurchaseOrders:
                        date_idx = [i for i, col in enumerate(self.PurchaseOrders_columns) if col['name'] == 'date'][0]
                        if record[date_idx]:
                            try:
                                # Parse date in whatever format it's in
                                date_obj = parse_date(record[date_idx]).date()
                                dates.append(date_obj)
                            except:
                                pass
                    
                    if dates:
                        min_date = min(dates).strftime('%Y-%m-%d')
                        max_date = max(dates).strftime('%Y-%m-%d')
                        
                        # Delete existing partitions for these dates
                        self.delete_partitions(self.PurchaseOrders_table_ref, date_column, min_date, max_date)
                        
                # Upload data to BigQuery
                column_names = [col['name'] for col in self.PurchaseOrders_columns]
                self.append_data_to_bigquery(self.PurchaseOrders_table_ref, self.PurchaseOrders, column_names)
                        
            # Log final metrics
            self.metrics.log_metrics()
            
            logger.info("Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            self.metrics.errors.append(str(e))
            self.metrics.log_metrics()
            raise
