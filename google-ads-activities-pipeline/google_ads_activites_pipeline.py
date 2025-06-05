import asyncio
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os
import logging
import logging.handlers
import json
from google.cloud import bigquery
import pandas as pd
from google_auth import GoogleAdsAuth
from google_ads_api import GoogleAdsAPI
import config
from pytz import timezone
import pytz
import random
import string

def setup_logging():
    """Configure advanced logging with custom formatting and handlers."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

class GoogleAdsPipelineMetrics:
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

class GoogleAdsPipeline:
    def __init__(self):
        """Initialize the pipeline with necessary clients and configurations."""
        # Environment variables
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
        self.table_id = os.environ.get("BIGQUERY_TABLE_ID")
        self.client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID")
        self.client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")
        self.developer_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
        self.refresh_token = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
        self.login_customer_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        self.start_date_days_back = int(os.environ.get("REFRESH_WINDOW_START_DAYS_BACK"))
        self.end_date_days_back = int(os.environ.get("REFRESH_WINDOW_END_DAYS_BACK"))
        self.API_limit = os.environ.get("API_LIMIT")
        self.During_period = os.environ.get("DURING_PERIOD")

        # """Initialize the pipeline with necessary clients and configurations."""
        # # Initialize configuration from config file
        # self.project_id = config.GCP_PROJECT_ID
        # self.dataset_id = config.BIGQUERY_DATASET_ID
        # self.client_id = config.GOOGLE_ADS_CLIENT_ID
        # self.refresh_token = config.GOOGLE_ADS_REFRESH_TOKEN
        # self.developer_token = config.GOOGLE_ADS_DEVELOPER_TOKEN
        # self.client_secret = config.GOOGLE_ADS_CLIENT_SECRET
        # self.table_id = config.BIGQUERY_TABLE_ID
        # self.start_date_days_back = config.REFRESH_WINDOW_START_DAYS_BACK
        # self.end_date_days_back = config.REFRESH_WINDOW_END_DAYS_BACK
        # self.login_customer_id = config.GOOGLE_ADS_LOGIN_CUSTOMER_ID
        # self.API_limit = config.API_LIMIT
        # self.During_period = config.DURING_PERIOD

        # Initialize clients and configurations
        # self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        # self.bq_client = bigquery.Client(project=self.project_id)
        # self.metrics = GoogleAdsPipelineMetrics()

        # Initialize clients and configurations
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.bq_client = bigquery.Client(project=self.project_id)
        self.metrics = GoogleAdsPipelineMetrics()
        
        # Initialize Google Ads authentication and API client
        self.auth = GoogleAdsAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.refresh_token
        )
        
        self.ads_api = GoogleAdsAPI(
            auth=self.auth,
            developer_token=self.developer_token,
            login_customer_id=self.login_customer_id
        )

        # Calculate date range with timezone
        gmt_plus_12 = timezone("Etc/GMT-12")  # Define the +12 GMT timezone
        self.now = datetime.now(gmt_plus_12)  # Set now to current time in +12 GMT
        self.start_date = (self.now - timedelta(days=self.start_date_days_back)).strftime("%Y-%m-%d")
        self.end_date = (self.now - timedelta(days=self.end_date_days_back)).strftime("%Y-%m-%d")
        
        self.columns = [
            "ID",
            "Customer ID",
            "DateTime",
            "User Email",
            "Customer Name",
            "Action Taken",
            "Campaign Name",
            "Changed Field",
            "Matched Field Value",
            "Action Type",
            "Ad Group",
            "Tool"
        ]

        logger.info(
            f"Initialized Google Ads pipeline for date range: {self.start_date} to {self.end_date}"
        )

    def create_big_query_table_if_not_exists(self):
        """Create BigQuery table if it doesn't exist."""
        start_time = datetime.now()
        logger.info(f"Checking/creating BigQuery table: {self.table_ref}")

        try:
            schema = [
                bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Customer ID", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("DateTime", "TIMESTAMP", mode="NULLABLE"),
                # bigquery.SchemaField("campaign_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("User Email", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Customer Name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Action Taken", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Campaign Name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Matched Field Value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Action Type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Ad Group", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Tool", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
            ]

            table = bigquery.Table(self.table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="DateTime"
            )
            table.clustering_fields = ["DateTime", "ID"]

            self.bq_client.create_table(table, exists_ok=True)
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Table {self.table_ref} is ready - Duration: {duration:.2f}s")
            self.metrics.bigquery_operations += 1

        except Exception as e:
            logger.error(f"Failed to create BigQuery table: {str(e)}")
            self.metrics.errors.append(str(e))
            raise

    async def get_customer_ids(self) -> List[str]:
        """Get list of customer IDs from the customer manager account."""
        try:
            customer_ids = []
            logger.debug("Starting to fetch customer IDs.")

            # If this is an MCC account, get all client accounts
            if self.login_customer_id:
                logger.debug(
                    f"Fetching client accounts for login customer ID: {self.login_customer_id}"
                )
                clients = self.ads_api.get_customer_clients(self.login_customer_id)
                logger.debug(
                    f"Received {len(clients)} clients: {clients[:5]} (sample)"
                )  # Log sample for debugging

                # Filter out the login customer ID
                customer_ids = [
                    client_id
                    for client_id in clients
                    if client_id != self.login_customer_id
                ]

            logger.info(f"Found {len(customer_ids)} customer IDs.")
            if customer_ids:
                logger.debug(f"Sample of customer IDs: {customer_ids[:5]}")
            return customer_ids

        except Exception as ex:
            logger.error(f"Failed to get customer IDs: {ex}")
            self.metrics.errors.append(str(ex))
            raise

    async def fetch_activity_data(self, customer_id: str) -> List[List[Any]]:
        """Fetch campaign data for a specific customer and prepare it for BigQuery appending."""
        try:
            self.metrics.api_calls += 1
            existing_ids = set()  # Track existing IDs

            # Fetch data from the API
            api_response = self.ads_api.get_customer_activity(
                customer_id=customer_id,
                start_date=self.start_date,
                end_date=self.end_date,
                API_limit=self.API_limit,
                During_period=self.During_period
            )

            # Check if the response only contains fieldMask (no activities case)
            if "fieldMask" in api_response and len(api_response) == 1:
                logger.info(f"No activities found for customer {customer_id}")
                return []

            # Validate the response
            if not isinstance(api_response, dict) or "results" not in api_response:
                raise ValueError(f"Invalid API response format: {type(api_response)}")

            results = api_response.get("results", [])
            if not isinstance(results, list):
                raise ValueError(f"'results' should be a list but got {type(results)}")

            processed_data = []
            for result in results:
                try:
                    change_event = result.get("changeEvent", {})
                    campaign = result.get("campaign", {})
                    customer = result.get("customer", {})
                    new_resource = change_event.get("newResource", {})

                    # Process changed fields
                    changed_fields = change_event.get("changedFields", "").split(',')
                    changed_fields = [field.split('.')[-1].strip() for field in changed_fields]

                    # Flatten the new resource object
                    flat_object, second_level_keys = flatten_object(new_resource)

                    # Generate unique ID
                    unique_id = generate_unique_id(existing_ids)
                    existing_ids.add(unique_id)

                    # Process each changed field
                    for field in changed_fields:
                        matched_field_value = ''
                        if field in second_level_keys:
                            matched_field_value = str(second_level_keys[field])
                        else:
                            matched_field_value = f"New Resources: {json.dumps(second_level_keys)}"

                        processed_row = [
                            unique_id,
                            format_customer_id(customer_id),
                            change_event.get("changeDateTime"),
                            change_event.get("userEmail"),
                            customer.get("descriptiveName"),
                            change_event.get("resourceChangeOperation"),
                            campaign.get("name", ""),
                            field,
                            matched_field_value,
                            change_event.get("changeResourceType"),
                            change_event.get("adGroup", ""),
                            change_event.get("clientType")
                        ]
                        processed_data.append(processed_row)

                except Exception as row_ex:
                    logger.warning(f"Failed to process a result for customer {customer_id}: {row_ex}")
                    continue

            # Log results
            self.metrics.successful_api_calls += 1
            self.metrics.records_processed += len(processed_data)
            logger.info(f"Processed {len(processed_data)} records for customer {customer_id}")
            # print(processed_data)

            return processed_data

        except Exception as ex:
            logger.error(f"Error fetching data for customer {customer_id}: {ex}")
            self.metrics.failed_api_calls += 1
            self.metrics.errors.append(str(ex))
            raise

    def delete_partitions(self):
        """Delete existing partitions for the date range."""
        start_time = datetime.now()
        logger.info(
            f"Deleting partitions for date range: {self.start_date} to {self.end_date}"
        )

        try:
            query = f"""
            DELETE FROM `{self.table_ref}`
            WHERE DATE(DateTime) BETWEEN "{self.start_date}" AND "{self.end_date}"
            """

            query_job = self.bq_client.query(query)
            result = query_job.result()

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Deleted partitions successfully - Duration: {duration:.2f}s")
            self.metrics.bigquery_operations += 1

        except Exception as e:
            logger.error(f"Failed to delete partitions: {str(e)}")
            self.metrics.errors.append(str(e))
            raise

    def append_data_to_bigquery(self, data: List[List[Any]]):
            """Append data to BigQuery."""
            if not data:
                logger.warning("No data to append to BigQuery")
                return

            self.validate_and_update_schema(data)  # Validate and update schema if necessary

            start_time = datetime.now()
            logger.info(f"Appending {len(data)} rows to BigQuery")

            try:
                # Create DataFrame with the new column structure
                df = pd.DataFrame(data, columns=[
                    "ID", "Customer ID", "DateTime", "User Email", "Customer Name",
                    "Action Taken", "Campaign Name", "Changed Field", "Matched Field Value",
                    "Action Type", "Ad Group", "Tool"
                ])

                # Convert date and timestamp columns
                df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
                df["processed_at"] = datetime.now(pytz.UTC)  # Add processing timestamp

                # Convert ID to string (since it's an alphanumeric unique identifier)
                df["ID"] = df["ID"].astype(str)

                # Ensure other columns are the correct type
                df["Customer ID"] = df["Customer ID"].astype(str)  # Keep as string since it includes dashes
                df["User Email"] = df["User Email"].astype(str)
                df["Customer Name"] = df["Customer Name"].astype(str)
                df["Action Taken"] = df["Action Taken"].astype(str)
                df["Campaign Name"] = df["Campaign Name"].astype(str)
                df["Changed Field"] = df["Changed Field"].astype(str)
                df["Matched Field Value"] = df["Matched Field Value"].astype(str)
                df["Action Type"] = df["Action Type"].astype(str)
                df["Ad Group"] = df["Ad Group"].astype(str)
                df["Tool"] = df["Tool"].astype(str)

                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                job = self.bq_client.load_table_from_dataframe(
                    df, self.table_ref, job_config=job_config
                )

                result = job.result()
                duration = (datetime.now() - start_time).total_seconds()

                logger.info(
                    f"Successfully appended {len(df)} rows to {self.table_ref} - "
                    f"Duration: {duration:.2f}s"
                )
                self.metrics.bigquery_operations += 1

            except Exception as e:
                logger.error(f"Failed to append data to BigQuery: {str(e)}")
                self.metrics.errors.append(str(e))
                raise

    def validate_and_update_schema(self, data: List[List[Any]]):
        """
        Check if the BigQuery table schema matches the new data schema.
        If not, update the schema with the new data schema.
        """
        logger.info("Validating BigQuery table schema...")
        try:
            # Fetch the existing table schema
            table = self.bq_client.get_table(self.table_ref)
            existing_schema = {field.name: field for field in table.schema}

            # Define the new schema for Google Ads activity data
            new_schema = {
                "ID": bigquery.SchemaField(
                    "ID", "STRING", mode="REQUIRED"  # Unique identifier
                ),
                "Customer ID": bigquery.SchemaField(
                    "Customer ID", "STRING", mode="REQUIRED"  # Formatted customer ID with dashes
                ),
                "DateTime": bigquery.SchemaField(
                    "DateTime", "TIMESTAMP", mode="REQUIRED"
                ),
                "User Email": bigquery.SchemaField(
                    "User Email", "STRING", mode="NULLABLE"
                ),
                "Customer Name": bigquery.SchemaField(
                    "Customer Name", "STRING", mode="NULLABLE"
                ),
                "Action Taken": bigquery.SchemaField(
                    "Action Taken", "STRING", mode="NULLABLE"
                ),
                "Campaign Name": bigquery.SchemaField(
                    "Campaign Name", "STRING", mode="NULLABLE"
                ),
                "Changed Field": bigquery.SchemaField(
                    "Changed Field", "STRING", mode="NULLABLE"
                ),
                "Matched Field Value": bigquery.SchemaField(
                    "Matched Field Value", "STRING", mode="NULLABLE"
                ),
                "Action Type": bigquery.SchemaField(
                    "Action Type", "STRING", mode="NULLABLE"
                ),
                "Ad Group": bigquery.SchemaField(
                    "Ad Group", "STRING", mode="NULLABLE"
                ),
                "Tool": bigquery.SchemaField(
                    "Tool", "STRING", mode="NULLABLE"
                ),
                "processed_at": bigquery.SchemaField(
                    "processed_at", "TIMESTAMP", mode="REQUIRED"
                )
            }

            # Convert table.schema to a mutable list before making modifications
            new_schema_list = list(table.schema)

            # Check for schema differences
            schema_updated = False

            for field_name, new_field in new_schema.items():
                if field_name not in existing_schema:
                    logger.info(f"Adding new field to schema: {field_name}")
                    new_schema_list.append(new_field)
                    schema_updated = True
                elif existing_schema[field_name].field_type != new_field.field_type:
                    logger.warning(
                        f"Schema mismatch for field '{field_name}': "
                        f"Existing type: {existing_schema[field_name].field_type}, "
                        f"New type: {new_field.field_type}"
                    )
                    raise ValueError(f"Field type mismatch for '{field_name}' in schema.")

            # Update the schema if required
            if schema_updated:
                table.schema = new_schema_list
                self.bq_client.update_table(table, ["schema"])
                logger.info("BigQuery table schema updated successfully.")

        except Exception as e:
            logger.error(f"Failed to validate/update BigQuery schema: {str(e)}")
            self.metrics.errors.append(str(e))
            raise
    
def format_customer_id(customer_id: str) -> str:
    """Format customer ID with dashes after third and sixth characters."""
    customer_id_str = str(customer_id)
    return f"{customer_id_str[:3]}-{customer_id_str[3:6]}-{customer_id_str[6:]}"

def generate_unique_id(existing_ids: set[str]) -> str:
    """Generate a unique 8-character alphanumeric ID."""
    characters = string.ascii_letters + string.digits
    while True:
        unique_id = ''.join(random.choice(characters) for _ in range(8))
        if unique_id not in existing_ids:
            return unique_id

def flatten_object(obj: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Flatten nested object and extract second-level keys."""
    flat_object = {}
    second_level_keys = {}
    
    def process_object(current_obj: Dict[str, Any], prefix: str = ""):
        for key, value in current_obj.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                process_object(value, new_key)
                second_level_keys[key] = value
            else:
                flat_object[new_key] = value
                if "." in new_key:
                    base_key = new_key.split(".")[-1]
                    second_level_keys[base_key] = value
    
    process_object(obj)
    return flat_object, second_level_keys

