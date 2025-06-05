import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os
import logging
import json
from google.cloud import bigquery
import pandas as pd
from google_auth import GoogleAdsAuth
from google_ads_api import GoogleAdsAPI
import config
from pytz import timezone
import pytz
import uuid
from logger import LoggerConfig

# Get a configured logger for this module
logger = LoggerConfig.get_logger_from_config(config, __name__)

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
        self.During_period = os.environ.get("DURING_PERIOD")
        columns_json = os.environ.get("COLUMN_DEFINITIONS")
        # Parse custom accounts from environment variable into a list
        raw_custom = os.environ.get("CUSTOME_ACCOUNTS", "")
        if raw_custom:
            # remove brackets and split by comma
            cleaned = raw_custom.strip("[]")
            self.custome_accounts = [acct.strip().strip("'\"\"") for acct in cleaned.split(",") if acct.strip()]
        else:
            self.custome_accounts = []


        # # Initialize configuration from config file
        # self.project_id = getattr(config, "GCP_PROJECT_ID", "")
        # self.dataset_id = getattr(config, "BIGQUERY_DATASET_ID", "")
        # self.client_id = getattr(config, "GOOGLE_ADS_CLIENT_ID", "")
        # self.refresh_token = getattr(config, "GOOGLE_ADS_REFRESH_TOKEN", "")
        # self.developer_token = getattr(config, "GOOGLE_ADS_DEVELOPER_TOKEN", "")
        # self.client_secret = getattr(config, "GOOGLE_ADS_CLIENT_SECRET", "")
        # self.table_id = getattr(config, "BIGQUERY_TABLE_ID", "")
        # self.start_date_days_back = getattr(config, "REFRESH_WINDOW_START_DAYS_BACK", 5)  # Default to 5 if not set
        # self.end_date_days_back = getattr(config, "REFRESH_WINDOW_END_DAYS_BACK", 0)  # Default to 0 if not set
        # self.login_customer_id = getattr(config, "GOOGLE_ADS_LOGIN_CUSTOMER_ID","")
        # self.custome_accounts = getattr(config, "CUSTOME_ACCOUNTS", "")
        # columns_json = getattr(config, "COLUMN_DEFINITIONS")
        # self.During_period = getattr(config,"DURING_PERIOD")
        # print(columns_json)
        
        # Initialize table reference and clients
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.bq_client = bigquery.Client(project=self.project_id)
        self.metrics = GoogleAdsPipelineMetrics()

        # Load column definitions from environment variable or config
        try:
            # Try to get column definitions from environment variable or config
            # columns_json = os.environ.get("COLUMN_DEFINITIONS") or getattr(config, "COLUMN_DEFINITIONS", None)
            if columns_json:
                self.column_definitions = json.loads(columns_json)
                # print(self.column_definitions)
                # print(self.column_definitions)
                logger.info(f"Loaded {len(self.column_definitions)} column definitions from configuration")
            else:
                # Fall back to default columns if not defined
                logger.warning("No column definitions found, using default columns")
                # self.column_definitions = self._get_default_column_definitions()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse column definitions JSON: {str(e)}")
            logger.warning("Using default column definitions")

        self.column_config = self._parse_column_config()
        # print(self.column_config)
        
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
        
        # Generate columns list from config
        # Exclude special configs (table indicators and date-range indicators)
        self.columns = [col_config["name"] for col_config in self.column_config
                        if not col_config.get("is_table", False)]
        
        # Add processed_at column if not in config
        if "processed_at" not in self.columns:
            self.columns.append("processed_at")
            self.column_config.append({
                "name": "processed_at",
                "type": "TIMESTAMP",
                "source_field": "None"  # Special case, will be handled separately
            })
        # # Add unique_id column for each record
        # if "ID" not in self.columns:
        #     self.columns.append("ID")
        #     self.column_config.append({
        #         "name": "ID",
        #         "type": "STRING",
        #         "source_field": "None"  # Special case for UUID
        #     })
        # print(f"Columns: {self.columns}")
        # print(f"Column Config: {self.column_config}")

        logger.info(
            f"Initialized Google Ads pipeline for date range: {self.start_date} to {self.end_date}"
        )
        logger.info(f"Configured with {len(self.columns)} columns")

    def _parse_column_config(self) -> List[Dict[str, Any]]:
        """Parse column configuration from environment variable."""
        try:
            column_config = self.column_definitions
            
            # Validate the configuration
            if not isinstance(column_config, list):
                raise ValueError("COLUMN_CONFIG must be a JSON array")
            
            # Validate each column configuration
            for i, col in enumerate(column_config):
                # Skip validation for special columns like table name indicators
                if col.get("is_table", False):
                    # Only name and type are required for table indicators
                    required_keys = ["name", "type"]
                    for key in required_keys:
                        if key not in col:
                            raise ValueError(f"Table config at index {i} missing required key: {key}")
                    continue
                    
                # Skip validation for date range indicators
                if col.get("is_date_range", False):
                    # For date range, we need name, type, and source_field
                    required_keys = ["name", "type", "source_field"]
                    for key in required_keys:
                        if key not in col:
                            raise ValueError(f"Date range config at index {i} missing required key: {key}")
                    continue
                  # Regular columns need name, type, and source_field
                if "name" not in col:
                    raise ValueError(f"Column config at index {i} missing required key: name")
                    
                if "type" not in col:
                    raise ValueError(f"Column config at index {i} missing required key: type")
                    
                # For regular columns, source_field is required
                if "source_field" not in col:
                    raise ValueError(f"Column config at index {i} missing required key: source_field")
                
                # Validate filter fields if filtered flag is present
                if col.get("filtered") == "true":
                    if "filter_type" not in col:
                        logger.warning(f"Column {col['name']} has filtered=true but missing filter_type")
                    if "filter_value" not in col:
                        logger.warning(f"Column {col['name']} has filtered=true but missing filter_value")
            
            logger.info(f"Successfully parsed {len(column_config)} column configurations")
            return column_config
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse COLUMN_CONFIG as JSON: {str(e)}")
            logger.warning("Using default column configuration")
            # return self._get_default_column_config()

    # def _get_default_column_config(self) -> List[Dict[str, Any]]:
    #     """Return default column configuration in case of parsing error."""
    #     return [
    #         {"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},
    #         {"name": "customer_name", "type": "STRING", "source_field": "customer.descriptiveName"},
    #         {"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},
    #         {"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},
    #         {"name": "clicks", "type": "INTEGER", "source_field": "metrics.clicks"},
    #         {"name": "all_conversions", "type": "FLOAT64", "source_field": "metrics.allConversions"},
    #         {"name": "conversions", "type": "FLOAT64", "source_field": "metrics.conversions"},
    #         {"name": "conversions_value", "type": "FLOAT64", "source_field": "metrics.conversionsValue"},
    #         {"name": "conversions_by_date", "type": "FLOAT64", "source_field": "metrics.conversionsByConversionDate"},
    #         {"name": "conversions_value_by_date", "type": "FLOAT64", "source_field": "metrics.conversionsValueByConversionDate"},
    #         {"name": "date", "type": "DATE", "source_field": "segments.date"},
    #         {"name": "currency_code", "type": "STRING", "source_field": "customer.currencyCode"},
    #         {"name": "timezone", "type": "STRING", "source_field": "customer.timeZone"},
    #         {"name": "cost", "type": "FLOAT64", "source_field": "metrics.costMicros", "transform": "lambda x: float(x) / 1_000_000 if x else 0"},
    #         {"name": "all_conversions_value", "type": "FLOAT64", "source_field": "metrics.allConversionsValue"},
    #         {"name": "impressions", "type": "INTEGER", "source_field": "metrics.impressions"},
    #         {"name": "interactions", "type": "INTEGER", "source_field": "metrics.interactions"},
    #         {"name": "device", "type": "STRING", "source_field": "segments.device"},
    #     ]

    def create_big_query_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist with enhanced logging and error handling."""
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
                dataset.location = "US"  # Default location, consider making this configurable
                
                # Create the dataset
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Created dataset {dataset_ref} - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                
            except Exception as e:
                logger.error(f"Failed to create BigQuery dataset: {str(e)}")
                self.metrics.errors.append(str(e))
                raise

    def create_big_query_table_if_not_exists(self):
        """Create BigQuery table if it doesn't exist with dynamically configured schema."""
        start_time = datetime.now()
        logger.info(f"Checking/creating BigQuery table: {self.table_ref}")

        try:
            # Generate schema from column config, excluding table/date-range indicators
            schema = []
            for col_config in self.column_config:
                if col_config.get("is_table"):
                    continue
                schema.append(
                    bigquery.SchemaField(
                        col_config["name"], 
                        col_config["type"], 
                        mode="NULLABLE"
                    )
                )

            table = bigquery.Table(self.table_ref, schema=schema)
              # Set partitioning and clustering
            date_field = next((col["name"] for col in self.column_config if col["type"] == "DATE"), None)
            
            # Check if date field exists to create partitioned or regular table
            if date_field:
                logger.info(f"Date field '{date_field}' found. Creating partitioned table.")
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field=date_field
                )
                
                # Set clustering fields if date field exists
                clustering_fields = [date_field]
                customer_id_field = next((col["name"] for col in self.column_config if col["name"] == "customer_id"), None)
                if customer_id_field:
                    clustering_fields.append(customer_id_field)
                    
                table.clustering_fields = clustering_fields
            else:
                logger.info("No DATE field found in configuration. Creating regular table without partitioning.")

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
                response = self.ads_api.get_customer_clients(self.login_customer_id)
                
                # Check if the response is an empty result with only fieldMask
                if isinstance(response, dict) and "fieldMask" in response and len(response) == 1:
                    logger.info(f"No client accounts found for login customer ID: {self.login_customer_id}")
                    return []
                
                # If response is valid, proceed as normal
                clients = response
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

    def generate_api_query(self, during_period_value: str = None) -> str:
        """Generate GAQL query dynamically based on column configuration."""
        # Collect all source fields from column config, excluding None and special transformations
        source_fields = []
        table_name = "campaign"  # Default table name
        date_field = "segments.date"  # Default date field
        filters = []  # To store field filters defined in column definitions
        
        for col in self.column_config:
            source_field = col.get("source_field")
            
            # Check for table name in configuration
            if col.get("is_table", False):
                table_name = col.get("name", "campaign")
                logger.info(f"Using table name from config: {table_name}")
                continue
            # Check for unique_id in configuration
            if col.get("is_unique_id", False):
                # table_name = col.get("name", "campaign")
                logger.info(f"Ignore unique_id from config: {table_name}")
                continue
                
            # Check for date range field in configuration
            if col.get("is_date_range", False):
                date_field = col.get("source_field", "segments.date")
                source_fields.append(date_field)
                logger.info(f"Using date range field from config: {date_field}")
                continue
                
            # Extract filter definitions if present
            if col.get("filtered") == "true" and source_field:
                filter_type = col.get("filter_type")
                filter_value = col.get("filter_value")
                if filter_type and filter_value:
                    filter_clause = f"{source_field} {filter_type} '{filter_value}'"
                    filters.append(filter_clause)
                    logger.info(f"Added filter: {filter_clause}")
                
            # Add standard fields to the query
            if source_field and source_field != "None":
                # Keep the complete source field path (with dot notation)
                # Google Ads requires the exact field path
                source_fields.append(source_field)
        
        # Remove duplicates while preserving order
        unique_source_fields = []
        for field in source_fields:
            if field not in unique_source_fields:
                unique_source_fields.append(field)
        
        # Build SELECT clause from source fields
        select_clause = "SELECT\n    " + ",\n    ".join(unique_source_fields)
        # Use the provided during_period_value if specified, otherwise use self.During_period
        active_during_period = during_period_value if during_period_value else self.During_period
        
        # Build the WHERE clause with date range
        if active_during_period:
            during_period = f"WHERE {date_field} DURING {active_during_period}"
        else:
            during_period = f"WHERE {date_field} BETWEEN '{self.start_date}' AND '{self.end_date}'"
        
        # Build the filters clause by joining all filter conditions with AND
        filters_clause = ""
        if filters:
            filters_clause = "AND " + " AND ".join(filters)
            logger.info(f"Applied filters: {filters_clause}")
        
        # Build the complete query
        query = f"""
{select_clause}
FROM {table_name}
{during_period}
{filters_clause}
ORDER BY {date_field} ASC
LIMIT 10000
"""
        logger.debug(f"Generated API query: {query}")
        # print(f"Generated API query: {query}")
        return query

    async def fetch_campaign_data(self, customer_id: str) -> List[List[Any]]:
        """Fetch campaign data for a specific customer and prepare it for BigQuery appending."""
        try:
            self.metrics.api_calls += 1
            all_processed_data = []

            # Check if we have multiple DURING periods
            if self.During_period and "," in self.During_period:
                # Split by comma and strip whitespace
                during_periods = [period.strip() for period in self.During_period.split(",")]
                logger.info(f"Processing multiple DURING periods: {during_periods}")
                
                # Iterate through each period
                for period in during_periods:
                    logger.info(f"Fetching data for DURING period: {period}")
                    # Generate query with specific during period
                    query = self.generate_api_query(during_period_value=period)
                    
                    # Fetch data from the API
                    api_response = self.ads_api.search(
                        customer_id=customer_id,
                        query=query
                    )
                    
                    # Log the raw response for debugging
                    logger.debug(f"Raw API response for customer {customer_id} and period {period}: {api_response}")
                    
                    # Check if the response only contains fieldMask (no activities case)
                    if "fieldMask" in api_response and len(api_response) == 1:
                        logger.info(f"No activities found for customer {customer_id} in period {period}")
                        continue  # Skip to next period
                        
                    # Validate the response type
                    if not isinstance(api_response, dict) or "results" not in api_response:
                        logger.warning(f"Invalid API response format for period {period}: {type(api_response)}")
                        continue  # Skip to next period

                    period_results = api_response.get("results", [])

                    if not isinstance(period_results, list):
                        logger.warning(f"'results' should be a list but got {type(period_results)} for period {period}")
                        continue  # Skip to next period
                    
                    # Process results for this period
                    period_processed_data = self._process_results(period_results, customer_id)
                    all_processed_data.extend(period_processed_data)
                    
                    logger.info(f"Fetched {len(period_processed_data)} records for period '{period}'")
            else:
                # Generate dynamic query based on column config
                query = self.generate_api_query()

                # Fetch data from the API
                api_response = self.ads_api.search(
                    customer_id=customer_id,
                    query=query
                )

                # Log the raw response for debugging
                logger.debug(f"Raw API response for customer {customer_id}: {api_response}")
                
                # Check if the response only contains fieldMask (no activities case)
                if "fieldMask" in api_response and len(api_response) == 1:
                    logger.info(f"No activities found for customer {customer_id}")
                    return []
                    
                # Validate the response type
                if not isinstance(api_response, dict) or "results" not in api_response:
                    raise ValueError(f"Invalid API response format: {type(api_response)}")

                results = api_response.get("results", [])

                if not isinstance(results, list):
                    raise ValueError(f"'results' should be a list but got {type(results)}")
                
                # Process the results
                all_processed_data = self._process_results(results, customer_id)
            
            # Log overall results
            self.metrics.successful_api_calls += 1
            self.metrics.records_processed += len(all_processed_data)
            logger.info(
                f"Processed {len(all_processed_data)} total records for customer {customer_id}"
            )

            return all_processed_data

        except Exception as ex:
            logger.error(f"Error fetching data for customer {customer_id}: {ex}")
            self.metrics.failed_api_calls += 1
            self.metrics.errors.append(str(ex))
            raise
            
    def _process_results(self, results: List[Dict[str, Any]], customer_id: str) -> List[List[Any]]:
        """Process API results into rows ready for BigQuery insertion."""
        processed_data = []
        
        for result in results:
            try:
                record_uuid = str(uuid.uuid4())  # Generate a unique UUID for each record
                
                # Process each result according to column configuration
                processed_row = []
                
                for col_config in self.column_config:
                    # Skip table and date-range indicator configs
                    if col_config.get("is_table"):
                        continue
                    col_name = col_config["name"]
                    source_field = col_config.get("source_field")

                    # Special case for unique_id
                    if col_name == "ID" and source_field == "None":
                        processed_row.append(record_uuid)
                        # print(f"Generated unique ID: {record_uuid}")
                        continue
                    
                    # Special case for processed_at
                    if col_name == "processed_at" and source_field == "None":
                        processed_row.append(datetime.now().isoformat())
                        continue
                        
                    # Dynamically extract value based on source field path
                    value = self._extract_value_from_result(result, source_field, col_config)
                    processed_row.append(value)
                
                processed_data.append(processed_row)
                
            except Exception as row_ex:
                logger.warning(
                    f"Failed to process a result for customer {customer_id}: {row_ex}"
                )
                continue  # Skip problematic row and proceed
                
        return processed_data

    def _convert_snake_to_camel(self, path_part: str) -> str:
        """
        Convert a snake_case string to camelCase.
        
        Examples:
            descriptive_name -> descriptiveName
            cost_micros -> costMicros
            all_conversions_from_interactions_rate -> allConversionsFromInteractionsRate
            app_campaign_setting -> appCampaignSetting
        """
        if "_" not in path_part:
            return path_part
            
        segments = path_part.split("_")
        result = segments[0]  # Keep first segment as is
        
        # Convert remaining segments to capitalized form
        for segment in segments[1:]:
            if segment:  # Handle potential empty segments
                result += segment[0].upper() + segment[1:]
                
        return result
        
    def _extract_value_from_result(self, result: Dict[str, Any], source_field: str, col_config: Dict[str, Any]) -> Any:
        """Extract and transform value from result based on field path and Google Ads API structure."""
        if not source_field:
            return None
            
        # Split the field path by dot notation
        path_parts = source_field.split('.')
        
        # Standardize path part names according to the API response format
        normalized_path = []
        for part in path_parts:
            # Convert snake_case to camelCase
            normalized_part = self._convert_snake_to_camel(part)
            normalized_path.append(normalized_part)
            
        logger.debug(f"Normalized path: {normalized_path} from source field: {source_field}")
        
        # Navigate through the result to extract the value
        current = result
        for part in normalized_path:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                # If part not found, log the issue for debugging
                logger.debug(f"Field part '{part}' not found in result. Available keys: {list(current.keys()) if isinstance(current, dict) else 'Not a dict'}")
                return self._get_default_value(col_config["type"])
                
        # Apply transformation if specified
        if "transform" in col_config:
            try:
                # Using eval for transformation is not ideal for production
                # In a real environment, consider using a safer approach
                transform_func = eval(col_config["transform"])
                return transform_func(current)
            except Exception as e:
                logger.warning(f"Error applying transformation: {e}")
                return current
                
        # Apply type conversion based on BigQuery type
        return self._convert_value_based_on_type(current, col_config["type"])

    def _get_default_value(self, bq_type: str) -> Any:
        """Get default value based on BigQuery type."""
        type_defaults = {
            "INTEGER": 0,
            "FLOAT64": 0.0,
            "STRING": "N/A",
            "TIMESTAMP": None,
            "DATE": None,
            "BOOLEAN": False
        }
        return type_defaults.get(bq_type, None)
        
    def _convert_value_based_on_type(self, value: Any, bq_type: str) -> Any:
        """Convert value to appropriate type based on BigQuery type."""
        if value is None:
            return self._get_default_value(bq_type)
            
        try:
            if bq_type == "INTEGER":
                return int(value)
            elif bq_type == "FLOAT64":
                return float(value)
            elif bq_type == "STRING":
                return str(value)
            elif bq_type == "TIMESTAMP":
                # Return as ISO format string
                if isinstance(value, datetime):
                    return value.isoformat()
                return value
            elif bq_type == "BOOLEAN":
                return bool(value)
            else:
                return value
        except Exception as e:
            logger.warning(f"Error converting value {value} to {bq_type}: {e}")
            return self._get_default_value(bq_type)

    def delete_partitions(self):
        """Delete existing partitions for the date range or based on DURING periods."""
        start_time = datetime.now()
        
        # Find date field from column configuration
        date_field = next((col["name"] for col in self.column_config if col["type"] == "DATE"), None)
        
        if not date_field:
            logger.warning("No DATE field found in configuration. Table was likely created as a regular table without partitioning.")
            logger.info("Skipping partition deletion as this is not a partitioned table.")
            return
        
        # Calculate date range from DURING periods if available
        start_date = self.start_date
        end_date = self.end_date
        
        if self.During_period:
            # Split by comma in case there are multiple periods
            during_periods = [period.strip() for period in self.During_period.split(",")]
            calculated_dates = []
            
            for period in during_periods:
                period_start, period_end = self._calculate_dates_from_period(period)
                if period_start and period_end:
                    calculated_dates.append((period_start, period_end))
                    logger.info(f"Calculated date range for '{period}': {period_start} to {period_end}")
            # If we have calculated dates, find the min start date and max end date
            if calculated_dates:
                start_date = min(date[0] for date in calculated_dates)
                end_date = max(date[1] for date in calculated_dates)
                logger.info(f"Using combined date range from DURING periods: {start_date} to {end_date}")
        
        logger.info(f"Deleting partitions for date range: {start_date} to {end_date}")
        
        try:
            query = f"""
            DELETE FROM `{self.table_ref}`
            WHERE DATE({date_field}) BETWEEN "{start_date}" AND "{end_date}"
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

    def _calculate_dates_from_period(self, period: str) -> tuple:
        """
        Calculate start and end dates from a Google Ads DURING period string.
        
        Args:
            period: A string like "LAST_7_DAYS", "YESTERDAY", "TODAY", etc.
        
        Returns:
            A tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        # Use the timezone from self.now which is already in GMT+12
        today = self.now.date()
        
        if period == "TODAY":
            return today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
        
        elif period == "YESTERDAY":
            yesterday = today - timedelta(days=1)
            return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
        
        elif period.startswith("LAST_") and period.endswith("_DAYS"):
            try:
                # Extract the number of days from "LAST_X_DAYS"
                days = int(period.split("_")[1])
                yesterday = today - timedelta(days=1)
                start_date = today - timedelta(days=days)  # -1 because LAST_7_DAYS includes today
                return start_date.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
            except (IndexError, ValueError):
                logger.warning(f"Could not parse days from period: {period}")
                return None, None
        
        elif period == "THIS_WEEK_SUN_TODAY":
            # Find the previous Sunday
            days_since_sunday = today.weekday() + 1  # Python: Monday=0, Sunday=6, so +1
            if days_since_sunday == 7:  # If today is Sunday
                start_date = today
            else:
                start_date = today - timedelta(days=days_since_sunday)
            return start_date.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
        
        elif period == "THIS_WEEK_MON_TODAY":
            # Find the previous Monday
            days_since_monday = today.weekday()
            if days_since_monday == 0:  # If today is Monday
                start_date = today
            else:
                start_date = today - timedelta(days=days_since_monday)
            return start_date.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
        
        elif period == "LAST_WEEK":
            # Last week is the 7 days before this week
            start_of_this_week = today - timedelta(days=today.weekday())
            end_of_last_week = start_of_this_week - timedelta(days=1)
            start_of_last_week = end_of_last_week - timedelta(days=6)
            return start_of_last_week.strftime("%Y-%m-%d"), end_of_last_week.strftime("%Y-%m-%d")
        
        elif period == "LAST_BUSINESS_WEEK":
            # Last business week is Monday-Friday of the previous week
            days_since_monday = today.weekday()
            last_friday = today - timedelta(days=(days_since_monday + 3))  # Go back to last Friday
            last_monday = last_friday - timedelta(days=4)  # Go back 4 days to get to Monday
            return last_monday.strftime("%Y-%m-%d"), last_friday.strftime("%Y-%m-%d")
        
        elif period == "LAST_MONTH":
            # Last month
            first_of_this_month = today.replace(day=1)
            last_of_last_month = first_of_this_month - timedelta(days=1)
            first_of_last_month = last_of_last_month.replace(day=1)
            return first_of_last_month.strftime("%Y-%m-%d"), last_of_last_month.strftime("%Y-%m-%d")
        
        else:
            logger.warning(f"Unsupported period format: {period}, falling back to configured date range")
            return None, None

    def append_data_to_bigquery(self, data: List[List[Any]]):
        """Append data to BigQuery."""
        if not data:
            logger.warning("No data to append to BigQuery")
            return
        self.validate_and_update_schema(data)
        start_time = datetime.now()
        logger.info(f"Appending {len(data)} rows to BigQuery")

        try:
            df = pd.DataFrame(data, columns=self.columns)

            # Convert data types based on schema configuration
            for col_config in self.column_config:
                col_name = col_config["name"]
                col_type = col_config["type"]
                
                if col_name in df.columns:
                    # Handle DATE and TIMESTAMP with UTC awareness
                    if col_type == "DATE" or col_type == "TIMESTAMP":
                        df[col_name] = pd.to_datetime(df[col_name], errors="coerce", utc=True)
                    elif col_type == "INTEGER":
                        df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype('Int64')
                    elif col_type == "FLOAT64":
                        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                    elif col_type == "BOOLEAN":
                        df[col_name] = df[col_name].astype('boolean')

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

            # Define the new schema based on column configuration, excluding table/date-range indicators
            new_schema = {}
            for col_config in self.column_config:
                if col_config.get("is_table"):
                    continue
                col_name = col_config["name"]
                # Convert FLOAT64 to FLOAT to match BigQuery's expected type
                col_type = "FLOAT" if col_config["type"] == "FLOAT64" else col_config["type"]
                new_schema[col_name] = bigquery.SchemaField(
                    col_name, col_type, mode="NULLABLE"
                )
            
                # print(f"New schema field: {col_name} of type {col_type}")

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