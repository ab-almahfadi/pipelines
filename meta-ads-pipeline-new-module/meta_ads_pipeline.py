import asyncio
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os
import json
from google.cloud import bigquery
import pandas as pd
import aiohttp
import config
import requests
from google.cloud import run_v2
from logger import LoggerConfig


# Get a configured logger for this module
logger = LoggerConfig.get_logger_from_config(config, __name__)

class MetaAdsPipelineMetrics:
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
        logger.info(f"Pipeline metrics: {json.dumps(metrics, indent=1)}")

class MetaAdsPipeline:
    def __init__(self):
        """Initialize the pipeline with necessary clients and configurations."""

        """Initialize the pipeline with necessary clients and configurations."""
        # Add Cloud Run specific environment variables
        self.app_id = os.environ.get("META_APP_ID")
        self.app_secret = os.environ.get("META_APP_SECRET")
        self.access_token = os.environ.get("META_ACCESS_TOKEN")
        self.business_id = os.environ.get("META_BUSINESS_ID")
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
        self.table_id = os.environ.get("BIGQUERY_TABLE_ID")
        self.job_name = os.environ.get("CLOUD_RUN_JOB_NAME", "unknown")
        self.start_date_days_back = int(os.environ.get("REFRESH_WINDOW_START_DAYS_BACK"))
        self.end_date_days_back = int(os.environ.get("REFRESH_WINDOW_END_DAYS_BACK"))
        self.batch_size = int(os.environ.get("ACCOUNTS_BATCH_SIZE", 1000))
        self.max_retries = int(os.environ.get("MAX_RETRIES", 20))
        self.batch_days = int(os.environ.get("BATCH_DAYS", 21))  # Number of days per batch
        self.account_delay = int(os.environ.get("ACCOUNT_DELAY_SECONDS", 0))  # Delay between accounts
        self.batch_delay = int(os.environ.get("BATCH_DELAY_SECONDS", 0))  # Delay between batches
        self.endpoint = os.environ.get("ENDPOINT", "insights")
        columns_json = os.environ.get("COLUMN_DEFINITIONS")
        max_requests = int(os.environ.get("MAX_REQUESTS_PER_HOUR", 1000)) 
        cooldown_minutes = int(os.environ.get("COOLDOWN_MINUTES", 5))
        # Parse custom accounts from environment variable into a list
        raw_custom = os.environ.get("CUSTOME_ACCOUNTS", "")
        if raw_custom:
            # remove brackets and split by comma
            cleaned = raw_custom.strip("[]")
            self.custome_accounts = [acct.strip().strip("'\"\"") for acct in cleaned.split(",") if acct.strip()]
        else:
            self.custome_accounts = []
        self.limit = os.environ.get("REQUEST_LIMITATION", 1000)

        # # Add Cloud Run specific environment variables
        # self.app_id = config.META_APP_ID
        # self.app_secret = config.META_APP_SECRET
        # self.access_token = config.META_ACCESS_TOKEN
        # self.business_id = config.META_BUSINESS_ID
        # self.project_id = config.GCP_PROJECT_ID
        # self.dataset_id = config.BIGQUERY_DATASET_ID
        # self.table_id = config.BIGQUERY_TABLE_ID
        # self.job_name = config.CLOUD_RUN_JOB_NAME
        # self.start_date_days_back = config.REFRESH_WINDOW_START_DAYS_BACK
        # self.end_date_days_back = config.REFRESH_WINDOW_END_DAYS_BACK
        # self.batch_size = config.ACCOUNTS_BATCH_SIZE
        # self.max_retries = config.MAX_RETRIES
        # self.batch_days = config.BATCH_DAYS  # Number of days per batch
        # self.account_delay = config.ACCOUNT_DELAY_SECONDS  # Delay between accounts
        # self.batch_delay = config.BATCH_DELAY_SECONDS  # Delay between batches
        # self.endpoint = getattr(config, "ENDPOINT", "")
        # columns_json = getattr(config, "COLUMN_DEFINITIONS", None)
        # max_requests = getattr(config, "MAX_REQUESTS_PER_HOUR", 1000)
        # cooldown_minutes = getattr(config, "COOLDOWN_MINUTES", 5)
        # self.custome_accounts = getattr(config, "CUSTOME_ACCOUNTS", "")
        # self.limit = getattr(config,"REQUEST_LIMITATION", 1000)

        self.rate_limiter = APIRateLimitTracker(max_requests, cooldown_minutes)
        
        # Load column definitions from environment variable or config
        try:
            # Try to get column definitions from environment variable or config
            # columns_json = os.environ.get("COLUMN_DEFINITIONS") or getattr(config, "COLUMN_DEFINITIONS", None)
            if columns_json:
                self.column_definitions = json.loads(columns_json)
                # print(self.column_definitions)
                logger.info(f"Loaded {len(self.column_definitions)} column definitions from configuration")
            else:
                # Fall back to default columns if not defined
                logger.warning("No column definitions found, using default columns")
                # self.column_definitions = self._get_default_column_definitions()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse column definitions JSON: {str(e)}")
            logger.warning("Using default column definitions")
            # self.column_definitions = self._get_default_column_definitions()

        # Extract breakdown fields from column definitions
        column_breakdown_fields = {col.get('source_field') for col in self.column_definitions 
                                if col.get('is_breakdown', False)}
        # print(column_breakdown_fields)
        
        # Set breakdowns based on column definitions
        self.breakdowns = ",".join(column_breakdown_fields) if column_breakdown_fields else ""
        logger.info(f"Using breakdowns from column definitions: {self.breakdowns}")
        
        # Initialize other components
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.client = bigquery.Client(project=self.project_id)
        # self.batch_size = 15
        # self.max_retries = 3
        self.organized_data = []
        self.metrics = MetaAdsPipelineMetrics()
        
        # Calculate date range
        self.now = datetime.now()
        self.start_date = (self.now - timedelta(days=self.start_date_days_back)).strftime("%Y-%m-%d")
        self.end_date = (self.now - timedelta(days=self.end_date_days_back)).strftime("%Y-%m-%d")
        
        # Extract column names from definitions
        self.columns = [col['name'] for col in self.column_definitions]
        
        # Generate API fields string based on column definitions
        self.api_fields = self._generate_api_fields()
        
        logger.info(
            f"Initialized pipeline for date range: {self.start_date} to {self.end_date} "
            f"(Job: {self.job_name})"
        )

    def _generate_api_fields(self) -> str:
        """Generate the fields parameter for Meta Ads API based on column definitions.
        Exclude fields that are marked as breakdowns and handle deeply nested fields."""
        api_fields = set()
        nested_fields = {}
        
        for col in self.column_definitions:
            # Skip auto-generated fields and breakdown fields
            if col.get('auto_generate') or col.get('is_breakdown', False):
                continue
                
            # Handle nested fields (e.g., actions.value)
            source_field = col.get('source_field', '')
            
            # Handle deeply nested fields with path_to_value
            if col.get('deep_nested') and col.get('path_to_value'):
                # Extract the top-level field and nested structure from the path
                path_segments = col.get('path_to_value').split('.')
                if path_segments:
                    top_level = path_segments[0]
                    
                    # Add the top-level field to the main fields list
                    api_fields.add(top_level)
                    
                    # For paths with more than 1 level, build the nested structure
                    if len(path_segments) > 1:
                        # Extract nested fields, excluding array indices
                        nested_path = []
                        for segment in path_segments[1:]:
                            if not segment.isdigit():  # Skip array indices
                                nested_path.append(segment)
                        
                        # Initialize the nested structure for this top-level field if needed
                        if top_level not in nested_fields:
                            nested_fields[top_level] = set()
                        
                        # Add the last segment as a field to fetch within the nested structure
                        if nested_path:
                            nested_fields[top_level].add(nested_path[-1])
            # Handle regular fields with dot notation
            elif '.' in source_field:
                base_field = source_field.split('.')[0]
                api_fields.add(base_field)
            else:
                api_fields.add(source_field)
        
        # Ensure no empty strings in the API fields
        api_fields = {field for field in api_fields if field}
        
        # Build the final fields string, adding nested fields in the correct format
        fields_list = list(api_fields)
        
        # Add nested field structures
        for parent_field, child_fields in nested_fields.items():
            if child_fields:
                # Format like "activities{actor_id,actor_name,event_type}"
                nested_str = f"{parent_field}{{{','.join(sorted(child_fields))}}}"
                # Remove the parent field by itself if we're adding a nested version
                if parent_field in fields_list:
                    fields_list.remove(parent_field)
                fields_list.append(nested_str)
        
        return ",".join(sorted(fields_list))

    def prepare_dataframe(self) -> pd.DataFrame:
        """Prepare DataFrame from organized data."""
        # # Pre-process date/timestamp fields in organized_data before creating DataFrame
        # processed_data = []
        
        # Create DataFrame with pre-processed data
        df = pd.DataFrame(self.organized_data, columns=self.columns)

        # Convert data types based on column definitions
        for col_def in self.column_definitions:
            col_name = col_def['name']
            col_type = col_def['type']
            
            if col_name in df.columns:
                # Handle date and timestamp conversions with utc=True
                if col_type == 'DATE' or col_type == 'TIMESTAMP':

                    df[col_name] = pd.to_datetime(df[col_name], utc=True)
                
                # Handle numeric conversions
                elif col_type == 'INTEGER' or col_type == 'FLOAT64':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        
        return df


    def process_record(self, record: Dict[str, Any]) -> List[Any]:
        """Process a single record from Meta Ads API response using dynamic column definitions.
        
        This method extracts values from records according to column definitions, handling:
        1. Simple direct fields
        2. Nested fields using dot notation (e.g., 'actions.value')
        3. Deeply nested structures with path_to_value (e.g., 'activities.data.*.actor_name')
        4. Array fields with selective extraction or all values combined
        5. Auto-generated fields like timestamps
        
        Returns:
            List of extracted values matching column definitions order
        """
        result = []
        
        for col_def in self.column_definitions:
            col_name = col_def['name']
            source_field = col_def.get('source_field')
            is_nested = col_def.get('is_nested', False)
            action_filter = col_def.get('action_filter')
            auto_generate = col_def.get('auto_generate', False)
            is_value_from = col_def.get('is_value_from')
            deep_nested = col_def.get('deep_nested', False)
            array_index = col_def.get('array_index')  # Optional array index, if not specified, returns all items or first based on array_all flag
            path_to_value = col_def.get('path_to_value')  # For deeply nested paths like activities.data.0.actor_name
            array_all = col_def.get('array_all', False)  # Flag to indicate whether to return all array items
            join_delimiter = col_def.get('join_delimiter', ',')  # Delimiter to join array values if array_all=True
            
            # Handle auto-generated fields
            if auto_generate:
                if col_name == 'processed_at':
                    result.append(datetime.now())
                else:
                    result.append("")
                continue
            
            # Handle deeply nested fields with path notation (e.g., "activities.data.0.actor_name" or "activities.data.*.actor_name")
            if deep_nested and path_to_value:
                try:
                    # Special case for wildcard notation to get all items in an array
                    if "*" in path_to_value:
                        # Replace * with a placeholder and set array_all to True
                        path_segments = path_to_value.split('.')
                        array_pos = None
                        for i, segment in enumerate(path_segments):
                            if segment == "*":
                                array_pos = i
                                path_segments[i] = "0"  # Replace with a placeholder
                                break
                        
                        # Reconstruct the path without the wildcard
                        if array_pos is not None:
                            path_to_value = '.'.join(path_segments)
                            array_all = True
                    
                    # Split the path into segments
                    path_segments = path_to_value.split('.')
                    current_value = record
                    
                    # Track if we're collecting all values from an array
                    collecting_array = False
                    array_position = -1
                    array_values = []
                    
                    # Navigate through the path segments to reach the target value or array
                    for i, segment in enumerate(path_segments):
                        # Check if this is a numeric segment (potential array index)
                        if segment.isdigit():
                            idx = int(segment)
                            # If array_all is True and we haven't started collecting, mark this position
                            if array_all and not collecting_array and i == array_pos:
                                collecting_array = True
                                array_position = i
                                
                                # If current_value is already a list, we're ready to collect values
                                if isinstance(current_value, list):
                                    break
                            
                            # If not collecting all array elements, just use the specific index
                            if isinstance(current_value, list):
                                if idx < len(current_value):
                                    current_value = current_value[idx]
                                else:
                                    current_value = ""
                                    break
                            elif isinstance(current_value, dict) and segment in current_value:
                                # Handle when segment is a string key in a dict that looks like a number
                                current_value = current_value[segment]
                            else:
                                current_value = ""
                                break
                        # Handle dictionary keys
                        elif isinstance(current_value, dict) and segment in current_value:
                            current_value = current_value[segment]
                            # If we encounter an array and array_all is True, this could be our collection point
                            if isinstance(current_value, list) and array_all and i == array_pos - 1:
                                collecting_array = True
                                array_position = i + 1  # Position after the current segment
                                break
                        else:
                            current_value = ""
                            break
                    
                    # Process array elements if needed
                    if collecting_array and current_value is not None and isinstance(current_value, list):
                        # Get remaining path segments to apply to each array item
                        remaining_path = path_segments[array_position+1:] if array_position < len(path_segments) - 1 else []
                        
                        # Process each array item with the remaining path
                        for item in current_value:
                            item_value = item
                            
                            # Apply remaining path segments to the item
                            for segment in remaining_path:
                                if isinstance(item_value, dict) and segment in item_value:
                                    item_value = item_value[segment]
                                elif segment.isdigit() and isinstance(item_value, list):
                                    idx = int(segment)
                                    if idx < len(item_value):
                                        item_value = item_value[idx]
                                    else:
                                        item_value = ""
                                        break
                                else:
                                    item_value = ""
                                    break
                            
                            if item_value is not None:
                                # Convert to string for joining, unless it's already a string
                                str_value = str(item_value) if not isinstance(item_value, str) else item_value
                                array_values.append(str_value)
                        
                        # Join all values with the specified delimiter or return the first one based on config
                        if array_values:
                            if array_all:
                                result.append(join_delimiter.join(array_values))
                            else:
                                # If not array_all but we extracted multiple values, take the first one
                                result.append(array_values[0] if array_values else "")
                        else:
                            result.append("")
                    else:
                        # Standard case - return the single value we found (or empty string if None)
                        result.append("" if current_value is None else current_value)
                        
                except Exception as e:
                    logger.warning(f"Error extracting deep nested value {path_to_value}: {str(e)}")
                    result.append("")

            # Handle nested fields with action filters (like specific action types)
            elif is_nested and action_filter:
                # For fields that get values from action_values array (like purchase_value)
                if is_value_from == "action_values" and "action_values" in record:
                    for action in record.get("action_values", []):
                        if action.get("action_type") == action_filter:
                            result.append(action.get("value", ""))
                            break
                    else:
                        result.append("")
                # For fields that get values from actions array (like purchases, leads, etc.)
                elif "actions" in record:
                    for action in record.get("actions", []):
                        if action.get("action_type") == action_filter:
                            result.append(action.get("value", ""))
                            break
                    else:
                        result.append("")
                else:
                    result.append("")
            
            # Handle general nested fields without specific filters
            elif is_nested:
                if "." in source_field:
                    parent_field, child_field = source_field.split(".")
                    if parent_field in record and record[parent_field]:
                        # Get the first item in the array that has the child_field
                        action = next((a for a in record[parent_field] if "value" in a), None)
                        if action:
                            if child_field == "action_type":
                                result.append(action.get("action_type", ""))
                            else:
                                result.append(action.get("value", ""))
                        else:
                            result.append("")
                    else:
                        result.append("")
                else:
                    result.append("")
            
            # Handle direct fields
            else:
                # Special handling for campaign_id to ensure it's an integer
                if source_field == "campaign_id" and record.get(source_field):
                    try:
                        result.append(int(record.get(source_field)))
                    except (ValueError, TypeError):
                        result.append("")
                # Handle date fields
                elif source_field == "date_start":
                    result.append(record.get(source_field, ""))
                # Handle all other direct fields
                else:
                    result.append(record.get(source_field, ""))
        # print(result)
        return result

    async def process_nested_response(self, response: Dict[str, Any], account: str, session: aiohttp.ClientSession = None) -> List[List[Any]]:
        """
        Process deeply nested JSON responses and extract records from the deepest level.
        This method handles nested structures like 'activities.data' and propagates parent-level values
        to each deeply nested record. It also handles pagination in nested data.
        
        Args:
            response: The JSON response from Meta API
            account: Account ID for context and data linking
            session: aiohttp ClientSession for making pagination requests
            
        Returns:
            List of processed records
        """
        processed_data = []
        
        # Check if response has data
        if "data" not in response:
            logger.warning(f"Response has no 'data' field: {response}")
            return processed_data
            
        # Process deeply nested structures
        for record in response["data"]:
            # Check for deeply nested arrays that need to be expanded
            nested_arrays = []
            nested_paginations = []
            
            # Check for nested data like 'activities.data'
            for field_name, field_value in record.items():
                if isinstance(field_value, dict):
                    # Check if it has a data array
                    if "data" in field_value and isinstance(field_value["data"], list):
                        # Found a nested array like activities.data
                        nested_arrays.append((field_name, field_value["data"]))
                        
                        # Check for pagination in the nested data
                        if "paging" in field_value and "next" in field_value["paging"]:
                            nested_paginations.append((field_name, field_value["paging"]["next"]))
            
            # Process nested arrays with pagination
            if nested_arrays and session:  # Only process pagination if a session is provided
                # Process each nested field with its own pagination
                for field_name, nested_items in nested_arrays:
                    # Create a list to store all items for this field (including from pagination)
                    all_nested_items = list(nested_items)  # Copy initial items
                    
                    # Find all pagination URLs for this field
                    pagination_urls = []
                    for f_name, pagination_url in nested_paginations:
                        if f_name == field_name:
                            pagination_urls.append(pagination_url)
                    
                    # Process pagination for this nested field - build a queue of URLs to process
                    urls_to_process = pagination_urls.copy()  # Start with initial pagination URLs
                    processed_urls = set()  # Keep track of processed URLs to avoid duplicates
                    
                    # Process all pagination URLs
                    while urls_to_process and session:
                        # Get the next URL to process
                        next_url = urls_to_process.pop(0)
                        
                        # Skip if already processed
                        if next_url in processed_urls:
                            continue
                        
                        processed_urls.add(next_url)
                        logger.info(f"Processing nested pagination for field '{field_name}' in account {account}")
                        
                        try:
                            # Fetch the next page of data
                            pagination_response = await self.process_url_with_retry(session, next_url)
                            
                            if isinstance(pagination_response, dict) and "data" in pagination_response:
                                # Add items from this page to our collection
                                all_nested_items.extend(pagination_response["data"])
                                
                                # Extract and queue up additional pagination URLs from this response
                                if "paging" in pagination_response:
                                    # Add 'next' URL if it exists
                                    if "next" in pagination_response["paging"]:
                                        next_pagination_url = pagination_response["paging"]["next"]
                                        if next_pagination_url not in processed_urls and next_pagination_url not in urls_to_process:
                                            urls_to_process.append(next_pagination_url)
                            else:
                                logger.warning(f"Invalid pagination response for nested field '{field_name}'")
                        except Exception as e:
                            logger.warning(f"Error processing nested pagination for field '{field_name}': {str(e)}")
                    
                    # Process all items (including paginated ones) for this field
                    for nested_item in all_nested_items:
                        # Create a new combined record with parent context
                        combined_record = record.copy()
                        
                        # Replace the nested field with a single item for processing
                        combined_record[field_name] = {"data": [nested_item]}
                        
                        # Process this combined record
                        processed_data.append(self.process_record(combined_record))
            elif nested_arrays:
                # If we have nested arrays but no session, just process what we have
                for field_name, nested_items in nested_arrays:
                    for nested_item in nested_items:
                        # Create a new combined record with parent context
                        combined_record = record.copy()
                        
                        # Replace the nested field with a single item for processing
                        combined_record[field_name] = {"data": [nested_item]}
                        
                        # Process this combined record
                        processed_data.append(self.process_record(combined_record))
            else:
                # No nested arrays to expand, process record as-is
                processed_data.append(self.process_record(record))
        # print(processed_data)
        return processed_data

    async def process_url_with_retry(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """
        Process a URL with retry logic and rate limiting.
        
        Args:
            session: aiohttp ClientSession for making the request
            url: The URL to request
            
        Returns:
            Dict: The JSON response from the API
            
        Raises:
            Exception: If all retries fail
        """
        max_retries = self.max_retries
        retry_count = 0
        last_error = None
        if hasattr(self, 'rate_limiter'):
            await self.rate_limiter.track_request()
        
        start_time = datetime.now()
        duration = (datetime.now() - start_time).total_seconds()
        
        # Initial URL processing
        original_url = url
        current_url = url
        
        while retry_count < max_retries:
            # Track API request for rate limiting
            await self.rate_limiter.track_request()
            
            try:
                self.metrics.api_calls += 1
                retry_count += 1
                
                headers = {"Authorization": f"Bearer {self.access_token}"}
                async with session.get(current_url, headers=headers) as response:
                    response_text = await response.text()
                    
                    # Handle successful response
                    if response.status == 200:
                        try:
                            data = json.loads(response_text)
                            self.metrics.successful_api_calls += 1
                            logger.info(f"API call successful - URL: {current_url}... - Duration: {duration:.2f}s")
                            return data
                        except json.JSONDecodeError as e:
                            logger.warning(f"Error decoding JSON response: {str(e)} - Response: {response_text}...")
                            self.metrics.failed_api_calls += 1
                            last_error = e
                    
                    # Handle rate limiting errors
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', '60'))
                        logger.warning(f"Rate limit reached, waiting {retry_after} seconds before retry {retry_count}/{max_retries}")
                        self.metrics.failed_api_calls += 1
                        await asyncio.sleep(retry_after)
                        continue
                    
                    # Handle token errors (need to refresh token)
                    elif response.status == 401 or response.status == 403:
                        error_json = json.loads(response_text)
                        error_message = error_json.get('error', {}).get('message', 'Unknown auth error')
                        logger.error(f"Authentication error: {error_message}")
                        self.metrics.failed_api_calls += 1
                        
                        # If the token has expired, refresh it and retry
                        if "expired" in error_message.lower() or "invalid" in error_message.lower():
                            try:
                                logger.info("Attempting to refresh Meta access token...")
                                self.update_meta_access_token()
                                logger.info("Successfully refreshed Meta access token, retrying request")
                                continue
                            except Exception as refresh_error:
                                logger.error(f"Failed to refresh token: {str(refresh_error)}")
                                last_error = refresh_error
                        
                        # For other authentication errors, abort retrying
                        raise Exception(f"Authentication failed: {error_message}")
                    
                    # Handle HTTP 500 errors with specific error messages about reducing data amount
                    elif response.status == 500:
                        try:
                            error_json = json.loads(response_text)
                            error_obj = error_json.get('error', {})
                            error_message = error_obj.get('message', '')
                            error_code = error_obj.get('code', 0)
                              # Check for specific errors that require reducing limit
                            if ((error_code == 1 and "Please reduce the amount of data" in error_message) or
                                (error_code == 1 and error_obj.get('error_subcode', 0) == 99)):
                                
                                # Extract current limit from the URL
                                current_limit = self._extract_limit_from_url(current_url)
                                if current_limit and current_limit > 10:
                                    # Determine reduction amount based on current limit
                                    if current_limit > 1000:
                                        # Reduce by 50 for limits greater than 200
                                        reduction_amount = 300     
                                    elif current_limit > 500 and current_limit <= 1000:
                                        # Reduce by 50 for limits greater than 200
                                        reduction_amount = 200                                    
                                    elif current_limit > 200 and current_limit <= 500:
                                        # Reduce by 50 for limits greater than 200
                                        reduction_amount = 100
                                    elif current_limit > 100 and current_limit <= 200:
                                        # Reduce by 20 for limits greater than 100
                                        reduction_amount = 50
                                    else:
                                        # Reduce by 10 for limits less than or equal to 200
                                        reduction_amount = 10
                                    
                                    # Calculate new limit, ensuring it doesn't go below 10
                                    new_limit = max(10, current_limit - reduction_amount)
                                    current_url = self._replace_limit_in_url(current_url, new_limit)
                                    
                                    # Store the reduced limit for future pagination requests
                                    self._reduced_limit = new_limit
                                    
                                    logger.warning(f"Reducing limit from {current_limit} to {new_limit} (by {reduction_amount}) due to error: {error_message}")
                                    self.metrics.failed_api_calls += 1
                                    continue  # Retry with reduced limit
                                else:
                                    # If we can't reduce further, log the error and continue with standard retry logic
                                    logger.warning(f"Cannot reduce limit below 10 or limit not found in URL: {current_url}")
                            
                            # For other HTTP 500 errors, log and continue with standard retry
                            error_message = f"API error (HTTP 500): {error_message}"
                            logger.warning(f"{error_message} - Retry {retry_count}/{max_retries}")
                            self.metrics.failed_api_calls += 1
                            last_error = Exception(error_message)
                        except (json.JSONDecodeError, KeyError) as e:
                            error_message = f"API error (HTTP 500) - Invalid or missing error format: {response_text[:100]}..."
                            logger.warning(f"{error_message} - Retry {retry_count}/{max_retries}")
                            self.metrics.failed_api_calls += 1
                            last_error = Exception(error_message)
                    
                    # Handle other API errors
                    else:
                        error_message = f"API error (HTTP {response.status}): URL: {current_url}... {response_text}..."
                        logger.warning(f"{error_message} - Retry {retry_count}/{max_retries}")
                        self.metrics.failed_api_calls += 1
                        last_error = Exception(error_message)
                
                # If we reach this point and haven't returned or continued, add a delay before retrying
                retry_delay = min(60, 5 * (2 ** retry_count))  # Exponential backoff with max of 60 seconds
                logger.info(f"Waiting {retry_delay} seconds before retry {retry_count}/{max_retries}")
                await asyncio.sleep(retry_delay)
                
            except asyncio.TimeoutError:
                self.metrics.failed_api_calls += 1
                logger.warning(f"Request timed out - Retry {retry_count}/{max_retries}")
                last_error = asyncio.TimeoutError("Request timed out")
                
                # Add a delay before retrying
                await asyncio.sleep(5 * retry_count)  # Increasing delay for timeouts
                
            except Exception as e:
                self.metrics.failed_api_calls += 1
                logger.warning(f"Error processing request: {str(e)} - Retry {retry_count}/{max_retries}")
                last_error = e
                
                # Add a delay before retrying
                await asyncio.sleep(5 * retry_count)
        
        # If we've exhausted all retries, raise the last error
        error_message = f"Failed to process URL after {max_retries} retries: {str(last_error)}"
        logger.error(error_message)
        raise Exception(error_message)
        
    def _extract_limit_from_url(self, url: str) -> int:
        """
        Extract the limit parameter from a URL.
        
        Args:
            url: The URL to extract limit from
            
        Returns:
            int: The extracted limit value, or None if not found
        """
        import re
        
        # Try to extract limit parameter using regex
        limit_match = re.search(r'[?&]limit=(\d+)', url)
        if limit_match:
            try:
                return int(limit_match.group(1))
            except (ValueError, IndexError):
                return None
        return None
    
    def _replace_limit_in_url(self, url: str, new_limit: int) -> str:
        """
        Replace the limit parameter in a URL with a new value.
        
        Args:
            url: The URL to modify
            new_limit: The new limit value to set
            
        Returns:
            str: The modified URL with the new limit
        """
        import re
        
        # Try to replace existing limit parameter
        if re.search(r'[?&]limit=\d+', url):
            return re.sub(r'([?&])limit=\d+', r'\1limit=' + str(new_limit), url)
        
        # If no limit parameter exists, add it
        if '?' in url:
            return url + f'&limit={new_limit}'
        else:
            return url + f'?limit={new_limit}'

    async def process_account_with_batched_dates(self, account: str, session: aiohttp.ClientSession):
        """
        Process an account by breaking the date range into smaller batches if is_date_range is provided,
        or using a regular process if no is_date_range is defined in column definitions.
        If a batch encounters any error, the entire batch is retried without storing partial data.
        
        Args:
            account: The account ID to process
            session: The aiohttp ClientSession to use for requests
                
        Returns:
            int: Number of records processed
        """
        # Reset the reduced_limit at the start of each account processing
        if hasattr(self, '_reduced_limit'):
            logger.info(f"Resetting reduced limit for new account {account}")
            delattr(self, '_reduced_limit')

        # Check if any column has is_date_range flag
        has_date_range = any(col.get('is_date_range', False) for col in self.column_definitions)
        
        if not has_date_range:
            # If no date range column is found, use regular processing without date batching
            return await self.process_account_without_batched_dates(account, session)
        
        # Continue with batched date processing if we have date range column
        batch_days = self.batch_days  # Use the configurable batch size

        # Calculate total days based on start_date and end_date
        start_date_obj = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d")
        total_days = (end_date_obj - start_date_obj).days + 1  # Inclusive
        records_processed = 0
        
        logger.info(f"Processing account {account} with batched date ranges ({batch_days} days per batch)")
        
        for start_idx in range(0, total_days, batch_days):
            # Calculate date range for this batch
            end_idx = min(start_idx + batch_days, total_days)
            
            # Calculate batch_start_date and batch_end_date
            batch_start_date = (start_date_obj + timedelta(days=start_idx)).strftime("%Y-%m-%d")
            batch_end_date = (start_date_obj + timedelta(days=end_idx - 1)).strftime("%Y-%m-%d")
            
            # Ensure batch_start_date <= batch_end_date
            if batch_start_date > batch_end_date:
                logger.warning(f"Skipping batch for account {account}: since ({batch_start_date}) > until ({batch_end_date})")
                continue
            
            logger.info(f"Processing batch for account {account}: {batch_start_date} to {batch_end_date}")
            
            # Maximum number of attempts for this batch
            max_batch_attempts = 3
            batch_attempt = 0
            batch_success = False
            
            while not batch_success and batch_attempt < max_batch_attempts:
                batch_attempt += 1
                date_column = next((col['name'] for col in self.column_definitions 
                                   if col.get('is_date_range', False)), None)
                
                # For retries, add a message
                if batch_attempt > 1:
                    logger.info(f"Retry attempt {batch_attempt}/{max_batch_attempts} for batch {batch_start_date} to {batch_end_date}")
                
                # Create URL for this date range
                url = (
                    f"https://graph.facebook.com/v22.0/{account}"
                )
                if self.endpoint:
                    url += f"/{self.endpoint}"

                url += f"?fields={self.api_fields}"

                if self.endpoint == "insights":
                    url += f"&time_increment=1"
                    url += f"&level=ad"
                
                if date_column:
                    url += f"&time_range[since]={batch_start_date}&time_range[until]={batch_end_date}"

                if self.breakdowns:
                    url += f"&breakdowns={self.breakdowns}"
                    
                if self.limit:
                    url += f"&limit={self.limit}"
                
                # Temporary storage for this batch's data
                batch_data = []
                batch_error = False
                # print(f"Batch URL: {url}")
                
                try:
                    # Process initial request
                    response = await self.process_url_with_retry(session, url)
                    
                    if isinstance(response, dict) and "data" in response:
                        # Process data based on the structure of the response
                        batch_data.extend(await self.process_nested_response(response, account, session))
                        
                        # Handle pagination - track processed URLs to prevent duplicates
                        processed_urls = set()
                          # Process pagination with a while loop to handle all pages
                        next_url = response.get("paging", {}).get("next")
                        
                        while next_url and not batch_error:
                            # Skip if we've already processed this URL
                            if next_url in processed_urls:
                                break
                            
                            # Mark URL as processed
                            processed_urls.add(next_url)
                            
                            try:
                                # Check if we have a reduced limit value from a previous error
                                current_limit = self._extract_limit_from_url(next_url)
                                if hasattr(self, '_reduced_limit') and self._reduced_limit and current_limit:
                                    if current_limit > self._reduced_limit:
                                        # Apply the reduced limit to this pagination URL
                                        next_url = self._replace_limit_in_url(next_url, self._reduced_limit)
                                        logger.info(f"Using reduced limit of {self._reduced_limit} for pagination request")
                                
                                page_response = await self.process_url_with_retry(session, next_url)
                                
                                if isinstance(page_response, dict) and "data" in page_response:
                                    # Process data from paginated response
                                    batch_data.extend(await self.process_nested_response(page_response, account, session))
                                    
                                    # Get the next URL for the following iteration
                                    next_url = page_response.get("paging", {}).get("next")
                                    
                                    # Check if the pagination response had a dynamically adjusted limit
                                    # and store it for future pagination requests
                                    if hasattr(self, '_reduced_limit') and self._reduced_limit:
                                        if next_url:
                                            next_url_limit = self._extract_limit_from_url(next_url)
                                            if next_url_limit and next_url_limit > self._reduced_limit:
                                                next_url = self._replace_limit_in_url(next_url, self._reduced_limit)
                                                logger.info(f"Applying reduced limit of {self._reduced_limit} to next pagination URL")
                                else:
                                    # If response doesn't contain data, mark batch as failed
                                    logger.warning(f"Invalid pagination response: {page_response}")
                                    batch_error = True
                                    break
                            except Exception as e:
                                logger.warning(f"Error processing pagination URL {next_url}: {str(e)}")
                                batch_error = True
                                break
                        
                        # Only if no errors occurred, consider the batch successful
                        if not batch_error:
                            batch_success = True
                            
                            # Only now add the data to the main storage
                            self.organized_data.extend(batch_data)
                            records_processed += len(batch_data)
                            
                            logger.info(f"Batch {batch_start_date} to {batch_end_date} for account {account}: "
                                    f"Processed {len(batch_data)} records successfully")
                        else:
                            logger.warning(f"Errors occurred during batch processing. Discarding partial data "
                                        f"and will retry (attempt {batch_attempt}/{max_batch_attempts})")
                    else:
                        logger.warning(f"Invalid response for batch: {response}")
                        batch_error = True
                
                except Exception as e:
                    logger.warning(f"Error processing batch {batch_start_date} to {batch_end_date} for account {account}: {str(e)}")
                    batch_error = True
                
                # If we encountered an error but have more attempts, wait before retrying
                if batch_error and batch_attempt < max_batch_attempts:
                    retry_delay = 30 * batch_attempt  # Increasing delay with each attempt
                    logger.info(f"Waiting {retry_delay} seconds before retrying batch")
                    await asyncio.sleep(retry_delay)
                elif batch_error:
                    logger.error(f"Failed to process batch after {max_batch_attempts} attempts. Skipping batch.")
            
            # Add delay between batches (successful or not) to avoid rate limiting
            batch_delay = self.batch_delay
            logger.info(f"Waiting {batch_delay} seconds before processing next batch")            
            await asyncio.sleep(batch_delay)
        
        return records_processed


    async def process_account_without_batched_dates(self, account: str, session: aiohttp.ClientSession):
        """
        Process an account directly without breaking the date range into batches.
        Used when is_date_range is not defined in column definitions.
        
        Args:
            account: The account ID to process
            session: The aiohttp ClientSession to use for requests
                
        Returns:
            int: Number of records processed
        """
        # Reset the reduced_limit at the start of each account processing
        if hasattr(self, '_reduced_limit'):
            logger.info(f"Resetting reduced limit for new account {account}")
            delattr(self, '_reduced_limit')

        records_processed = 0
        logger.info(f"Processing account {account} without date batching")
        
        # Maximum number of attempts for this account
        max_attempts = 3
        attempt = 0
        success = False
        
        while not success and attempt < max_attempts:
            attempt += 1
            
            # For retries, add a message
            if attempt > 1:
                logger.info(f"Retry attempt {attempt}/{max_attempts} for account {account}")
            
            # Create URL for this account without date range
            url = f"https://graph.facebook.com/v22.0/{account}"
            
            if self.endpoint:
                url += f"/{self.endpoint}"

            url += f"?fields={self.api_fields}"

            if self.breakdowns:
                url += f"&breakdowns={self.breakdowns}"
                
            if self.limit:
                url += f"&limit={self.limit}"
            
            # Temporary storage for this account's data
            account_data = []
            error_occurred = False
            
            try:
                # Process initial request
                response = await self.process_url_with_retry(session, url)
                
                if isinstance(response, dict) and "data" in response:
                    # Process data based on the structure of the response
                    account_data.extend(await self.process_nested_response(response, account, session))
                    
                    # Handle pagination - track processed URLs to prevent duplicates
                    processed_urls = set()
                    
                    # Process pagination with a while loop to handle all pages
                    next_url = response.get("paging", {}).get("next")
                    
                    while next_url and not error_occurred:
                        # Skip if we've already processed this URL
                        if next_url in processed_urls:
                            break
                        
                        # Mark URL as processed
                        processed_urls.add(next_url)
                        try:
                            # Check if we have a reduced limit value from a previous error
                            current_limit = self._extract_limit_from_url(next_url)
                            if hasattr(self, '_reduced_limit') and self._reduced_limit and current_limit:
                                if current_limit > self._reduced_limit:
                                    # Apply the reduced limit to this pagination URL
                                    next_url = self._replace_limit_in_url(next_url, self._reduced_limit)
                                    logger.info(f"Using reduced limit of {self._reduced_limit} for pagination request")
                                    
                            page_response = await self.process_url_with_retry(session, next_url)
                            
                            if isinstance(page_response, dict) and "data" in page_response:
                                # Process data from paginated response
                                account_data.extend(await self.process_nested_response(page_response, account, session))
                                
                                # Get the next URL for the following iteration
                                next_url = page_response.get("paging", {}).get("next")
                                
                                # Check if the pagination response had a dynamically adjusted limit
                                # and apply it to the next URL
                                if hasattr(self, '_reduced_limit') and self._reduced_limit:
                                    if next_url:
                                        next_url_limit = self._extract_limit_from_url(next_url)
                                        if next_url_limit and next_url_limit > self._reduced_limit:
                                            next_url = self._replace_limit_in_url(next_url, self._reduced_limit)
                                            logger.info(f"Applying reduced limit of {self._reduced_limit} to next pagination URL")
                            else:
                                # If response doesn't contain data, mark as failed
                                logger.warning(f"Invalid pagination response: {page_response}")
                                error_occurred = True
                                break
                        except Exception as e:
                            logger.warning(f"Error processing pagination URL {next_url}: {str(e)}")
                            error_occurred = True
                            break
                    
                    # Only if no errors occurred, consider the processing successful
                    if not error_occurred:
                        success = True
                        
                        # Only now add the data to the main storage
                        self.organized_data.extend(account_data)
                        records_processed = len(account_data)
                        
                        logger.info(f"Account {account} processed successfully: {records_processed} records")
                    else:
                        logger.warning(f"Errors occurred during account processing. Discarding partial data "
                                    f"and will retry (attempt {attempt}/{max_attempts})")
                else:
                    logger.warning(f"Invalid response for account: {response}")
                    error_occurred = True
            
            except Exception as e:
                logger.warning(f"Error processing account {account}: {str(e)}")
                error_occurred = True
            
            # If we encountered an error but have more attempts, wait before retrying
            if error_occurred and attempt < max_attempts:
                retry_delay = 30 * attempt  # Increasing delay with each attempt
                logger.info(f"Waiting {retry_delay} seconds before retrying account")
                await asyncio.sleep(retry_delay)
            elif error_occurred:
                logger.error(f"Failed to process account after {max_attempts} attempts. Skipping account.")
        
        return records_processed


    async def process_batch(self, accounts: List[str]):
        """
        Process a batch of accounts using the batched date approach for all accounts.
        Added improved error handling and reporting.
        
        Args:
            accounts: List of account IDs to process
        """
        batch_start_time = datetime.now()
        logger.info(f"Starting batch processing for {len(accounts)} accounts with batched date ranges")
        
        # Track account processing status
        account_statuses = {
            "success": 0,
            "failed": 0,
            "total": len(accounts)
        }
        
        async with aiohttp.ClientSession() as session:
            for i, account in enumerate(accounts):
                account_start_time = datetime.now()
                try:
                    records = await self.process_account_with_batched_dates(account, session)
                    account_statuses["success"] += 1
                    self.metrics.records_processed += records
                    
                    account_duration = (datetime.now() - account_start_time).total_seconds()
                    logger.info(f"Completed processing account {account}: {records} records in {account_duration:.2f}s "
                            f"({account_statuses['success']}/{account_statuses['total']} accounts processed)")
                except Exception as e:
                    account_statuses["failed"] += 1
                    logger.error(f"Failed to process account {account}: {str(e)}")
                    self.metrics.errors.append(f"Account {account}: {str(e)}")
                
                # Add a delay between accounts to avoid rate limiting
                # Only add delay if this isn't the last account
                if i < len(accounts) - 1:
                    account_delay = self.account_delay
                    logger.info(f"Waiting {account_delay} seconds before processing next account "
                            f"({account_statuses['success']} successful, {account_statuses['failed']} failed, "
                            f"{len(accounts) - i - 1} remaining)")
                    await asyncio.sleep(account_delay)

        duration = (datetime.now() - batch_start_time).total_seconds()
        logger.info(f"Batch processing completed - Duration: {duration:.2f}s - "
                f"Success: {account_statuses['success']}/{account_statuses['total']} accounts")

    async def get_facebook_accounts(self) -> List[str]:
        """
        Gets the list of facebook accounts with retry mechanism to handle rate limitations
        and other potential errors.

        Returns:
            List[str]: List of Facebook account IDs
        """
        accounts = []
        account_types = ["client_ad_accounts", "owned_ad_accounts"]

        async with aiohttp.ClientSession() as session:
            for account_type in account_types:
                after_cursor = None    
                while True:
                    url = f"https://graph.facebook.com/v22.0/{self.business_id}/{account_type}?limit=100"
                    if after_cursor:
                        url += f"&after={after_cursor}"

                    # Using retry mechanism for handling failures
                    max_retries = self.max_retries if hasattr(self, 'max_retries') else 3
                    retry_count = 0
                    last_error = None
                    success = False
                    
                    # Try to get accounts with retries
                    while retry_count < max_retries and not success:
                        try:
                            # Track metric if available
                            if hasattr(self, 'metrics'):
                                self.metrics.api_calls += 1
                            
                            # Rate limiter if available
                            if hasattr(self, 'rate_limiter'):
                                await self.rate_limiter.track_request()
                            
                            retry_count += 1
                            headers = {"Authorization": f"Bearer {self.access_token}"}
                            
                            async with session.get(url, headers=headers) as response:
                                response_text = await response.text()
                                
                                # Handle successful response
                                if response.status == 200:
                                    try:
                                        data = json.loads(response_text)
                                        if hasattr(self, 'metrics'):
                                            self.metrics.successful_api_calls += 1
                                        
                                        logger.info(f"Successfully fetched accounts of type {account_type}")
                                        
                                        if "data" not in data:
                                            logger.warning(f"No data found in response for account type {account_type}")
                                            break
                                        
                                        accounts.extend([account["id"] for account in data["data"]])
                                        
                                        if "paging" not in data or "next" not in data["paging"]:
                                            after_cursor = None
                                            break
                                        
                                        after_cursor = data["paging"]["cursors"]["after"]
                                        success = True
                                        
                                    except json.JSONDecodeError as e:
                                        if hasattr(self, 'metrics'):
                                            self.metrics.failed_api_calls += 1
                                        logger.warning(f"Error decoding JSON response: {str(e)}")
                                        last_error = e
                                
                                # Handle rate limiting errors
                                elif response.status == 429:
                                    retry_after = int(response.headers.get('Retry-After', '60'))
                                    if hasattr(self, 'metrics'):
                                        self.metrics.failed_api_calls += 1
                                    logger.warning(f"Rate limit reached, waiting {retry_after} seconds before retry {retry_count}/{max_retries}")
                                    await asyncio.sleep(retry_after)
                                    continue
                                
                                # Handle token errors (need to refresh token)
                                elif response.status == 401 or response.status == 403:
                                    error_json = json.loads(response_text)
                                    error_message = error_json.get('error', {}).get('message', 'Unknown auth error')
                                    if hasattr(self, 'metrics'):
                                        self.metrics.failed_api_calls += 1
                                    logger.error(f"Authentication error: {error_message}")
                                    
                                    # If the token has expired, refresh it and retry
                                    if "expired" in error_message.lower() or "invalid" in error_message.lower():
                                        try:
                                            logger.info("Attempting to refresh Meta access token...")
                                            self.update_meta_access_token()
                                            logger.info("Successfully refreshed Meta access token, retrying request")
                                            continue
                                        except Exception as refresh_error:
                                            logger.error(f"Failed to refresh token: {str(refresh_error)}")
                                            last_error = refresh_error
                                    
                                    # For other authentication errors, abort retrying
                                    raise Exception(f"Authentication failed: {error_message}")
                                
                                # Handle other API errors
                                else:
                                    error_message = f"API error (HTTP {response.status}): {response_text[:200]}..."
                                    if hasattr(self, 'metrics'):
                                        self.metrics.failed_api_calls += 1
                                    logger.warning(f"{error_message} - Retry {retry_count}/{max_retries}")
                                    last_error = Exception(error_message)
                            
                            # If we reach this point and haven't returned or continued, add a delay before retrying
                            if not success:
                                retry_delay = min(60, 5 * (2 ** retry_count))  # Exponential backoff with max of 60 seconds
                                logger.info(f"Waiting {retry_delay} seconds before retry {retry_count}/{max_retries}")
                                await asyncio.sleep(retry_delay)
                            
                        except asyncio.TimeoutError:
                            if hasattr(self, 'metrics'):
                                self.metrics.failed_api_calls += 1
                            logger.warning(f"Request timed out - Retry {retry_count}/{max_retries}")
                            last_error = asyncio.TimeoutError("Request timed out")
                            
                            # Add a delay before retrying
                            await asyncio.sleep(5 * retry_count)  # Increasing delay for timeouts
                            
                        except Exception as e:
                            if hasattr(self, 'metrics'):
                                self.metrics.failed_api_calls += 1
                            logger.warning(f"Error processing request: {str(e)} - Retry {retry_count}/{max_retries}")
                            last_error = e
                            
                            # Add a delay before retrying
                            await asyncio.sleep(5 * retry_count)
                    
                    # If we've exhausted all retries without success, raise the last error
                    if not success and retry_count >= max_retries:
                        error_message = f"Failed to fetch accounts of type {account_type} after {max_retries} retries: {str(last_error)}"
                        logger.error(error_message)
                        raise Exception(error_message)
                    
                    # If there's no more pagination, break out of the pagination loop
                    if after_cursor is None:
                        break

        logger.info(f"Successfully fetched {len(accounts)} Facebook accounts")
        return accounts
    
    def create_big_query_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist with enhanced logging and error handling."""
        start_time = datetime.now()
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        logger.info(f"Checking/creating BigQuery dataset: {dataset_ref}")
        
        try:
            # Check if dataset exists
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
            return
        except Exception:
            try:
                # Create dataset
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Default location, consider making this configurable
                
                # Create the dataset
                dataset = self.client.create_dataset(dataset, timeout=30)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Created dataset {dataset_ref} - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                
            except Exception as e:
                logger.error(f"Failed to create BigQuery dataset: {str(e)}")
                self.metrics.errors.append(str(e))
                raise

    def create_big_query_table_if_not_exists(self):
        """Create BigQuery table with enhanced logging and error handling."""
        start_time = datetime.now()
        logger.info(f"Checking/creating BigQuery table: {self.table_ref}")
        
        try:
            self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_ref} already exists")
            return
        except Exception:
            try:
                # Create schema based on column definitions
                schema = []
                
                for col_def in self.column_definitions:
                    field_name = col_def['name']
                    field_type = col_def['type']
                    # All fields are NULLABLE by default
                    schema.append(bigquery.SchemaField(field_name, field_type, mode="NULLABLE"))

                table = bigquery.Table(self.table_ref, schema=schema)
                
                # Find the date column for partitioning
                # First, check for columns with is_date_range flag
                date_column = next((col['name'] for col in self.column_definitions 
                                   if col.get('is_date_range', False)), None)
                
                # If no is_date_range flag, fall back to columns with DATE type
                if not date_column:
                    date_column = next((col['name'] for col in self.column_definitions 
                                      if col['type'] == 'DATE'), None)
                
                # Check if a date column exists
                if date_column:
                    # Create a partitioned table if date column exists
                    logger.info(f"Date column '{date_column}' found. Creating partitioned table.")
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=date_column
                    )
                else:
                    # Create a regular table if no date column exists
                    logger.info("No date column found in column definitions. Creating regular table without partitioning.")
                
                # Set clustering fields based on column definitions
                # Default to date, account_id, device if available
                clustering_fields = []
                if date_column:
                    clustering_fields.append(date_column)
                
                account_id_col = next((col['name'] for col in self.column_definitions 
                                     if col['name'] == 'account_id'), None)
                if account_id_col:
                    clustering_fields.append(account_id_col)
                    
                device_col = next((col['name'] for col in self.column_definitions 
                                 if col['name'] == 'device'), None)
                if device_col:
                    clustering_fields.append(device_col)
                
                if clustering_fields:
                    table.clustering_fields = clustering_fields
                
                self.client.create_table(table)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Created table {self.table_ref} - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                
            except Exception as e:
                logger.error(f"Failed to create BigQuery table: {str(e)}")
                self.metrics.errors.append(str(e))
                raise    
    def delete_partitions(self, min_date, max_date):
        """Delete partitions with enhanced logging and error handling."""
        start_time = datetime.now()
        logger.info(f"Deleting partitions for date range: {min_date} to {max_date}")
        
        try:            
            # Find the date column for querying
            # First, check for columns with is_date_range flag
            date_column = next((col['name'] for col in self.column_definitions 
                               if col.get('is_date_range', False)), None)
            
            # Handle case when min_date and max_date are None (coming from non-dated tables)
            if min_date is None or max_date is None:
                logger.info("No date range provided. Will delete all data from the table instead.")
                
                query = f"""
                DELETE FROM `{self.table_ref}`
                WHERE TRUE
                """
                
                logger.info(f"Executing full table deletion query: {query}")
                query_job = self.client.query(query)
                result = query_job.result()
                
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Deleted all data from table successfully - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
                return
            else:
                # Get the table schema to verify the column exists
                table = self.client.get_table(self.table_ref)
                table_schema_fields = [field.name for field in table.schema]
                # print(table_schema_fields)
                
                if date_column not in table_schema_fields:
                    raise ValueError(f"Column '{date_column}' not found in table schema. Available columns: {', '.join(table_schema_fields)}")
                
                logger.info(f"Using column '{date_column}' for partition deletion")
                
                query = f"""
                DELETE FROM `{self.table_ref}`
                WHERE DATE({date_column}) BETWEEN "{min_date}" AND "{max_date}"
                """
                
                logger.info(f"Executing deletion query: {query}")
                query_job = self.client.query(query)
                result = query_job.result()
                
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Deleted partitions successfully - Duration: {duration:.2f}s")
                self.metrics.bigquery_operations += 1
            
        except Exception as e:
            logger.error(f"Failed to delete partitions: {str(e)}")
            self.metrics.errors.append(str(e))
            # Raise the exception with more context
            raise Exception(f"Failed to delete partitions: {str(e)} - Check if your date column exists and is correctly named in your table schema.") from e

    def append_data_to_bigquery(self, data: pd.DataFrame):
        """Append data to BigQuery with enhanced logging and error handling."""
        self.validate_and_update_schema(data)
        start_time = datetime.now()
        logger.info(f"Appending {len(data)} rows to BigQuery")
        
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            
            job = self.client.load_table_from_dataframe(
                data, self.table_ref, job_config=job_config
            )
            
            result = job.result()  # Wait for the job to complete
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(
                f"Successfully appended {len(data)} rows to {self.table_ref} - "
                f"Duration: {duration:.2f}s"
            )
            self.metrics.bigquery_operations += 1
            
        except Exception as e:
            logger.error(f"Failed to append data to BigQuery: {str(e)}")
            self.metrics.errors.append(str(e))
            raise

    def update_meta_access_token(self):
        app_id = self.app_id
        app_secret = self.app_secret
        old_access_token = self.access_token
        cloud_run_job_name = self.job_name
        project_id = self.project_id
        region = "us-central1"

        if not all([app_id, app_secret, old_access_token, project_id]):
            raise ValueError("Missing required environment variables")

        url = f"https://graph.facebook.com/v21.0/oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={old_access_token}&set_token_expires_in_60_days=true"
        response = requests.get(url)

        if response.status_code == 200:
            new_access_token = response.json().get("access_token")
            if not new_access_token:
                raise ValueError("Failed to extract new access token.")

            print("Meta access token fetched successfully.")
            client = run_v2.JobsClient()
            job_name = f"projects/{project_id}/locations/{region}/jobs/{cloud_run_job_name}"

            job = client.get_job(name=job_name)

            env_vars = job.template.template.containers[0].env

            for env_var in env_vars[:]: 
                if env_var.name == "META_ACCESS_TOKEN":
                    env_vars.remove(env_var)  

            client.update_job(job=job)
            print("Removed old META_ACCESS_TOKEN successfully.")

            job = client.get_job(name=job_name)

            job.template.template.containers[0].env.append(
                {"name": "META_ACCESS_TOKEN", "value": new_access_token}
            )

            updated_job = client.update_job(job=job)
            print("Updated Cloud Run job successfully with new META_ACCESS_TOKEN.")

        else:
            raise Exception(f"Failed to refresh token: {response.status_code}, {response.text}")
            
    def validate_and_update_schema(self, data: List[List[Any]]):
        """
        Check if the BigQuery table schema matches the new data schema.
        If not, update the schema with the new data schema.
        """
        logger.info("Validating BigQuery table schema...")
        try:
            # Fetch the existing table schema
            table = self.client.get_table(self.table_ref)
            existing_schema = {field.name: field for field in table.schema}

            # Define the new schema based on column configuration, excluding table/date-range indicators
            new_schema = {}
            for col_config in self.column_definitions:
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
                self.client.update_table(table, ["schema"])
                logger.info("BigQuery table schema updated successfully.")

        except Exception as e:
            logger.error(f"Failed to validate/update BigQuery schema: {str(e)}")
            self.metrics.errors.append(str(e))
            raise

class APIRateLimitTracker:
    """
    Tracks API requests to ensure we don't exceed Meta's rate limits.
    Implements a sliding window to track requests in the past hour with reset functionality.
    """
    def __init__(self, max_requests_per_hour, cooldown_minutes):
        # self.max_requests_per_hour = int(os.environ.get("MAX_REQUESTS_PER_HOUR", 600)) or getattr(config, "MAX_REQUESTS_PER_HOUR", 600)
        # self.cooldown_minutes = int(os.environ.get("COOLDOWN_MINUTES", 5)) or getattr(config, "COOLDOWN_MINUTES", 5)
        self.max_requests_per_hour = int(os.environ.get("MAX_REQUESTS_PER_HOUR", max_requests_per_hour))
        self.cooldown_minutes = int(os.environ.get("COOLDOWN_MINUTES", cooldown_minutes))
        self.request_timestamps = []
        self.start_time = datetime.now()
        self.total_requests = 0
        self.cooldowns = 0
        self.last_reset_time = None
        
        logger.info(f"Rate limit tracker initialized: {self.max_requests_per_hour} requests/hour, "
                   f"{self.cooldown_minutes} minute cooldown and reset if exceeded")
    
    async def track_request(self):
        """
        Tracks a new API request and enforces rate limits if necessary.
        After a cooldown period, resets the request counter.
        
        Returns: True if the request should proceed, False if it was rate-limited
        """
        now = datetime.now()
        self.total_requests += 1
        
        # Check if we should reset counters after cooldown
        if self.last_reset_time is not None:
            time_since_reset = (now - self.last_reset_time).total_seconds() / 60
            if time_since_reset >= self.cooldown_minutes:
                logger.info(f"Resetting request counter after {self.cooldown_minutes} minute cooldown period")
                self.request_timestamps = []
                self.last_reset_time = None
        
        # Add current timestamp to the list
        self.request_timestamps.append(now)
        
        # Remove timestamps older than 1 hour
        one_hour_ago = now - timedelta(hours=1)
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > one_hour_ago]
        
        # Check if we've exceeded the rate limit
        requests_in_last_hour = len(self.request_timestamps)
        
        # Log every 10 requests
        if self.total_requests % 10 == 0:
            elapsed_minutes = (now - self.start_time).total_seconds() / 60
            rate_per_hour = requests_in_last_hour / (elapsed_minutes / 60) if elapsed_minutes > 0 else 0
            logger.info(f"Rate limit status: {requests_in_last_hour}/{self.max_requests_per_hour} "
                       f"requests in the last hour (avg: {rate_per_hour:.2f} req/hr)")
        
        if requests_in_last_hour >= self.max_requests_per_hour:
            self.cooldowns += 1
            logger.warning(f"Rate limit reached: {requests_in_last_hour} requests in the last hour "
                          f"exceeds limit of {self.max_requests_per_hour}. "
                          f"Cooling down and resetting for {self.cooldown_minutes} minutes.")
            
            # Record the time we started the cooldown
            self.last_reset_time = now
            
            # Wait for the cooldown period
            await asyncio.sleep(self.cooldown_minutes * 60)
            
            # After the cooldown, the counter will be reset on the next request
            return True
        
        return True
    
    def get_metrics(self):
        """Returns metrics about rate limiting for reporting"""
        return {
            "total_api_requests": self.total_requests,
            "requests_in_last_hour": len(self.request_timestamps),
            "max_requests_per_hour": self.max_requests_per_hour,
            "cooldown_periods": self.cooldowns,
            "runtime_minutes": (datetime.now() - self.start_time).total_seconds() / 60,
            "last_reset_ago_minutes": (datetime.now() - self.last_reset_time).total_seconds() / 60 if self.last_reset_time else None
        }