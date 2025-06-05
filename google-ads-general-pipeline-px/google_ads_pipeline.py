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
# import config
from pytz import timezone
import pytz

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


        """Initialize the pipeline with necessary clients and configurations."""
        # Initialize configuration from config file
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
        # Initialize clients and configurations
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.bq_client = bigquery.Client(project=self.project_id)
        self.metrics = GoogleAdsPipelineMetrics()

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
            "customer_id",  # Directly available (in Manager Account reports)
            "customer_name",  # Directly available (in Manager Account reports)
            "campaign_id",  # Directly available
            "campaign_name",  # Directly available
            "ad_group_id",  # Directly available
            "ad_group_name",  # Directly available
            "clicks",  # Directly available
            "all_conversions",  # Directly available
            "conversions",  # Directly available
            "conversions_value",  # Directly available
            "conversions_by_date",  # Directly available (use `segments.date`)
            "conversions_value_by_date",  # Directly available (use `segments.date`)
            "date",  # Directly available (use `segments.date`)
            "currency_code",  # Directly available
            "timezone",  # Directly available
            "cost",  # Directly available
            "all_conversions_value",  # Directly available
            "impressions",  # Directly available
            "interactions",  # Directly available
            "device",  # Directly available (as `segments.device`)
            "click_type",
            "budget_status",  # Directly available (as `campaign.budget.status`)
            "budget_period",  # Directly available (as `campaign.budget.period`)
            "budget_amount",  # Directly available (as `campaign.budget.amount_micros`)
            "billing_start_date",  # Directly available (in Manager Account reports)
            "processed_at",  # Not directly available (requires custom implementation)
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
                bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("campaign_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ad_group_id", "INTEGER", mode="NULLABLE"),  # Added
                bigquery.SchemaField("ad_group_name", "STRING", mode="NULLABLE"),  # Added
                bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("all_conversions", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("conversions", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("conversions_value", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("conversions_by_date", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("conversions_value_by_date", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("timezone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("cost", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("all_conversions_value", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("budget_status", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("budget_period", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("budget_amount", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("interactions", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("device", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("click_type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("billing_start_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
            ]

            table = bigquery.Table(self.table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="date"
            )
            table.clustering_fields = ["date", "customer_id"]

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

    async def fetch_campaign_data(self, customer_id: str) -> List[List[Any]]:
        """Fetch campaign data for a specific customer and prepare it for BigQuery appending."""
        try:
            self.metrics.api_calls += 1

            # Fetch data from the API
            api_response = self.ads_api.get_campaign_performance(
                customer_id=customer_id,
                start_date=self.start_date,
                end_date=self.end_date,
            )

            # Log the raw response for debugging
            logger.debug(f"Raw API response for customer {customer_id}: {api_response}")
            # Check if the response only contains fieldMask (no activities case)
            if "fieldMask" in api_response and len(api_response) == 1:
                logger.info(f"No activities found for customer {customer_id}")
                return []
            # Validate the response
            # if not isinstance(api_response, list):
            #     raise ValueError(f"API response is not a list: {type(api_response)}")
            # Validate the response type
            if not isinstance(api_response, dict) or "results" not in api_response:
                raise ValueError(f"Invalid API response format: {type(api_response)}")

            results = api_response.get("results", [])
            if not isinstance(results, list):
                raise ValueError(f"'results' should be a list but got {type(results)}")
            processed_data = []
            for result in results:
                try:
                    # Extract data with defaults for missing keys
                    customer_data = result.get("customer", {})
                    campaign_data = result.get("campaign", {})
                    ad_group_data = result.get("adGroup",{})
                    metrics_data = result.get("metrics", {})
                    segments_data = result.get("segments", {})
                    #print(segments_data)

                    # Prepare a row for BigQuery
                    processed_row = [
                        int(customer_data.get("id", "N/A")),  # Customer ID
                        customer_data.get("descriptiveName", "id"),  # Customer Name
                        int(campaign_data.get("id", 0)),
                        campaign_data.get("name", "N/A"),  # Campaign Name
                        int(ad_group_data.get("id", 0)),
                        ad_group_data.get("name", "N/A"),                        
                        int(metrics_data.get("clicks", 0)),  # Clicks
                        float(metrics_data.get("allConversions", 0)),  # All Conversions
                        float(metrics_data.get("conversions", 0)),  # Conversions
                        float(
                            metrics_data.get("conversionsValue", 0)
                        ),  # Conversion Value
                        float(
                            metrics_data.get("conversionsByConversionDate", 0)
                        ),  # Conversions by Date
                        float(
                            metrics_data.get("conversionsValueByConversionDate", 0)
                        ),  # Conversion Value by Date
                        segments_data.get("date", "N/A"),  # Segment Date
                        customer_data.get("currencyCode", "N/A"),  # Currency Code
                        customer_data.get("timeZone", "N/A"),  # Time Zone
                        float(metrics_data.get("costMicros", 0))
                        / 1_000_000,  # Cost (converted from micros)
                        float(
                            metrics_data.get("allConversionsValue", 0)
                        ),  # All Conversions Value
                        int(metrics_data.get("impressions", 0)),  # Impressions
                        int(metrics_data.get("interactions", 0)),  # Interactions
                        segments_data.get("device", "N/A"),  # Segment Device
                        segments_data.get("clickType", "N/A"),
                        datetime.now().isoformat(),  # Ingestion Timestamp
                    ]
                    
                    
                    processed_data.append(processed_row)
                except Exception as row_ex:
                    logger.warning(
                        f"Failed to process a result for customer {customer_id}: {row_ex}"
                    )
                    continue  # Skip problematic row and proceed

            # Log results
            self.metrics.successful_api_calls += 1
            self.metrics.records_processed += len(processed_data)
            logger.info(
                f"Processed {len(processed_data)} records for customer {customer_id}"
            )
                    
            # print(segments_data)
            # print(ad_group_data)
            # print(metrics_data)
            # print(campaign_data)
            #print(processed_data)
            return processed_data

        except Exception as ex:
            logger.error(f"Error fetching data for customer {customer_id}: {ex}")
            self.metrics.failed_api_calls += 1
            self.metrics.errors.append(str(ex))
            raise

    async def fetch_budget_data(self, customer_id: str) -> List[List[Any]]:
        """Fetch budget data for a specific customer and prepare it for BigQuery appending."""
        try:
            self.metrics.api_calls += 1

            # Fetch budget data from the API
            api_response = self.ads_api.get_campaign_budget_data(
                customer_id=customer_id,
                start_date=self.start_date,
                end_date=self.end_date,
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
                    # Extract data with defaults for missing keys
                    customer_data = result.get("customer", {})
                    campaign_data = result.get("campaign", {})
                    budget_data = result.get("campaignBudget", {})
                    segments_data = result.get("segments", {})

                    # Prepare a row for BigQuery
                    processed_row = [
                        int(customer_data.get("id", "N/A")),  # Customer ID
                        customer_data.get(
                            "resourceName", "N/A"
                        ),  # Customer Resource Name
                        int(campaign_data.get("id", "N/A")),  # Campaign ID
                        campaign_data.get("name", "N/A"),  # Campaign Name
                        budget_data.get("id", "N/A"),  # Budget ID
                        budget_data.get("name", "N/A"),  # Budget Name
                        budget_data.get("status", "N/A"),  # Budget Status
                        budget_data.get("period", "N/A"),  # Budget Period
                        float(budget_data.get("amountMicros", 0))
                        / 1_000_000,  # Amount (converted from micros)
                        segments_data.get("date", "N/A"),  # Segment Date
                        datetime.now().isoformat(),  # Ingestion Timestamp
                    ]

                    processed_data.append(processed_row)
                except Exception as row_ex:
                    logger.warning(
                        f"Failed to process a result for budget data for customer {customer_id}: {row_ex}"
                    )
                    continue  # Skip problematic row and proceed

            # Log results
            self.metrics.successful_api_calls += 1
            self.metrics.records_processed += len(processed_data)
            logger.info(
                f"Processed {len(processed_data)} budget records for customer {customer_id}"
            )
            #print(processed_row)
            return processed_data
            
        
        except Exception as ex:
            logger.error(f"Error fetching budget data for customer {customer_id}: {ex}")
            self.metrics.failed_api_calls += 1
            self.metrics.errors.append(str(ex))
            raise

    async def fetch_billing_data(self, customer_id: str) -> List[List[Any]]:
        """Fetch budget data for a specific customer and prepare it for BigQuery appending."""
        try:
            self.metrics.api_calls += 1

            # Fetch budget data from the API
            api_response = self.ads_api.get_billing_data(
                customer_id=customer_id,
                start_date=self.start_date,
                end_date=self.end_date,
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
                    # Extract data with defaults for missing keys
                    customer_data = result.get("customer", {})
                    billing_data = result.get("billingSetup", {})

                    # Check if the customer_id is "9049925758" and update startDateTime
                    start_date_time = "2024-08-01 19:23:02" if customer_id == "9049925758" else billing_data.get("startDateTime", "N/A")

                    # Prepare a row for BigQuery
                    processed_row = [
                        int(customer_data.get("id", "N/A")),  # Customer ID
                        start_date_time  # Billing Setup Start Date Time
                    ]

                    processed_data.append(processed_row)
                except Exception as row_ex:
                    logger.warning(
                        f"Failed to process a result for billing date for customer {customer_id}: {row_ex}"
                    )
                    continue  # Skip problematic row and proceed

            # Log results
            self.metrics.successful_api_calls += 1
            self.metrics.records_processed += len(processed_data)
            logger.info(
                f"Processed {len(processed_data)} billing records for customer {customer_id}"
            )
            #print(processed_row)
            return processed_data

        except Exception as ex:
            logger.error(f"Error fetching billing date for customer {customer_id}: {ex}")
            self.metrics.failed_api_calls += 1
            self.metrics.errors.append(str(ex))
            raise

    @staticmethod
    async def merge_campaign_budget_data(
        campaign_data: List[List[Any]], budget_data: List[List[Any]], billing_data: List[List[Any]]
        ) -> List[List[Any]]:
        """Merge campaign data with budget data based on campaign/customer IDs and date, avoiding duplicate budget entries."""

        # Convert budget data into a dictionary for quick lookup, using (campaign_id, customer_id, date) as key
        budget_dict = {
            (entry[2], entry[0], entry[9]): entry  # (campaign_id, customer_id, date) -> budget entry
            for entry in budget_data
        }
        billing_dict = {
            (entry[0]): entry  # (customer_id) -> billing entry
            for entry in billing_data
        }

        processed_campaigns = set()  # Track processed (campaign_id, customer_id)

        merged_data = []
        for campaign in campaign_data:
            # Campaign identifiers
            campaign_id = campaign[2]  # Campaign ID
            customer_id = campaign[0]  # Customer ID
            campaign_date = campaign[10]  # Campaign date

            # Check if this campaign/customer combination is already processed
            campaign_key = (campaign_id, customer_id, campaign_date)
            if campaign_key in processed_campaigns:
                # Skip budget data for duplicate entries
                budget_entry = None
            else:
                budget_entry = budget_dict.get((campaign_id, customer_id, campaign_date))
                processed_campaigns.add(campaign_key)  # Mark as processed

            # Retrieve billing data (this remains consistent for all rows in a customer)
            billing_entry = billing_dict.get(customer_id)
            
            # Prepare merged row
            merged_row = campaign[:21]  # Start with campaign data

            if budget_entry:
                # Append budget-specific data to the merged row (only for the first occurrence)
                merged_row.extend(
                    [
                        budget_entry[6],  # Budget Status
                        budget_entry[7],  # Budget Period
                        budget_entry[8],  # Budget Amount
                    ]
                )
            else:
                # Append placeholders if no budget match is found or it's a duplicate
                merged_row.extend(
                    [
                        "N/A",  # Budget Status
                        "N/A",  # Budget Period
                        0.0,  # Budget Amount
                    ]
                )

            if billing_entry:
                merged_row.append(billing_entry[1])  # Billing Start Date
            else:
                merged_row.append("N/A")  # Placeholder if no billing data found

            # Add processed timestamp
            merged_row.append(datetime.now().isoformat())

            # Add to final merged dataset
            merged_data.append(merged_row)
        print(merged_row)    

        return merged_data

    def delete_partitions(self):
        """Delete existing partitions for the date range."""
        start_time = datetime.now()
        logger.info(
            f"Deleting partitions for date range: {self.start_date} to {self.end_date}"
        )

        try:
            query = f"""
            DELETE FROM `{self.table_ref}`
            WHERE DATE(date) BETWEEN "{self.start_date}" AND "{self.end_date}"
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
            df = pd.DataFrame(data, columns=self.columns)

            # Convert data types and filter out invalid datetime values
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
            df["billing_start_date"] = pd.to_datetime(df["billing_start_date"], errors="coerce")

            # Convert numeric columns
            numeric_columns = [
                "customer_id",
                "campaign_id",
                "clicks",
                "all_conversions",
                "conversions",
                "conversions_value",
                "conversions_by_date",
                "conversions_value_by_date",
                "cost",
                "all_conversions_value",
                "budget_amount",
                "impressions",
                "interactions"
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

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

            # Define the new schema
            new_schema = {
                "customer_id": bigquery.SchemaField(
                    "customer_id", "INTEGER", mode="NULLABLE"
                ),
                "customer_name": bigquery.SchemaField(
                    "customer_name", "STRING", mode="NULLABLE"
                ),
                "campaign_id": bigquery.SchemaField(
                    "campaign_id", "INTEGER", mode="NULLABLE"
                ),
                "campaign_name": bigquery.SchemaField(
                    "campaign_name", "STRING", mode="NULLABLE"
                ),
                "ad_group_id":bigquery.SchemaField(
                    "ad_group_id", "INTEGER", mode="NULLABLE"
                ),  # Added
                "ad_group_name":bigquery.SchemaField(
                    "ad_group_name", "STRING", mode="NULLABLE"
                ),  # Added
                "clicks": bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
                "all_conversions": bigquery.SchemaField(
                    "all_conversions", "FLOAT", mode="NULLABLE"
                ),
                "conversions": bigquery.SchemaField(
                    "conversions", "FLOAT", mode="NULLABLE"
                ),
                "conversions_value": bigquery.SchemaField(
                    "conversions_value", "FLOAT", mode="NULLABLE"
                ),
                "conversions_by_date": bigquery.SchemaField(
                    "conversions_by_date", "FLOAT", mode="NULLABLE"
                ),
                "conversions_value_by_date": bigquery.SchemaField(
                    "conversions_value_by_date", "FLOAT", mode="NULLABLE"
                ),
                "date": bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
                "currency_code": bigquery.SchemaField(
                    "currency_code", "STRING", mode="NULLABLE"
                ),
                "timezone": bigquery.SchemaField("timezone", "STRING", mode="NULLABLE"),
                "cost": bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
                "all_conversions_value": bigquery.SchemaField(
                    "all_conversions_value", "FLOAT", mode="NULLABLE"
                ),
                "budget_status": bigquery.SchemaField(
                    "budget_status", "STRING", mode="NULLABLE"
                ),
                "budget_period": bigquery.SchemaField(
                    "budget_period", "STRING", mode="NULLABLE"
                ),
                "budget_amount": bigquery.SchemaField(
                    "budget_amount", "FLOAT", mode="NULLABLE"
                ),
                "impressions": bigquery.SchemaField(
                    "impressions", "INTEGER", mode="NULLABLE"
                ),
                "interactions": bigquery.SchemaField(
                    "interactions", "INTEGER", mode="NULLABLE"
                ),
                "device": bigquery.SchemaField("device", "STRING", mode="NULLABLE"),
                "click_type": bigquery.SchemaField("click_type", "STRING", mode="NULLABLE"),
                "billing_start_date": bigquery.SchemaField(
                    "billing_start_date", "DATE", mode="NULLABLE"
                ),
                "processed_at": bigquery.SchemaField(
                    "processed_at", "TIMESTAMP", mode="NULLABLE"
                ),
            }

            # Convert table.schema to a mutable list before making modifications
            new_schema_list = list(table.schema)  # Ensure this is defined at the start

            # Check for schema differences
            schema_updated = False

            for field_name, new_field in new_schema.items():
                if field_name not in existing_schema:
                    logger.info(f"Adding new field to schema: {field_name}")
                    new_schema_list.append(new_field)  # Append new field to the new list
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
                table.schema = new_schema_list  # Assign the modified schema list
                self.bq_client.update_table(table, ["schema"])
                logger.info("BigQuery table schema updated successfully.")

        except Exception as e:
            logger.error(f"Failed to validate/update BigQuery schema: {str(e)}")
            self.metrics.errors.append(str(e))
            raise