"""Configuration settings for the Google Ads Pipeline."""

# Google Cloud Settings
GCP_PROJECT_ID = "ads-data-pipeline-444412"
BIGQUERY_DATASET_ID = "ECH_Google_Ads_Data"
BIGQUERY_TABLE_ID = "ECH_google_ads_campains_pipeline_new_module"

# Google Ads API Settings
GOOGLE_ADS_CLIENT_ID = "471513021246-r9m01cmlr8gcd4tqj0qr8qgij350irgg.apps.googleusercontent.com"
GOOGLE_ADS_CLIENT_SECRET = "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"
GOOGLE_ADS_DEVELOPER_TOKEN = "QBtMyQUAhrKpWNIauasjnA"
GOOGLE_ADS_REFRESH_TOKEN = "1//04csIGMBaUUpkCgYIARAAGAQSNwF-L9IrrNXm-0-6HxIHd3FXI04Ny0-KKlJaptpPE9dvA6hTjNa11-FQCaIc-mD4ZDizPUh4a5c"
GOOGLE_ADS_LOGIN_CUSTOMER_ID = "8085995990"
GOOGLE_ADS_API_VERSION = "v19"
ENABLE_CONSOLE_LOGS = "true"  

# Pipeline Settings
REFRESH_WINDOW_START_DAYS_BACK = 5
REFRESH_WINDOW_END_DAYS_BACK = 0
CUSTOME_ACCOUNTS = ['4336405194']
DURING_PERIOD = "LAST_7_DAYS"

  # {"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time"},
  # {"name": "unique_id", "type": "STRING", "source_field": None},

# COLUMN_DEFINITIONS = """[
#   {"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},
#   {"name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name"},
#   {"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},
#   {"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},
#   {"name": "change_event", "type": "STRING", "is_table": true},
#   {"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time", "is_date_range": true},
#   {"name": "change_resource_type", "type": "STRING", "source_field": "change_event.change_resource_type"},
#   {"name": "change_resource_name", "type": "STRING", "source_field": "change_event.change_resource_name"},
#   {"name": "changed_fields", "type": "STRING", "source_field": "change_event.changed_fields"},
#   {"name": "feed", "type": "STRING", "source_field": "change_event.feed"},
#   {"name": "feed_item", "type": "STRING", "source_field": "change_event.feed_item"},
#   {"name": "old_resource", "type": "STRING", "source_field": "change_event.old_resource"},
#   {"name": "resource_change_operation", "type": "STRING", "source_field": "change_event.resource_change_operation"},
#   {"name": "user_email", "type": "STRING", "source_field": "change_event.user_email"},
#   {"name": "new_resource", "type": "STRING", "source_field": "change_event.new_resource"},
#   {"name": "descriptive_name", "type": "STRING", "source_field": "customer.descriptive_name"},
#   {"name": "client_type", "type": "STRING", "source_field": "change_event.client_type", "filtered": "true", "filter_type": "!=", "filter_value": "GOOGLE_ADS_SCRIPTS"}
# ]"""

  # {"name": "ID", "type": "STRING", "source_field": "None", "is_unique_id": true},
  # {"name": "conversions_from_interactions_rate", "type": "FLOAT64", "source_field": "metrics.conversions_from_interactions_rate"},

COLUMN_DEFINITIONS = """[
  {"name": "campaign", "type": "STRING", "is_table": true},
  {"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},
  {"name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name"},
  {"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},
  {"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},
  {"name": "clicks", "type": "INTEGER", "source_field": "metrics.clicks"},
  {"name": "all_conversions", "type": "FLOAT", "source_field": "metrics.all_conversions"},
  {"name": "conversions", "type": "FLOAT", "source_field": "metrics.conversions"},
  {"name": "conversions_value", "type": "FLOAT", "source_field": "metrics.conversions_value"},
  {"name": "conversions_by_date", "type": "FLOAT", "source_field": "metrics.conversions_by_conversion_date"},
  {"name": "conversions_value_by_date", "type": "FLOAT", "source_field": "metrics.conversions_value_by_conversion_date"},
  {"name": "date", "type": "DATE", "source_field": "segments.date", "is_date_range": true},
  {"name": "currency_code", "type": "STRING", "source_field": "customer.currency_code"},
  {"name": "timezone", "type": "STRING", "source_field": "customer.time_zone"},
  {"name": "cost", "type": "FLOAT", "source_field": "metrics.cost_micros", "transform": "lambda x: float(x) / 1_000_000 if x else 0"},
  {"name": "all_conversions_value", "type": "FLOAT", "source_field": "metrics.all_conversions_value"},
  {"name": "impressions", "type": "INTEGER", "source_field": "metrics.impressions"},
  {"name": "interactions", "type": "INTEGER", "source_field": "metrics.interactions"},
  {"name": "device", "type": "STRING", "source_field": "segments.device"},
  {"name": "processed_at", "type": "TIMESTAMP", "source_field": "None"}
]"""

# [{"name": "change_event", "type": "STRING", "is_table": true},{"name": "ID", "type": "STRING", "source_field": "None", "is_unique_id": true}, {"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},{"name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},{"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},{"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time", "is_date_range": true},{"name": "change_resource_type", "type": "STRING", "source_field": "change_event.change_resource_type"},{"name": "change_resource_name", "type": "STRING", "source_field": "change_event.change_resource_name"},{"name": "changed_fields", "type": "STRING", "source_field": "change_event.changed_fields"},{"name": "feed", "type": "STRING", "source_field": "change_event.feed"},{"name": "feed_item", "type": "STRING", "source_field": "change_event.feed_item"},{"name": "old_resource", "type": "STRING", "source_field": "change_event.old_resource"},{"name": "resource_change_operation", "type": "STRING", "source_field": "change_event.resource_change_operation"},{"name": "user_email", "type": "STRING", "source_field": "change_event.user_email"},{"name": "new_resource", "type": "STRING", "source_field": "change_event.new_resource"},{"name": "descriptive_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "client_type", "type": "STRING", "source_field": "change_event.client_type", "filtered": "true", "filter_type": "!=", "filter_value": "GOOGLE_ADS_SCRIPTS"}]

  # {"name": "app_store", "type": "STRING", "source_field": "campaign.app_campaign_setting.app_store"}

# [{"name": "change_event", "type": "STRING", "is_table": true},{"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time", "is_date_range": true},{"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},{"name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},{"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},{"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time"},{"name": "change_resource_type", "type": "STRING", "source_field": "change_event.change_resource_type"},{"name": "change_resource_name", "type": "STRING", "source_field": "change_event.change_resource_name"},{"name": "changed_fields", "type": "STRING", "source_field": "change_event.changed_fields"},{"name": "feed", "type": "STRING", "source_field": "change_event.feed"},{"name": "feed_item", "type": "STRING", "source_field": "change_event.feed_item"},{"name": "old_resource", "type": "STRING", "source_field": "change_event.old_resource"},{"name": "resource_change_operation", "type": "STRING", "source_field": "change_event.resource_change_operation"},{"name": "user_email", "type": "STRING", "source_field": "change_event.user_email"},{"name": "new_resource", "type": "STRING", "source_field": "change_event.new_resource"},{"name": "descriptive_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "client_type", "type": "STRING", "source_field": "change_event.client_type"}]


# [{"name": "unique_id", "type": "STRING", "source_field": "None"},{"name": "customer_id", "type": "INTEGER", "source_field": "customer.id"},{"name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id"},{"name": "campaign_name", "type": "STRING", "source_field": "campaign.name"},{"name": "change_event", "type": "STRING", "is_table": true},{"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time", "is_date_range": true},{"name": "change_date_time", "type": "DATE", "source_field": "change_event.change_date_time"},{"name": "change_resource_type", "type": "STRING", "source_field": "change_event.change_resource_type"},{"name": "change_resource_name", "type": "STRING", "source_field": "change_event.change_resource_name"},{"name": "changed_fields", "type": "STRING", "source_field": "change_event.changed_fields"},{"name": "feed", "type": "STRING", "source_field": "change_event.feed"},{"name": "feed_item", "type": "STRING", "source_field": "change_event.feed_item"},{"name": "old_resource", "type": "STRING", "source_field": "change_event.old_resource"},{"name": "resource_change_operation", "type": "STRING", "source_field": "change_event.resource_change_operation"},{"name": "user_email", "type": "STRING", "source_field": "change_event.user_email"},{"name": "new_resource", "type": "STRING", "source_field": "change_event.new_resource"},{"name": "descriptive_name", "type": "STRING", "source_field": "customer.descriptive_name"},{"name": "client_type", "type": "STRING", "source_field": "change_event.client_type"}]


# [  { "name": "campaign", "type": "STRING", "is_table": true },  { "name": "date", "type": "DATE", "source_field": "segments.date", "is_date_range": true },  { "name": "customer_id", "type": "INTEGER", "source_field": "customer.id" },  { "name": "customer_name", "type": "STRING", "source_field": "customer.descriptive_name" },  { "name": "campaign_id", "type": "INTEGER", "source_field": "campaign.id" },  { "name": "campaign_name", "type": "STRING", "source_field": "campaign.name" },  { "name": "clicks", "type": "INTEGER", "source_field": "metrics.clicks" },  { "name": "all_conversions", "type": "FLOAT64", "source_field": "metrics.all_conversions" },  { "name": "conversions", "type": "FLOAT64", "source_field": "metrics.conversions" },  { "name": "conversions_value", "type": "FLOAT64", "source_field": "metrics.conversions_value" },  { "name": "conversions_by_date", "type": "FLOAT64", "source_field": "metrics.conversions_by_conversion_date" },  { "name": "conversions_value_by_date", "type": "FLOAT64", "source_field": "metrics.conversions_value_by_conversion_date" },  { "name": "conversions_from_interactions_rate", "type": "FLOAT64", "source_field": "metrics.conversions_from_interactions_rate" },  { "name": "currency_code", "type": "STRING", "source_field": "customer.currency_code" },  { "name": "timezone", "type": "STRING", "source_field": "customer.time_zone" },  { "name": "cost", "type": "FLOAT64", "source_field": "metrics.cost_micros", "transform": "lambda x: float(x) / 1_000_000 if x else 0" },  { "name": "all_conversions_value", "type": "FLOAT64", "source_field": "metrics.all_conversions_value" },  { "name": "impressions", "type": "INTEGER", "source_field": "metrics.impressions" },  { "name": "interactions", "type": "INTEGER", "source_field": "metrics.interactions" },  { "name": "app_store", "type": "STRING", "source_field": "campaign.app_campaign_setting.app_store" }]