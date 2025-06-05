"""Configuration settings for the Google Ads Pipeline."""

# Google Cloud Settings
GCP_PROJECT_ID = "ads-data-pipeline-444412"
BIGQUERY_DATASET_ID = "POA_Meta_Ads_Data"
BIGQUERY_TABLE_ID = "poa_meta_account_activities_test"
ENABLE_CONSOLE_LOGS = "True"
# Meta Ads API Settings
META_APP_ID = "571649355412583"
META_APP_SECRET = "540a7581213f85849ca80b0cabecc1f2"
META_ACCESS_TOKEN = "EAAIH6XzD9GcBO7g6tZCm1msKeUMWeM7hcEJZBruYs0mlnIgzhPLIQHIGLrrEfVad36eJXDBjFaeWwZBxXdGJza9ZCFbBiGgXhZCtvrpjJIGZB8xml2xTUhQk2h1tVaLeo7ZCDsdLDL5OiiHM6gL2F6O6ZC23TbqSUpm4YgbGwbL5HtC0SbEdLi0UcC1AcQeqY0vQcRDBdP4pA4Cowmatrj4HkSSKW9wyExpsP0m0"
META_BUSINESS_ID = "377267969556624"
CLOUD_RUN_JOB_NAME = "poa-meta-ads-by-age-gender-pipeline"
ACCOUNTS_BATCH_SIZE = 1000
MAX_RETRIES = 20

# Pipeline Settings
REFRESH_WINDOW_START_DAYS_BACK = 20
REFRESH_WINDOW_END_DAYS_BACK = 0

BATCH_DAYS =  5  # Number of days per batch
ACCOUNT_DELAY_SECONDS = 0  # Delay between accounts
BATCH_DELAY_SECONDS = 0  # Delay between batches
MAX_REQUESTS_PER_HOUR = 10000
COOLDOWN_MINUTES = 4
ENDPOINT = "client_ad_accounts"
CUSTOME_ACCOUNTS = ['377267969556624']
REQUEST_LIMITATION = 1000


# {"name": "campaign_id", "type": "INTEGER", "source_field": "campaign_id"},

# https://graph.facebook.com/v22.0/375171785405606/client_ad_accounts?fields=account_id,name,activities{actor_id,actor_name,application_id,application_name,date_time_in_timezone,event_type,event_time,extra_data,object_id,object_name,object_type,translated_event_type}"


# COLUMN_DEFINITIONS = """[{"name": "ad_id", "type": "INTEGER", "source_field": "id"}, {"name": "ad_name", "type": "STRING", "source_field": "name"}, {"name": "account_id", "type": "INTEGER", "source_field": "account_id"}, {"name": "campaign_id", "type": "INTEGER", "source_field": "campaign_id"}, {"name": "adset_id", "type": "INTEGER", "source_field": "adset_id"}, {"name": "status", "type": "STRING", "source_field": "status"}, {"name": "thumbnail_url", "type": "STRING", "deep_nested": true, "path_to_value": "adcreatives.data.*.thumbnail_url", "array_all": true, "join_delimiter": ", "},{"name": "updated_time", "type": "DATE", "source_field": "updated_time"}]"""

# [
# {"name": "account_id", "type": "STRING", "source_field": "account_id"},
# {"name": "custom_conversion_id", "type": "STRING", "source_field": "id"},
# {"name": "custom_conversion_name", "type": "STRING", "source_field": "name"},
# {"name": "is_archived", "type": "BOOLEAN", "source_field": "is_archived"},
# {"name": "is_unavailable", "type": "BOOLEAN", "source_field": "is_unavailable"},
# {"name": "conversion_status_date", "type": "DATE", "source_field": "updated_time"},
# {"name": "processed_at", "type": "TIMESTAMP", "auto_generate": true}
# ]


COLUMN_DEFINITIONS = """[
{"name": "account_id", "type": "STRING", "source_field": "account_id"},
{"name": "account_name", "type": "STRING", "source_field": "name"},
{"name": "account_status", "type": "BOOLEAN", "source_field": "account_status"},
{"name": "activity_actor_id", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.actor_id", "array_all": true, "join_delimiter": ", "},
{"name": "activity_actor_name", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.actor_name", "array_all": true, "join_delimiter": ", "},
{"name": "activity_application_id", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.application_id", "array_all": true, "join_delimiter": ", "},
{"name": "activity_application_name", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.application_name", "array_all": true, "join_delimiter": ", "},
{"name": "activity_date_time_in_timezone", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.date_time_in_timezone", "array_all": true, "join_delimiter": ", "},
{"name": "activity_extra_data", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.extra_data", "array_all": true, "join_delimiter": ", "},
{"name": "activity_object_id", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.object_id", "array_all": true, "join_delimiter": ", "},
{"name": "activity_object_name", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.object_name", "array_all": true, "join_delimiter": ", "},
{"name": "activity_object_type", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.object_type", "array_all": true, "join_delimiter": ", "},
{"name": "activity_event_type", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.event_type", "array_all": true, "join_delimiter": ", "},
{"name": "activity_event_time", "type": "DATE", "deep_nested": true, "path_to_value": "activities.data.*.event_time", "array_all": true, "join_delimiter": ", ", "is_date_range": true},
{"name": "activity_translated_event", "type": "STRING", "deep_nested": true, "path_to_value": "activities.data.*.translated_event_type", "array_all": true, "join_delimiter": ", "},
{"name": "processed_at", "type": "TIMESTAMP", "source_field": "processed_at", "auto_generate": true}
]"""


# {"name": "campaign_name", "type": "STRING", "source_field": "campaign_name"},
# {"name": "adset_name", "type": "STRING", "source_field": "adset_name"},
# {"name": "ad_name", "type": "STRING", "source_field": "ad_name"},
# {"name": "date", "type": "DATE", "source_field": "date_start"},
# {"name": "spend", "type": "FLOAT64", "source_field": "spend"},
# {"name": "reach", "type": "INTEGER", "source_field": "reach"},
# {"name": "frequency", "type": "FLOAT64", "source_field": "frequency"},
# {"name": "clicks", "type": "INTEGER", "source_field": "clicks"},
# {"name": "link_clicks", "type": "INTEGER", "source_field": "inline_link_clicks"},
# {"name": "impressions", "type": "INTEGER", "source_field": "impressions"},
# {"name": "action_type", "type": "STRING", "source_field": "actions.action_type", "is_nested": true},
# {"name": "action_count", "type": "INTEGER", "source_field": "actions.value", "is_nested": true},
# {"name": "conversions", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "omni_purchase"},
# {"name": "conversion_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "omni_purchase", "is_value_from": "action_values"},
# {"name": "account_currency", "type": "STRING", "source_field": "account_currency"},
# {"name": "age", "type": "STRING", "source_field": "age", "is_breakdown": true},
# {"name": "gender", "type": "STRING", "source_field": "gender", "is_breakdown": true},
# {"name": "processed_at", "type": "TIMESTAMP", "source_field": "processed_at", "auto_generate": true}
# ]"""
# {"name": "device", "type": "STRING", "source_field": "device_platform", "is_breakdown": true},

# COLUMN_DEFINITIONS = """[
#     {"name": "name", "type": "STRING", "source_field": "name"},
#     {"name": "age", "type": "FLOAT64", "source_field": "age"},
#     {"name": "balance", "type": "INTEGER", "source_field": "balance"},
#     {"name": "currency", "type": "STRING", "source_field": "currency"},
#     {"name": "business_name", "type": "STRING", "source_field": "business_name"},
#     {"name": "owner", "type": "STRING", "source_field": "owner"},
#     {"name": "created_time", "type": "DATE", "source_field": "created_time"},
#     {"name": "spend_cap", "type": "FLOAT64", "source_field": "spend_cap"},
#     {"name": "account_status", "type": "INTEGER", "source_field": "account_status"},
#     {"name": "amount_spent", "type": "FLOAT64", "source_field": "amount_spent"},
#     {"name": "disable_reason", "type": "INTEGER", "source_field": "disable_reason"},
#     {"name": "timezone_name", "type": "STRING", "source_field": "timezone_name"}
# ]"""


# COLUMN_DEFINITIONS = """[
#     {"name": "campaign_name", "type": "STRING", "source_field": "campaign_name"},
#     {"name": "adset_name", "type": "STRING", "source_field": "adset_name"},
#     {"name": "adset_id", "type": "STRING", "source_field": "adset_id"},
#     {"name": "ad_name", "type": "STRING", "source_field": "ad_name"},
#     {"name": "date", "type": "DATE", "source_field": "date_start"},
#     {"name": "spend", "type": "FLOAT64", "source_field": "spend"},
#     {"name": "impressions", "type": "INTEGER", "source_field": "impressions"},
#     {"name": "purchases", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "purchase"},
#     {"name": "purchase_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "purchase", "is_value_from": "action_values"},
#     {"name": "video_play_actions", "type": "INTEGER", "source_field": "video_play_actions.value", "is_nested": true, "action_filter": "video_view"},
#     {"name": "video_thruplay_watched_actions", "type": "INTEGER", "source_field": "video_thruplay_watched_actions.value", "is_nested": true, "action_filter": "video_view"}
# ]"""


    
# {"name": "link_clicks", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "link_click"},
    # {"name": "offsite_conversions", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "offsite_conversion.fb_pixel_purchase"},
    # {"name": "offsite_conversion_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "offsite_conversion.fb_pixel_purchase", "is_value_from": "action_values"},

# {"name": "purchases", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "purchase"},
# {"name": "purchase_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "purchase", "is_value_from": "action_values"},
# {"name": "leads", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "lead"},
# {"name": "landing_page_views", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "landing_page_view"},



# [{"name":"account_id","type":"STRING","source_field":"account_id"},{"name":"account_name","type":"STRING","source_field":"account_name"},{"name":"campaign_id","type":"INTEGER","source_field":"campaign_id"},{"name":"campaign_name","type":"STRING","source_field":"campaign_name"},{"name":"adset_name","type":"STRING","source_field":"adset_name"},{"name":"ad_name","type":"STRING","source_field":"ad_name"},{"name":"date","type":"DATE","source_field":"date_start"},{"name":"spend","type":"FLOAT64","source_field":"spend"},{"name":"reach","type":"INTEGER","source_field":"reach"},{"name":"frequency","type":"FLOAT64","source_field":"frequency"},{"name":"clicks","type":"INTEGER","source_field":"clicks"},{"name":"impressions","type":"INTEGER","source_field":"impressions"},{"name":"action_type","type":"STRING","source_field":"actions.action_type","is_nested":true},{"name":"action_count","type":"INTEGER","source_field":"actions.value","is_nested":true},{"name":"purchases","type":"INTEGER","source_field":"actions.value","is_nested":true,"action_filter":"purchase"},{"name":"purchase_value","type":"FLOAT64","source_field":"action_values.value","is_nested":true,"action_filter":"purchase","is_value_from":"action_values"},{"name":"leads","type":"INTEGER","source_field":"actions.value","is_nested":true,"action_filter":"lead"},{"name":"landing_page_views","type":"INTEGER","source_field":"actions.value","is_nested":true,"action_filter":"landing_page_view"},{"name":"link_clicks","type":"INTEGER","source_field":"actions.value","is_nested":true,"action_filter":"link_click"},{"name":"offsite_conversions","type":"INTEGER","source_field":"actions.value","is_nested":true,"action_filter":"offsite_conversion.fb_pixel_purchase"},{"name":"offsite_conversion_value","type":"FLOAT64","source_field":"action_values.value","is_nested":true,"action_filter":"offsite_conversion.fb_pixel_purchase","is_value_from":"action_values"},{"name":"account_currency","type":"STRING","source_field":"account_currency"},{"name":"age","type":"STRING","source_field":"age","is_breakdown":true},{"name":"gender","type":"STRING","source_field":"gender","is_breakdown":true},{"name":"processed_at","type":"TIMESTAMP","source_field":"processed_at","auto_generate":true}]


# [{"name": "campaign_name", "type": "STRING", "source_field": "campaign_name"},{"name": "adset_name", "type": "STRING", "source_field": "adset_name"},{"name": "adset_id", "type": "STRING", "source_field": "adset_id"},{"name": "ad_name", "type": "STRING", "source_field": "ad_name"},{"name": "date", "type": "DATE", "source_field": "date_start"},{"name": "spend", "type": "FLOAT64", "source_field": "spend"},{"name": "impressions", "type": "INTEGER", "source_field": "impressions"},{"name": "purchases", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "purchase"},{"name": "purchase_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "purchase", "is_value_from": "action_values"},{"name": "video_play_actions", "type": "INTEGER", "source_field": "actions.video_play_actions", "is_nested": true, "action_filter": "video_view"},{"name": "video_thruplay_watched_actions", "type": "INTEGER", "source_field": "actions.video_thruplay_watched_actions", "is_nested": true, "action_filter": "video_view"}]


# COLUMN_DEFINITIONS = """[{"name": "account_id", "type": "STRING", "source_field": "account_id"},{"name": "account_name", "type": "STRING", "source_field": "account_name"},{"name": "campaign_name", "type": "STRING", "source_field": "campaign_name"},{"name": "adset_name", "type": "STRING", "source_field": "adset_name"},{"name": "ad_name", "type": "STRING", "source_field": "ad_name"},{"name": "date", "type": "DATE", "source_field": "date_start"},{"name": "spend", "type": "FLOAT64", "source_field": "spend"},{"name": "reach", "type": "INTEGER", "source_field": "reach"},{"name": "frequency", "type": "FLOAT64", "source_field": "frequency"},{"name": "clicks", "type": "INTEGER", "source_field": "clicks"},{"name": "link_clicks", "type": "INTEGER", "source_field": "inline_link_clicks"},{"name": "impressions", "type": "INTEGER", "source_field": "impressions"},{"name": "action_type", "type": "STRING", "source_field": "actions.action_type", "is_nested": true},{"name": "action_count", "type": "INTEGER", "source_field": "actions.value", "is_nested": true},{"name": "conversions", "type": "INTEGER", "source_field": "actions.value", "is_nested": true, "action_filter": "purchase"},{"name": "conversion_value", "type": "FLOAT64", "source_field": "action_values.value", "is_nested": true, "action_filter": "purchase", "is_value_from": "action_values"},{"name": "account_currency", "type": "STRING", "source_field": "account_currency"},{"name": "device", "type": "STRING", "source_field": "device_platform", "is_breakdown": true},{"name": "processed_at", "type": "TIMESTAMP", "source_field": "processed_at", "auto_generate": true} ]"""

# [{"name": "account_id", "type": "STRING", "source_field": "account_id"}, {"name": "campaign_id", "type": "STRING", "source_field": "campaign_id"}, {"name": "adset_id", "type": "STRING", "source_field": "adset_id"}, {"name": "ad_id", "type": "STRING", "source_field": "id"}, {"name": "ad_name", "type": "STRING", "source_field": "name"}, {"name": "status", "type": "STRING", "source_field": "status"}, {"name": "configured_status", "type": "STRING", "source_field": "configured_status"}, {"name": "effective_status", "type": "STRING", "source_field": "effective_status"}, {"name": "created_time", "type": "TIMESTAMP", "source_field": "created_time"}, {"name": "updated_time", "type": "DATE", "source_field": "updated_time"}, {"name": "disapproval_reasons", "type": "STRING", "source_field": "ad_creative.disapproval_reasons", "is_nested": true}, {"name": "recommendations", "type": "STRING", "source_field": "ad_creative.recommendations", "is_nested": true}, {"name": "processed_at", "type": "TIMESTAMP", "auto_generate": true}]


[            { "name": "account_id", "type": "STRING", "source_field": "account_id" },            { "name": "campaign_id", "type": "STRING", "source_field": "id" },            { "name": "campaign_name", "type": "STRING", "source_field": "name" },            { "name": "budget_remaining", "type": "FLOAT64", "source_field": "budget_remaining" },            { "name": "daily_budget", "type": "FLOAT64", "source_field": "daily_budget" },            { "name": "effective_status", "type": "STRING", "source_field": "effective_status" },            { "name": "status", "type": "STRING", "source_field": "status" },            { "name": "start_time", "type": "TIMESTAMP", "source_field": "start_time" },            { "name": "created_time", "type": "TIMESTAMP", "source_field": "created_time" },            { "name": "updated_time", "type": "TIMESTAMP", "source_field": "updated_time" },            { "name": "configured_status", "type": "STRING", "source_field": "configured_status" }          ]