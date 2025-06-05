"""Configuration settings for the Google Ads Pipeline."""

# Google Cloud Settings
GCP_PROJECT_ID = "ads-data-pipeline-444412"
BIGQUERY_DATASET_ID = "LDX_Google_Ads_Data"
BIGQUERY_TABLE_ID = "LDX_google_ads_metrics_pipeline"

# Google Ads API Settings
GOOGLE_ADS_CLIENT_ID = "1069866743003-iod9blu6apn07koiobbgj1h821euo1c9.apps.googleusercontent.com"
GOOGLE_ADS_CLIENT_SECRET = "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"
GOOGLE_ADS_DEVELOPER_TOKEN = "hjiQ1bPou_rMKKSLQ0grrw"
GOOGLE_ADS_REFRESH_TOKEN = "1//04tj3l_awuJqxCgYIARAAGAQSNwF-L9IrCtxNiMKkxQhftt5bntZ_BX2ljAGrMNht45o98rKA0GsQx2ktlottJesTyJ96zlti864"
GOOGLE_ADS_LOGIN_CUSTOMER_ID = "3738177701"
GOOGLE_ADS_API_VERSION = "v18"

# Pipeline Settings
REFRESH_WINDOW_START_DAYS_BACK = 4000
REFRESH_WINDOW_END_DAYS_BACK = 0
LOG_EXECUTION_ID = True