"""Configuration settings for the Google Ads Pipeline."""

# Google Cloud Settings
GCP_PROJECT_ID = "ads-data-pipeline-444412"
BIGQUERY_DATASET_ID = "ECH_Google_Ads_Data"
BIGQUERY_TABLE_ID = "ECH_google_ads_by_country_pipeline"

# Google Ads API Settings
GOOGLE_ADS_CLIENT_ID = "1078797522899-tme2lpvptbdjfbe9fid788pvp9bkmdr8.apps.googleusercontent.com"
GOOGLE_ADS_CLIENT_SECRET = "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"
GOOGLE_ADS_DEVELOPER_TOKEN = "r4Eeq6ge2pbwDivOVDvLbw"
GOOGLE_ADS_REFRESH_TOKEN = "1//04jzvcqG1yhbjCgYIARAAGAQSNwF-L9IrENXNk_cBKckTvGpiqw7Z7SyKx3QirQgJDmYNQNCwothNPxkSuTC8MEF35RjklD-N0Rc"
GOOGLE_ADS_LOGIN_CUSTOMER_ID = "8085995990"
GOOGLE_ADS_API_VERSION = "v18"

# Pipeline Settings
REFRESH_WINDOW_START_DAYS_BACK = 20
REFRESH_WINDOW_END_DAYS_BACK = 0
LOG_EXECUTION_ID = True