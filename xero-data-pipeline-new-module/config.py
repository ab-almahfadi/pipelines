"""Configuration settings for the Xero Data Pipeline."""

# Google Cloud Settings
GCP_PROJECT_ID = "ads-data-pipeline-444412"
BIGQUERY_DATASET_ID = "PAH_Xero_Data"
BIGQUERY_TABLE_ID_INVOICES = "pah_xero_invoices_au"
BIGQUERY_TABLE_ID_CREDIT_NOTES = "pah_xero_credit_notes_au"
BIGQUERY_TABLE_ID_PROFIT_LOSS = "pah_xero_profit_loss_au"
ENABLE_CONSOLE_LOGS = True  # Set to True to enable console logs

# Xero API Settings
XERO_CLIENT_ID = "801AF9C60C4046AA9E556170F8F7F9D2"  # Replace with actual client ID
XERO_CLIENT_SECRET = "r_fJ-zU8lEsE3vwIUK8CbSueqN_2YJ1PfV_piFKJKBV5YfsS"  # Replace with actual client secret
XERO_TENANT_ID = "137f0c84-976c-4da8-be07-cd2e59fe9344"  # From the app script
XERO_REDIRECT_URI = "https://developer.xero.com/app/manage/app/29ba7ca1-4ab1-48db-a92d-03aff1866982/redirecturi"  # Replace with your redirect URI
XERO_SCOPES = "openid profile email accounting.transactions accounting.settings accounting.transactions.read accounting.reports.read offline_access"
XERO_REFRESH_TOKEN = "a9fNyOZ-0Ll1MvtKe4NnLLjsefeeCNnjY_t6QLvoeFs"  # Initial refresh token to use
XERO_SECRET_NAME = "xero-refresh-token-MTM3ZjBjODQtOTc2Yy00ZGE4LWJlMDctY2QyZTU5ZmU5MzQ0"
# Cloud Run Settings
CLOUD_RUN_JOB_NAME = "pah-xero-data-pipeline-new-module"
CLOUD_RUN_EXECUTION_ID = True

# Pipeline Settings
REFRESH_WINDOW_START_DATE = "2022-01-01"  # From the app script
BATCH_SIZE = 100  # Number of records per batch
MAX_RETRIES = 5
RATE_LIMIT_DELAY = 10  # Seconds to wait when rate limited

# Custom product quantity multipliers from the app script
PRODUCT_QUANTITY_MULTIPLIERS = {
    "Digestive EQ - 1 box of 4 x 4kg Tubs": 4,
    "Digestive RP - 1 box of 4 x 4kg Tubs": 4,
    "Digestive VM 1 box of 4 x 4kg Tubs": 4,
    "Digestive EQ - 1 box of 5 x 4kg Sachets": 5,
    "Digestive VM - 1 box of 5 x 4kg Sachets": 5,
    "Stress Paste - 1 box of 12 x 60ml syringes": 12
}

# Column definitions for the various Xero tables
INVOICES_COLUMN_DEFINITIONS = [
    {"name": "type", "type": "STRING", "source_field": "Type"},
    {"name": "invoice_id", "type": "STRING", "source_field": "InvoiceID"},
    {"name": "invoice_number", "type": "STRING", "source_field": "InvoiceNumber"},
    {"name": "reference", "type": "STRING", "source_field": "Reference"},
    {"name": "payments_amount", "type": "FLOAT64", "source_field": "Payments.Amount", "is_nested": True},
    {"name": "amount_due", "type": "FLOAT64", "source_field": "AmountDue"},
    {"name": "amount_paid", "type": "FLOAT64", "source_field": "AmountPaid"},
    {"name": "amount_credited", "type": "FLOAT64", "source_field": "AmountCredited"},
    {"name": "url", "type": "STRING", "source_field": "Url"},
    {"name": "currency_rate", "type": "FLOAT64", "source_field": "CurrencyRate"},
    {"name": "contact_name", "type": "STRING", "source_field": "Contact.Name", "is_nested": True},
    {"name": "date", "type": "DATE", "source_field": "DateString"},
    {"name": "due_date", "type": "DATE", "source_field": "DueDateString"},
    {"name": "status", "type": "STRING", "source_field": "Status"},
    {"name": "unit_amount", "type": "FLOAT64", "source_field": "LineItems.UnitAmount", "is_nested": True},
    {"name": "item_name", "type": "STRING", "source_field": "LineItems.Item.Name", "is_nested": True},
    {"name": "item_code", "type": "STRING", "source_field": "LineItems.Item.Code", "is_nested": True},
    {"name": "quantity", "type": "FLOAT64", "source_field": "LineItems.Quantity", "is_nested": True},
    {"name": "sub_total", "type": "FLOAT64", "source_field": "SubTotal"},
    {"name": "currency_code", "type": "STRING", "source_field": "CurrencyCode"},
    {"name": "account_code", "type": "STRING", "source_field": "LineItems.AccountCode", "is_nested": True},
    {"name": "processed_at", "type": "TIMESTAMP", "auto_generate": True}
]

CREDIT_NOTES_COLUMN_DEFINITIONS = [
    {"name": "type", "type": "STRING", "source_field": "Type"},
    {"name": "credit_note_id", "type": "STRING", "source_field": "CreditNoteID"},
    {"name": "credit_note_number", "type": "STRING", "source_field": "CreditNoteNumber"},
    {"name": "reference", "type": "STRING", "source_field": "Reference"},
    {"name": "amount_credited", "type": "FLOAT64", "source_field": "Total"},
    {"name": "url", "type": "STRING", "source_field": "Attachments.Url", "is_nested": True},
    {"name": "currency_rate", "type": "FLOAT64", "source_field": "CurrencyRate"},
    {"name": "contact_name", "type": "STRING", "source_field": "Contact.Name", "is_nested": True},
    {"name": "date", "type": "DATE", "source_field": "DateString"},
    {"name": "status", "type": "STRING", "source_field": "Status"},
    {"name": "unit_amount", "type": "FLOAT64", "source_field": "LineItems.UnitAmount", "is_nested": True},
    {"name": "item_name", "type": "STRING", "source_field": "LineItems.Item.Name", "is_nested": True},
    {"name": "item_code", "type": "STRING", "source_field": "LineItems.Item.Code", "is_nested": True},
    {"name": "quantity", "type": "FLOAT64", "source_field": "LineItems.Quantity", "is_nested": True},
    {"name": "sub_total", "type": "FLOAT64", "source_field": "SubTotal"},
    {"name": "currency_code", "type": "STRING", "source_field": "CurrencyCode"},
    {"name": "account_code", "type": "STRING", "source_field": "LineItems.AccountCode", "is_nested": True},
    {"name": "processed_at", "type": "TIMESTAMP", "auto_generate": True}
]

PROFIT_LOSS_COLUMN_DEFINITIONS = [
    {"name": "category", "type": "STRING", "source_field": "Section.Title"},
    {"name": "account_name", "type": "STRING", "source_field": "Row.Cells[0].Value", "is_nested": True},
    {"name": "amount", "type": "FLOAT64", "source_field": "Row.Cells[1].Value", "is_nested": True},
    {"name": "date_from", "type": "DATE", "source_field": "ReportDate.FromDate"},
    {"name": "date_to", "type": "DATE", "source_field": "ReportDate.ToDate"},
    {"name": "processed_at", "type": "TIMESTAMP", "auto_generate": True}
]

# Report date ranges
REPORT_DATE_RANGES = {
    "current_year": {
        "from_date": "2024-01-01",
        "to_date": "2024-12-31"
    },
    "prior_year": {
        "from_date": "2023-01-01",
        "to_date": "2023-12-31"
    },
    "current_month": {
        "from_date": "2024-01-01",
        "to_date": "2024-01-31"
    },
    "prior_month": {
        "from_date": "2023-12-01",
        "to_date": "2023-12-31"
    }
}