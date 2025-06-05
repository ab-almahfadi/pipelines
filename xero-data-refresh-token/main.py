import base64
import requests
from logger import LoggerConfig
import config
import os
import traceback

logger = LoggerConfig.get_logger_from_config(config, __name__)
BUCKET_NAME = 'pah-xero-refresh-token-bucket'
TOKEN_FILE_NAME = 'refresh_token.txt'
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", config.GCP_PROJECT_ID)
client_id = os.environ.get("XERO_CLIENT_ID", config.XERO_CLIENT_ID)
client_secret = os.environ.get("XERO_CLIENT_SECRET", config.XERO_CLIENT_SECRET)


def get_refresh_token_from_gcs() -> str:
    """
    Get refresh token from Google Cloud Storage bucket.
    Replicates getRefreshTokenFromGCS function from App Script.
    
    Returns:
        The refresh token as string or None if not found
    """
    try:
        from google.cloud import storage
        
        logger.info(f"Getting refresh token from GCS bucket: {BUCKET_NAME}/{TOKEN_FILE_NAME}")
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(TOKEN_FILE_NAME)
        
        if blob.exists():
            token = blob.download_as_text()
            logger.info("Retrieved refresh token from GCS bucket")
            return token
        else:
            logger.warning(f"Refresh token file not found in GCS bucket: {TOKEN_FILE_NAME}")
            return None
            
    except Exception as e:
        logger.error(f"Error reading refresh token from GCS: {str(e)}")
        logger.error(f"Full exception: {traceback.format_exc()}")
        return None


def get_refresh_token() -> str:
    """
    Get the refresh token from various sources with fallbacks.
    First tries GCS bucket, then falls back to environment variable.
    Replicates getRefreshToken function from App Script.
    
    Returns:
        The refresh token or None if not found
    """
    # First try to get from GCS
    token_from_gcs = get_refresh_token_from_gcs()
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

def update_xero_access_token(refresh_token):
    try:
        if not all([client_id, client_secret, refresh_token]):
            logger.error("Missing required environment variables")
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
            logger.info("Got 200 stus code from Xero")
            response_data = response.json()
            new_refresh_token = response_data['refresh_token']
            save_refresh_token(new_refresh_token)
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        print(traceback.format_exc())

def save_refresh_token(token_value) -> bool:
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
    gcs_success = save_refresh_token_to_gcs(token_value)
    if gcs_success:
        success = True
    return success

def save_refresh_token_to_gcs(token_value) -> bool:
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
        logger.info(f"Saving refresh token to GCS bucket: {BUCKET_NAME}/{TOKEN_FILE_NAME}")
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(TOKEN_FILE_NAME)
        
        blob.upload_from_string(token_value, content_type="text/plain")
        logger.info("Saved refresh token to GCS bucket successfully")
        return True
            
    except Exception as e:
        logger.error(f"Error saving refresh token to GCS: {str(e)}")
        logger.error(f"Full exception: {traceback.format_exc()}")
        return False

current_refresh_token = get_refresh_token()
if current_refresh_token: # Ensure a token was actually retrieved
    update_xero_access_token(current_refresh_token)
else:
    logger.critical("CRITICAL: Could not obtain a refresh token to use. Aborting update.")