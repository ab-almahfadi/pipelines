import requests
import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Your credentials
CLIENT_ID = "711280396707-3rqfi27ei1hgkf0slt70gqtq3lg0m0sn.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REFRESH_TOKEN = "1//04tj3l_awuJqxCgYIARAAGAQSNwF-L9IrCtxNiMKkxQhftt5bntZ_BX2ljAGrMNht45o98rKA0GsQx2ktlottJesTyJ96zlti864"

def test_token_refresh():
    """Test OAuth2 token refresh directly."""
    
    url = "https://oauth2.googleapis.com/token"
    
    # Prepare the request exactly as specified in Google's documentation
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }
    
    logger.info("Sending token refresh request...")
    logger.debug(f"URL: {url}")
    logger.debug(f"Headers: {json.dumps(headers, indent=2)}")
    logger.debug(f"Data: {json.dumps(data, indent=2)}")
    
    try:
        response = requests.post(url, headers=headers, data=data)
        logger.info(f"Response status code: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        response_text = response.text
        logger.debug(f"Response body: {response_text}")
        
        if response.status_code == 200:
            token_data = response.json()
            logger.info("Successfully obtained new access token!")
            logger.info(f"Access token preview: {token_data['access_token'][:15]}...")
            logger.info(f"Token type: {token_data.get('token_type', 'Bearer')}")
            logger.info(f"Expires in: {token_data.get('expires_in')} seconds")
            return True
            
        else:
            logger.error(f"Failed to refresh token. Status code: {response.status_code}")
            logger.error(f"Error response: {response_text}")
            return False
            
    except Exception as e:
        logger.error(f"Exception during token refresh: {str(e)}")
        return False

if __name__ == "__main__":
    print("\nStarting OAuth2 token refresh debug test...")
    print("=" * 80)
    success = test_token_refresh()
    print("=" * 80)
    if success:
        print("Token refresh successful!")
    else:
        print("Token refresh failed. Check the logs above for details.")