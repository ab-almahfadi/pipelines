import requests
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class GoogleAdsAuth:
    """Handles Google Ads API authentication."""
    
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        """
        Initialize with OAuth2 credentials.
        
        Args:
            client_id (str): The client ID from Google Cloud Console
            client_secret (str): The client secret from Google Cloud Console
            refresh_token (str): The refresh token from initial OAuth2 flow
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._access_token = None
        self._token_expiry = None

    def get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            str: A valid access token
        """
        if not self._is_token_valid():
            self._refresh_access_token()
        return self._access_token

    def _is_token_valid(self) -> bool:
        """
        Check if the current access token is valid.
        
        Returns:
            bool: True if token exists and is not expired
        """
        if not self._access_token or not self._token_expiry:
            return False
        # Consider token expired if less than 5 minutes remaining
        return datetime.now() + timedelta(minutes=5) < self._token_expiry

    def _refresh_access_token(self):
        """
        Refresh the access token using the refresh token.
        Following Google's OAuth2 specification for refresh token requests.
        """
        try:
            # Prepare the token refresh request
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
                'grant_type': 'refresh_token'
            }
            
            logger.debug("Sending refresh token request")
            response = requests.post(
                url=self.TOKEN_URL,
                headers=headers,
                data=data
            )
            
            # Check for HTTP errors
            if response.status_code != 200:
                error_msg = f"Token refresh failed with status {response.status_code}: {response.text}"
                logger.error(error_msg)
                response.raise_for_status()
            
            # Parse response
            token_data = response.json()
            
            # Required fields check
            if 'access_token' not in token_data:
                raise ValueError("No access_token in response")
            if 'expires_in' not in token_data:
                raise ValueError("No expires_in in response")
            
            # Update token information
            self._access_token = token_data['access_token']
            expires_in = int(token_data['expires_in'])
            self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            logger.info(f"Successfully obtained new access token, expires in {expires_in} seconds")
            logger.debug(f"Token type: {token_data.get('token_type', 'Bearer')}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during token refresh: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Error parsing token response: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during token refresh: {str(e)}")
            raise