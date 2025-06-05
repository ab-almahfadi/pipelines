from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
import json
import logging
import os
import secrets
import wsgiref.simple_server
import wsgiref.util
import webbrowser

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OAuth 2.0 scopes for Google Ads API
SCOPES = [
    'https://www.googleapis.com/auth/adwords'
]

def create_authorization_url(flow):
    """Create authorization URL with state parameter."""
    state = secrets.token_urlsafe(16)
    auth_url, _ = flow.authorization_url(
        access_type='offline',
        state=state,
        prompt='consent',
        include_granted_scopes='true'
    )
    return auth_url, state

def generate_refresh_token():
    """Generate a new refresh token using OAuth 2.0 flow."""
    
    # Client configuration
    client_config = {
        "web": {
            "client_id": "711280396707-3rqfi27ei1hgkf0slt70gqtq3lg0m0sn.apps.googleusercontent.com",
            "client_secret": "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",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost:8080/oauth2callback"]
        }
    }

    try:
        logger.info("Starting OAuth 2.0 flow...")
        
        # Create flow instance
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=client_config['web']['redirect_uris'][0]
        )

        # Get the authorization URL
        auth_url, state = create_authorization_url(flow)
        
        # Store state for verification
        os.environ['OAUTH_STATE'] = state
        
        print("\nPlease visit this URL to authorize this application:")
        print(auth_url)
        print("\nAfter authorization, you'll be redirected to localhost.")
        
        # Start local server to handle the redirect
        host = 'localhost'
        port = 8080
        
        def handle_oauth_callback(environ, start_response):
            """Handle the OAuth 2.0 redirect."""
            args = environ['QUERY_STRING'].split('&')
            pairs = [arg.split('=') for arg in args]
            params = dict(pairs)
            
            if 'error' in params:
                start_response('400 Bad Request', [('Content-Type', 'text/plain')])
                return [b'Authorization failed: ' + params['error'].encode('utf-8')]
            
            if 'code' not in params:
                start_response('400 Bad Request', [('Content-Type', 'text/plain')])
                return [b'Authorization code missing']
            
            # Verify state
            if params.get('state') != os.environ.get('OAUTH_STATE'):
                start_response('400 Bad Request', [('Content-Type', 'text/plain')])
                return [b'State mismatch. CSRF protection triggered.']
            
            # Exchange code for tokens
            flow.fetch_token(code=params['code'])
            credentials = flow.credentials
            
            # Save tokens
            token_data = {
                'refresh_token': credentials.refresh_token,
                'client_id': client_config['web']['client_id'],
                'client_secret': client_config['web']['client_secret'],
                'token_type': credentials.token_type,
                'scopes': credentials.scopes,
            }
            
            with open('google_ads_tokens.json', 'w') as f:
                json.dump(token_data, f, indent=2)
                logger.info("Tokens saved to google_ads_tokens.json")
            
            logger.info("\nNew refresh token generated successfully!")
            logger.info(f"Refresh token: {credentials.refresh_token}")
            
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [b'Authorization successful! You can close this window.']
        
        httpd = wsgiref.simple_server.make_server(host, port, handle_oauth_callback)
        logger.info(f"Waiting for authorization redirect on {host}:{port}...")
        httpd.handle_request()
        
    except Exception as e:
        logger.error(f"Error generating refresh token: {str(e)}")
        raise

if __name__ == "__main__":
    print("\nGenerating new refresh token for Google Ads API...")
    print("=" * 80)
    print("Please follow these steps:")
    print("1. A URL will be displayed below")
    print("2. Copy and paste this URL into your browser")
    print("3. Sign in and authorize the application")
    print("4. You'll be redirected to localhost - allow this")
    print("5. The token will be generated automatically")
    print("=" * 80)
    
    generate_refresh_token()