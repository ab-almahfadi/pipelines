import requests
import logging
import json
from typing import Dict, Any, List, Optional
from google_auth import GoogleAdsAuth
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class GoogleAdsAPI:
    """Google Ads API service with retry logic."""

    API_VERSION = "v18"
    BASE_URL = "https://googleads.googleapis.com"

    def __init__(
        self, auth: GoogleAdsAuth, developer_token: str, login_customer_id: str
    ):
        self.auth = auth
        self.developer_token = developer_token
        self.login_customer_id = login_customer_id.replace("-", "")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        access_token = self.auth.get_access_token()
        logger.info(f"Access token obtained: {access_token[:10]}...")

        return {
            "Authorization": f"Bearer {access_token}",
            "developer-token": self.developer_token,
            "login-customer-id": self.login_customer_id,
            "Content-Type": "application/json",
        }

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff (2s, 4s, 8s)
        retry=retry_if_exception_type((requests.exceptions.RequestException, json.JSONDecodeError)),
        reraise=True
    )
    def _make_request(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handles making API requests with retry logic."""
        logger.info(f"Making request to: {url}")
        
        response = requests.post(url=url, headers=self._get_headers(), json=payload)
        logger.info(f"Response status code: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Error response: {response.text}")
            response.raise_for_status()

        try:
            return response.json()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {response.text}")
            raise e

    def search(
        self, customer_id: str, query: str, page_size: int = 100000
    ) -> Dict[str, Any]:
        """Execute a GAQL search query with retry logic."""
        customer_id = customer_id.replace("-", "")
        url = f"{self.BASE_URL}/{self.API_VERSION}/customers/{customer_id}/googleAds:search"
        payload = {"query": query.strip()}

        try:
            return self._make_request(url, payload)
        except Exception as e:
            logger.error(f"Failed search request for customer {customer_id}: {e}")
            raise

    def get_customer_clients(self, manager_customer_id: str) -> List[str]:
        """Get all customer client IDs for a manager account."""
        query = """
            SELECT 
                customer_client.id,
                customer_client.descriptive_name,
                customer_client.status,
                customer_client.time_zone,
                customer_client.manager
            FROM customer_client
            WHERE 
                customer_client.manager = FALSE
        """

        try:
            response = self.search(manager_customer_id, query)
            customer_ids = [
                result["customerClient"]["id"]
                for result in response.get("results", [])
                if result.get("customerClient", {}).get("id") != manager_customer_id
            ]
            logger.info(f"Found {len(customer_ids)} customer IDs")
            return customer_ids
        except Exception as e:
            logger.error(f"Failed to get customer clients: {e}")
            raise

    def get_customer_activity(
        self, customer_id: str, start_date: str, end_date: str, API_limit: str, During_period: str
    ) -> List[Dict[str, Any]]:
        """Get campaign performance data with retry logic."""
        query = f"""
        SELECT change_event.asset, 
            change_event.change_date_time, 
            change_event.change_resource_type, 
            change_event.change_resource_name, 
            change_event.changed_fields, 
            change_event.feed, 
            change_event.feed_item, 
            change_event.old_resource, 
            change_event.resource_change_operation, 
            change_event.user_email, 
            campaign.name, 
            change_event.new_resource, change_event.ad_group, 
            customer.descriptive_name, 
            change_event.client_type
        FROM change_event
        WHERE change_event.change_date_time DURING {During_period} 
              AND change_event.client_type != 'GOOGLE_ADS_SCRIPTS'
        ORDER BY change_event.change_date_time ASC
        LIMIT {API_limit}
        """
        # SELECT change_event.asset, change_event.change_resource_type, change_event.change_resource_name, change_event.change_date_time, change_event.changed_fields, change_event.feed, change_event.feed_item, change_event.old_resource, change_event.resource_change_operation, change_event.user_email, campaign.name, change_event.new_resource, change_event.ad_group, customer.descriptive_name, change_event.client_type FROM change_event WHERE change_event.change_date_time DURING LAST_7_DAYS AND change_event.client_type != 'GOOGLE_ADS_SCRIPTS' ORDER BY change_event.change_date_time ASC LIMIT 1000

        try:
            return self.search(customer_id, query)
        except Exception as e:
            logger.error(f"Failed to get campaign performance for customer {customer_id}: {e}")
            raise
