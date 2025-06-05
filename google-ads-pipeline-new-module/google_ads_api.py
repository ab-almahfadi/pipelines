import requests
import logging
import json
from typing import Dict, Any, List, Optional
from google_auth import GoogleAdsAuth

logger = logging.getLogger(__name__)


class GoogleAdsAPI:
    """Google Ads API service."""

    API_VERSION = "v18"
    BASE_URL = "https://googleads.googleapis.com"

    def __init__(
        self, auth: GoogleAdsAuth, developer_token: str, login_customer_id: str
    ):
        self.auth = auth
        self.developer_token = developer_token
        self.login_customer_id = login_customer_id.replace("-", "")  # Remove any dashes

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        access_token = self.auth.get_access_token()
        logger.info(f"Access token obtained: {access_token[:10]}...")

        headers = {
            "Authorization": f"Bearer {access_token}",
            "developer-token": self.developer_token,
            "login-customer-id": self.login_customer_id,
            "Content-Type": "application/json",
        }
        logger.info("Headers prepared (excluding sensitive data)")
        return headers

    def search(
        self, customer_id: str, query: str, page_size: int = 100000
    ) -> Dict[str, Any]:
        """Execute a GAQL search query."""
        # Format customer ID
        customer_id = customer_id.replace("-", "")
        url = f"{self.BASE_URL}/{self.API_VERSION}/customers/{customer_id}/googleAds:search"

        # Format request payload according to Google Ads API specification
        payload = {"query": query.strip()}

        logger.info(f"Making request to: {url}")
        logger.info(f"Query: {query}")

        try:
            # Make POST request
            response = requests.post(url=url, headers=self._get_headers(), json=payload)

            logger.info(f"Response status code: {response.status_code}")
            logger.debug(
                f"Response content: {response.text[:200]}..."
            )  # Log first 200 chars

            if response.status_code != 200:
                error_text = response.text
                logger.error(f"Error response: {error_text}")
                try:
                    error_json = response.json()
                    logger.error(f"Error details: {error_json}")
                except:
                    pass
                response.raise_for_status()

            # Process response
            try:
                data = response.json()
                logger.info(f"Successfully parsed JSON response")
                return data
            except json.JSONDecodeError:
                logger.error("Failed to parse response as JSON")
                logger.error(f"Raw response: {response.text}")
                raise

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error occurred: {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Error Response Content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
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
            # Fetch response from the API
            response = self.search(manager_customer_id, query)
            # print(response)
            customer_ids = []

            # Validate and process the response
            if not isinstance(response, dict) or "results" not in response:
                raise ValueError("Unexpected response format from the API")

            # Extract customer IDs
            for result in response.get("results", []):
                customer_client = result.get("customerClient", {})
                customer_id = customer_client.get("id")

                # Exclude the manager customer ID from the results
                if customer_id and customer_id != manager_customer_id:
                    customer_ids.append(customer_id)

            # Log results
            logger.info(f"Found {len(customer_ids)} customer IDs")
            if customer_ids:
                logger.debug("Sample of customer IDs: %s", customer_ids[:5])

            return customer_ids

        except Exception as e:
            logger.error(f"Failed to get customer clients: {str(e)}")
            raise