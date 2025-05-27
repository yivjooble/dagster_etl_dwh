import os.path
import pickle
from typing import List, Dict, Any, Optional
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient import errors

from dagster import ConfigurableResource
from utility_hub.core_tools import get_creds_from_vault


class PostmasterAPI:
    """
    Handler for Google Postmaster Tools API operations.

    This class manages authentication and data retrieval from Google Postmaster Tools API.
    It supports both initial OAuth2 authentication and token refresh, with the ability to
    save and load authentication tokens for subsequent uses.

    Attributes:
        SCOPES (List[str]): Required OAuth2 scopes for API access
        API_NAME (str): Name of the Google API service
        API_VERSION (str): Version of the API to use
    """

    SCOPES = ["https://www.googleapis.com/auth/postmaster.readonly"]
    API_NAME = 'gmailpostmastertools'
    API_VERSION = 'v1'

    def __init__(
        self,
        context,
        credentials_file: str,
        token_file: str = "token.pickle",
    ):
        """
        Initialize PostmasterAPI handler.

        Args:
            context: Dagster operation context for logging
            credentials_file (str): Path to OAuth client_secrets.json
            token_file (str): Path to save/load OAuth tokens. Should be a path within
                            the job directory to ensure persistence in Docker environment
        """
        self.context = context
        self.credentials_file = credentials_file
        self.token_file = os.path.join(os.path.dirname(__file__), token_file)
        self.service = None
        self.credentials = None
        self._domains_cache = None  # Add cache for domains

    def authenticate(self) -> None:
        """
        Handle OAuth 2.0 authentication flow.
        Supports both initial authentication and token refresh.
        Uses pre-existing token.pickle if available.

        Raises:
            Exception: If authentication fails or token cannot be refreshed/created
        """
        creds = None

        # Load existing tokens if available
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
                self.context.log.info("Loaded existing credentials from token file")
            except Exception as e:
                self.context.log.error(f"Error loading credentials from token file: {e}")

        # Refresh or create new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    self.context.log.info("Refreshed expired credentials")
                except Exception as e:
                    self.context.log.error(f"Error refreshing credentials: {e}")
                    creds = None

            # If no valid credentials available, run OAuth flow
            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, self.SCOPES)
                    creds = flow.run_local_server(port=0)
                    self.context.log.info("Generated new credentials through OAuth flow")
                except Exception as e:
                    self.context.log.error(f"Error in OAuth flow: {e}")
                    raise

            # Save valid credentials
            try:
                with open(self.token_file, 'wb') as token:
                    pickle.dump(creds, token)
                self.context.log.info("Saved credentials to token file")
            except Exception as e:
                self.context.log.error(f"Error saving credentials to token file: {e}")

        self.credentials = creds
        self.service = build(
            self.API_NAME,
            self.API_VERSION,
            credentials=self.credentials
        )

    def get_domains(self, country: str) -> List[str]:
        """
        Fetch domain for specific country from Postmaster Tools.

        Args:
            country (str): Two-letter country code to filter domains

        Returns:
            List[str]: List containing domain name for the country
        """
        if not self.service:
            self.context.log.error("Service not initialized. Call authenticate() first")
            raise RuntimeError("Authentication required")

        # Define excluded domains
        EXCLUDED_DOMAINS = {
            'by.jooble.org',
            'cu.jooble.org',
            'ru.jooble.org',
            've.jooble.org',
            'us.jooble.org',  # US uses jooble.org
            'cn.jooble.org',
            'jp.jooble.org'
        }

        # Use cached domains if available
        if self._domains_cache is None:
            try:
                response = self.service.domains().list().execute()
                all_domains = {
                    domain['name'].split('/')[-1]
                    for domain in response.get('domains', [])
                }
                # Filter out excluded domains before caching
                self._domains_cache = all_domains - EXCLUDED_DOMAINS
                self.context.log.debug(
                    f"Cached {len(self._domains_cache)} domains from API\n"
                    f"Excluded {len(EXCLUDED_DOMAINS)} domains from total {len(all_domains)}\n"
                    f"Excluded domains: {EXCLUDED_DOMAINS}"
                )
            except errors.HttpError as error:
                self.context.log.error(f"Error fetching domains: {error}")
                raise

        # Get domain for country
        target_domain = 'jooble.org' if country.lower() == 'us' else f"{country.lower()}.jooble.org"
        filtered_domains = [d for d in self._domains_cache if d == target_domain]

        if not filtered_domains:
            self.context.log.warning(f"No domain found for country: {country}")
            return []

        self.context.log.debug(f"Found domain for country {country}: {filtered_domains}")
        return filtered_domains

    def get_traffic_stats(
        self,
        domain: str,
        date: str,
        retry_count: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Get traffic stats for a specific domain and date.

        This method handles API requests with retry logic for rate limits
        and other temporary failures.

        Args:
            domain (str): Domain name (e.g., 'example.com')
            date (str): Date in YYYYMMDD format
            retry_count (int): Number of retries on failure (default: 3)

        Returns:
            Optional[Dict[str, Any]]: Dictionary containing traffic stats or None on failure

        Note:
            Uses exponential backoff for retries on rate limit (429) errors
        """
        if not self.service:
            self.context.log.error("Service not initialized. Call authenticate() first")
            raise RuntimeError("Authentication required")

        query = f"domains/{domain}/trafficStats/{date}"

        for attempt in range(retry_count):
            try:
                stats = self.service.domains().trafficStats().get(name=query).execute()
                self.context.log.debug(f"Retrieved stats for domain: {domain}\nDate: {date}")
                return stats
            except errors.HttpError as error:
                if error.resp.status == 429:  # Rate limit exceeded
                    wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60 seconds
                    self.context.log.warning(
                        f"Rate limit hit for {domain}, waiting {wait_time} seconds. "
                        f"Attempt {attempt + 1}/{retry_count}"
                    )
                    time.sleep(wait_time)
                    continue
                self.context.log.error(f"Error fetching stats for {domain}: {error}")
                return None
            except Exception as e:
                self.context.log.error(f"Unexpected error for {domain}: {e}")
                return None

        return None

    def get_country_domains_stats(
        self,
        date: str,
        country: str
    ) -> Dict[str, Dict]:
        """
        Get traffic stats for country domain for a specific date.

        Args:
            date (str): Date in YYYYMMDD format
            country (str): Two-letter country code

        Returns:
            Dict[str, Dict]: Dictionary mapping domain names to their stats.
            Example:
            {
                'uk.jooble.org': {
                    'userReportedSpamRatio': 0.1,
                    'domainReputation': 'HIGH',
                    ...
                }
            }
        """
        results = {}
        domains = self.get_domains(country)

        for domain in domains:
            stats = self.get_traffic_stats(domain, date)
            if stats:
                results[domain] = stats
            else:
                self.context.log.warning(f"No stats retrieved for domain: {domain}\nDate: {date}")

        return results


class PostmasterResource(ConfigurableResource):
    """Resource that provides access to Google Postmaster Tools API"""

    def get_api(self, context) -> PostmasterAPI:
        """Initialize and return authenticated PostmasterAPI instance"""
        # Get credentials from vault and create credentials file
        credentials_content = get_creds_from_vault('GOOGLE_POSTMASTER_OAUTH')
        temp_creds_file = "temp_credentials.json"

        try:
            # Write credentials to temporary file
            with open(temp_creds_file, 'w') as f:
                f.write(credentials_content)

            # Initialize API and authenticate
            api = PostmasterAPI(
                context=context,
                credentials_file=temp_creds_file,
                token_file="token.pickle"
            )
            api.authenticate()
            context.log.debug("Successfully authenticated with Google Postmaster API")
            return api
        finally:
            # Cleanup temporary credentials file
            if os.path.exists(temp_creds_file):
                os.remove(temp_creds_file)
