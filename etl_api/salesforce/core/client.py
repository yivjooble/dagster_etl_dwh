import jwt
import requests
from datetime import datetime, timedelta, timezone
from simple_salesforce.api import Salesforce
from utility_hub.core_tools import get_creds_from_vault


class SalesforceJWTClient:
    """
    Client for authenticating with Salesforce using JWT bearer flow.

    This class handles authentication with Salesforce using JWT (JSON Web Token)
    bearer flow authentication. It automatically retrieves credentials from vault
    and manages token generation and client creation.

    Attributes:
        client_id (str): Connected App's consumer key from vault
        username (str): Salesforce username from vault
        private_key (str): Private key for JWT signing from vault
        domain (str): Salesforce domain (defaults to test.salesforce.com)
        instance_url (str): Salesforce instance URL from vault

    Example:
        >>> client = SalesforceJWTClient()
        >>> sf = client.get_client()
        >>> # Use sf for Salesforce operations
    """
    # Default domain for Salesforce authentication
    DEFAULT_DOMAIN = 'login.salesforce.com'

    def __init__(self, domain: str = None):
        """
        Initialize Salesforce JWT Client using vault credentials.

        Args:
            domain (str, optional): Salesforce domain. Defaults to test.salesforce.com.
                Use test.salesforce.com for sandbox and login.salesforce.com for production.
        """
        self.client_id = get_creds_from_vault('SF_CLIENT_ID')
        self.username = get_creds_from_vault('SF_USERNAME')
        self.private_key = get_creds_from_vault('SF_PRIVATE_KEY')
        self.domain = domain or self.DEFAULT_DOMAIN
        self.instance_url = get_creds_from_vault('SF_INSTANCE_URL')

    def get_client(self) -> Salesforce:
        """
        Get authenticated Salesforce client.

        Creates and returns a Salesforce client instance authenticated with JWT flow.
        Handles token generation and client initialization automatically.

        Returns:
            Salesforce: Authenticated Salesforce client instance ready for API operations.

        Raises:
            Exception: If authentication fails or token generation fails.
        """
        access_token = self._get_access_token()
        return Salesforce(
            instance_url=self.instance_url,
            session_id=access_token
        )

    def _get_access_token(self) -> str:
        """Get Salesforce access token using JWT"""
        jwt_token = self._generate_jwt()

        token_url = f"https://{self.domain}/services/oauth2/token"
        payload = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token
        }

        response = requests.post(token_url, data=payload)
        if response.status_code == 200:
            return response.json()['access_token']
        else:
            raise Exception(f"Failed to get access token: {response.text}\n")

    def _generate_jwt(self) -> str:
        """Generate JWT token"""
        header = {
            "alg": "RS256",
            "typ": "JWT"
        }

        current_time = datetime.now(timezone.utc)
        exp_time = current_time + timedelta(minutes=3)
        payload = {
            "iss": self.client_id,
            "aud": f"https://{self.domain}",
            "sub": self.username,
            "exp": int(exp_time.timestamp()),
        }

        return jwt.encode(
            payload,
            self.private_key,
            algorithm='RS256',
            headers=header
        )
