from typing import Optional, List, Dict, Any
from simple_salesforce.api import Salesforce
from .client import SalesforceJWTClient
from .operations import SalesforceOperations


class BaseSalesforceOps:
    """
    Base class for Salesforce operations providing common functionality.

    This class serves as a foundation for all Salesforce operations, providing:
    - Automated authentication handling
    - Lazy client initialization
    - Common data upload methods
    - Standardized error handling

    The class uses JWT authentication and manages Salesforce client lifecycle.
    Inherit from this class to implement specific Salesforce object operations.

    Attributes:
        _client (SalesforceJWTClient): JWT client for authentication
        _sf_client (Salesforce): Lazy-loaded Salesforce client instance

    Example:
        >>> class AccountOperations(BaseSalesforceOps):
        ...     def update_accounts(self, data):
        ...         self.upload_data(
        ...             records=data,
        ...             object_name='Account',
        ...             operation='update'
        ...         )
    """

    def __init__(self, domain: str = None):
        """
        Initialize base Salesforce operations.

        Args:
            domain (str, optional): Salesforce domain. Defaults to None.
                Use test.salesforce.com for sandbox and login.salesforce.com for production.
        """
        self._client = SalesforceJWTClient(domain=domain)
        self._sf_client = None

    @property
    def sf_client(self) -> Salesforce:
        """
        Get authenticated Salesforce client instance.

        Implements lazy initialization of Salesforce client. The client is created
        only when first accessed and then cached for subsequent uses.

        Returns:
            Salesforce: Authenticated Salesforce client instance.

        Note:
            This property ensures efficient resource usage by creating the client
            only when needed.
        """
        if self._sf_client is None:
            self._sf_client = self._client.get_client()
        return self._sf_client

    def upload_data_csv(self,
                        records: List[Dict[str, Any]],
                        object_name: str,
                        operation: str = 'upsert',
                        external_id_field: Optional[str] = None,
                        context=None) -> list:
        """
        Upload data to Salesforce object using Bulk API.
        """
        return SalesforceOperations.upload_to_salesforce_csv(
            records=records,
            sf_client=self.sf_client,
            object_name=object_name,
            operation=operation,
            external_id_field=external_id_field,
            context=context
        )

    def check_upload_errors(self,
                            job_results: list,
                            object_name: str,
                            job_name: str,
                            context=None) -> None:
        """
        Check for errors in upload job results and send notifications if needed.

        Args:
            job_results: Results from bulk upload operation
            object_name: API name of the Salesforce object
            job_name: Name of the dagster job
            context: Dagster execution context for logging
        """
        SalesforceOperations.check_bulk_job_errors(
            job_results=job_results,
            sf_client=self.sf_client,
            object_name=object_name,
            job_name=job_name,
            context=context
        )
