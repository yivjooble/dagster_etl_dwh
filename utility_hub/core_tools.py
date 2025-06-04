import os
import inspect
import gitlab
import sqlparse
import hvac
import tableauserverclient as tsc
import pandas as pd
import pytz
from typing import Optional
from pathlib import PurePosixPath
from datetime import datetime, timedelta
from .data_collections import gitlab_projects_config, job_prefixes
from dotenv import load_dotenv
from tableauserverclient import NotSignedInError
from dagster import Failure, ScheduleDefinition, DefaultScheduleStatus
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError

load_dotenv()


def check_time_condition(time_to_check: float, comparison: str) -> Optional[bool]:
    """
    Args:
        time_to_check (float): Needed time to check. E.g., 11.30 (for 11:30).
        comparison (str): Comparison operator. Options: ">=", "<", "=" or "!=".
    Returns:
        Optional[bool]: True if condition is met, None if not, or raises ValueError for invalid comparison.
    """
    # Define Kyiv timezone
    kyiv_tz = pytz.timezone('Europe/Kyiv')

    current_time_utc = datetime.now(pytz.utc)
    current_time_kyiv = current_time_utc.astimezone(kyiv_tz)  # Convert to Kyiv time

    # Extract hour and minute as a float (e.g., 11:30 -> 11.30)
    current_time = current_time_kyiv.hour + (current_time_kyiv.minute / 100.0)

    # Perform comparison
    if comparison == ">=":
        condition = current_time >= time_to_check
    elif comparison == "<":
        condition = current_time < time_to_check
    elif comparison == "=":
        condition = current_time == time_to_check
    elif comparison == "!=":
        condition = current_time != time_to_check
    else:
        raise ValueError("Invalid comparison operator. Use '>=', '<', '=', or '!='.")

    return True if condition else None


def define_schedule(
    job,
    cron_schedule,
    execution_timezone="Europe/Kiev",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if os.environ.get("INSTANCE") == "PRD"
        else DefaultScheduleStatus.STOPPED
    ),
):
    return ScheduleDefinition(
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone=execution_timezone,
        default_status=default_status,
    )


def submit_external_job_run(job_name_with_prefix: str, repository_location_name: str, instance: str = 'dagster_dwh'):
    """
    Create a GraphQL client with dagster webserver to start an external job.
    Submit job execution over GraphQL API by job name.
    External (different cole location) job name template: code_location__job_name, e.g.: prd__account_agg

    Args:
        job_name_with_prefix (str): external job name.
        repository_location_name (str): external code location name.
        instance (str): dagster instance name.

    Raises DagsterGraphQLClientError:
        InvalidStepError
        InvalidOutputError
        RunConflict
        PipelineConfigurationInvalid
        JobNotFoundError
        PythonError
    """
    try:
        # Default instance is dagster_dwh
        client = DagsterGraphQLClient(hostname=get_creds_from_vault('GRAPHQL_HOSTNAME') if os.getenv('INSTANCE') == 'PRD' else get_creds_from_vault('GRAPHQL_HOSTNAME_DEV'),
                                      port_number=int(get_creds_from_vault('GRAPHQL_PORT' if os.getenv('INSTANCE') == 'PRD' else 'GRAPHQL_PORT_DEV')))

        # If instance is cabacus, then use cabacus client
        if instance == 'cabacus':
            client = DagsterGraphQLClient(hostname=get_creds_from_vault('CABACUS_GRAPHQL_HOSTNAME'),
                                          port_number=int(get_creds_from_vault('CABACUS_GRAPHQL_PORT')))

        client.submit_job_execution(
                job_name=job_name_with_prefix,
                repository_location_name=repository_location_name,
                repository_name='__repository__',
            )
    except DagsterGraphQLClientError as exc:
        raise Failure(f"{exc}")


def run_tableau_object_refresh(context, datasource_id: str = None, workbook_id: str = None):
    """
    Docs: https://tableau.github.io/server-client-python/docs/api-ref.html#data-sources
    Refreshes a Tableau datasource or workbook based on provided IDs within a Dagster pipeline.

    Parameters:
    - context (Dagster context): Provides logging and other functionalities specific to Dagster execution environments.
    - datasource_id (str, optional): The unique identifier for a Tableau datasource.
    - workbook_id (str, optional): The unique identifier for a Tableau workbook.

    Raises:
    - Failure: Raises a Dagster Failure when any part of the operation (authentication, refresh action, etc.) fails. This allows for better error management within Dagster pipelines. Specific scenarios include:
      - ServerResponseError: Raised when the Tableau Server API returns an error in response to a request. This might occur due to issues like non-existent IDs, lack of permissions, or server-side errors affecting the requested operation.
      - NotSignedInError: Raised when there is a failure in establishing a session with the Tableau server, often due to incorrect credentials or network issues.
      - Exception: Covers any unexpected errors during the execution of the function, ensuring that no error type is unhandled, which might include configuration errors, network failures, or coding errors in the function itself.

    The function uses secured credentials fetched from a vault, ensuring sensitive data is handled securely.
    It logs into the Tableau Server, performs the refresh operation, and ensures clean logout and error handling.
    """
    from utility_hub.utils import save_to_db
    try:
        tableau_auth = tsc.TableauAuth(username=get_creds_from_vault('TABLEAU_USERNAME'),
                                       password=get_creds_from_vault('TABLEAU_PASSWORD'),
                                       site_id='')
        server = tsc.Server('https://tableau.jooble.com')
        server.version = '3.1'

        with server.auth.sign_in(tableau_auth):
            context.log.info('Connection made')
            try:
                def refresh_item(item_id, item_type, item_getter, item_refresher, item_name_column):
                    if item_id:
                        item = item_getter(item_id)
                        context.log.info(f"Refreshing {item_type}: {item.name}")
                        refreshed_item = item_refresher(item)
                        job_item_dict = refreshed_item.__dict__
                        df = pd.DataFrame([job_item_dict])
                        df[item_name_column] = item.name

                        # Add workbook name and project name to the dataframe
                        try:
                            if item_type == "datasource":
                                workbook = server.workbooks.get_by_id(refreshed_item.workbook_id)
                                df['_workbook_name'] = workbook.name
                                df['_workbook_project_name'] = workbook.project_name
                        except Exception as e:
                            context.log.warning(f"Failed to get workbook name and project name: {str(e)}")

                        try:
                            save_to_db(df)
                        except Exception as e:
                            context.log.error(f"Failed to save job item to the database: {str(e)}")

                # Refresh datasource and workbook
                refresh_item(datasource_id, "datasource", server.datasources.get_by_id, server.datasources.refresh,
                             "_datasource_name")
                refresh_item(workbook_id, "workbook", server.workbooks.get_by_id, server.workbooks.refresh,
                             "_workbook_name")

                context.log.info('Success')
            except tsc.ServerResponseError as e:
                raise Failure(f"Failed to refresh workbook due to server response error: {str(e)}")
            except Exception as e:
                raise Failure(f"Failed to refresh workbook due to an unexpected error: {str(e)}")
            finally:
                server.auth.sign_out()
                context.log.info('Connection closed')

    except NotSignedInError:
        raise Failure("Failed to sign in to Tableau Server")
    except Exception as exc:
        raise Failure(f"Execution failed: {str(exc)}")


def get_creds_from_vault(key):
    """
    Fetches a secret value from a Vault storage by a specified key.

    This function connects to a Vault server using a pre-configured token
    and retrieves a secret value from the specified path. The secret value
    is returned based on the provided key.

    Uses connection pooling, retry mechanisms, and session caching to improve
    reliability during high traffic periods.

    Args:
        key (str): The key for the desired secret value.

    Returns:
        str: The secret value corresponding to the provided key.

    Raises:
        Exception: If the client fails to authenticate or if the secret
                cannot be retrieved from Vault.
    """
    import time
    import requests
    from cachetools import cached, TTLCache
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Cache configuration
    MAX_CACHE_SIZE = 128
    CACHE_TTL = 86400  # Cache secrets for 24 hours

    # Connection pooling settings
    POOL_CONNECTIONS = 10
    POOL_MAXSIZE = 20

    # Retry configuration
    MAX_RETRIES = 5
    RETRY_BACKOFF_FACTOR = 0.5
    RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]

    @cached(cache=TTLCache(maxsize=MAX_CACHE_SIZE, ttl=CACHE_TTL))
    def get_cached_secret(secret_key, vault_url, vault_token, secret_path):
        """Cache secrets to reduce load on Vault server"""
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_FORCELIST,
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(
            pool_connections=POOL_CONNECTIONS,
            pool_maxsize=POOL_MAXSIZE,
            max_retries=retry_strategy
        )
        with requests.Session() as session:
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # Create HVAC client with custom session
            client = hvac.Client(
                url=vault_url,
                token=vault_token,
                session=session
            )

            # Attempt to fetch the secret with retries
            for attempt in range(MAX_RETRIES):
                try:
                    # Verify authentication - with timeout to prevent hanging
                    if not client.is_authenticated():
                        raise Exception("Client authentication with Vault failed")

                    # Read secret - with timeout to prevent hanging
                    read_response = client.secrets.kv.v1.read_secret(
                        path=secret_path,
                        mount_point='secret'
                    )

                    secret_data = read_response['data']
                    if secret_key not in secret_data:
                        raise Exception(
                            f"Secret key '{secret_key}' not found in Vault at path '{secret_path}'"
                        )

                    return secret_data[secret_key]

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    # Connection or timeout errors should be retried with backoff
                    if attempt < MAX_RETRIES - 1:
                        sleep_time = RETRY_BACKOFF_FACTOR * (2 ** attempt)
                        time.sleep(sleep_time)
                    else:
                        raise Exception(
                            f"Failed to connect to Vault server after {MAX_RETRIES} attempts: {str(e)}"
                        )
                except Exception as e:
                    # For other exceptions, retry only a few times with longer backoff
                    if attempt < 2:  # Only retry non-connection errors twice
                        time.sleep(RETRY_BACKOFF_FACTOR * (2 ** (attempt + 2)))  # Longer backoff
                    else:
                        raise Exception(f"Failed to retrieve secret from Vault: {str(e)}")

    # Vault connection parameters
    vault_url = 'http://10.0.2.155:8205'
    vault_token = os.environ.get('VAULT_TOKEN')
    secret_path = 'dagster'

    try:
        return get_cached_secret(key, vault_url, vault_token, secret_path)
    except Exception as e:
        raise Exception(f"Error accessing Vault: {str(e)}")


def fetch_gitlab_data(
        config_key: str,
        file_name: str,
        dir_name: Optional[str] = None,
        ref: str = 'master',
        manual_object_type: Optional[str] = None,
        root_dir: Optional[str] = None
) -> tuple:
    """
    Fetches the SQL file content from a specified GitLab project and constructs a URL to the file.

    Args:
        config_key (str): The key to select the project configuration from PROJECT_CONFIG.
        file_name (str): The name of the file to fetch.
        dir_name (Optional[str], optional): The directory name where the file is located. Defaults to None.
        ref (Optional[str], optional): The branch or tag name. Defaults to 'master'.
        manual_object_type (Optional[str], optional): The object type to use instead of the one from the project configuration. Defaults to None.
        root_dir (Optional[Union[str, Path]]): Override the core_dir from config.

    Returns:
        tuple: A tuple containing the formatted DDL string and the URL to the file in GitLab.

    Raises:
        ValueError: If the provided config_key is not found in PROJECT_CONFIG.
        Exception: If there is an error while fetching the file content from GitLab.
    """

    # Retrieve project configuration
    config = gitlab_projects_config[config_key]
    project_id = config['project_id']
    api_token = get_creds_from_vault(config['api_token_env'])
    object_type = manual_object_type or config.get('object_type', '')
    core_dir = root_dir or config.get('core_dir', '')

    def build_path(*parts: str) -> str:
        """
        Build a GitLab-compatible path from parts using pathlib.
        GitLab always uses forward slashes, regardless of the platform.

        Args:
            *parts: Variable number of path components.

        Returns:
            str: A properly formatted path string with forward slashes.
        """
        # Filter out empty or None values and convert to strings
        filtered_parts = [str(part).strip() for part in parts if part]

        if not filtered_parts:
            raise ValueError("No valid path components provided")

        # Create path with forward slashes using PurePosixPath
        path = PurePosixPath(*filtered_parts)

        # Ensure .sql extension
        if path.suffix != '.sql':
            path = path.with_suffix('.sql')

        return str(path)

    # Construct the file path based on the project configuration
    match config_key:
        case 'default':
            file_path = build_path(dir_name, object_type, file_name)
        case 'repstat':
            if root_dir:
                file_path = build_path(core_dir, file_name)
            else:
                file_path = build_path(core_dir, object_type, dir_name, file_name)
        case 'clickhouse':
            file_path = build_path(core_dir, object_type, dir_name, file_name)
        case _:
            raise ValueError(f'Unknown config key: {config_key}')

    try:
        url = 'https://gitlab.jooble.com'
        gl = gitlab.Gitlab(url, private_token=api_token, retry_transient_errors=True)
        project = gl.projects.get(project_id)
        file_content = project.files.get(file_path=file_path, ref=ref).decode()
        formatted_ddl = sqlparse.format(file_content, reindent=True, keyword_case='upper')
        ddl_url = f'{project.web_url}/-/blob/{ref}/{file_path}'
    except Exception as e:
        raise Exception(
            f'Error while getting file content from GitLab: {e}\n'
            f'Project ID: {project_id}\n'
            f'File Path: {file_path}\n'
            f'Ref: {ref}'
        )

    return formatted_ddl, ddl_url


def get_datediff(date: str) -> int:
    current_date = datetime.strptime(f"{date}", "%Y-%m-%d").date()
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (current_date - reference_date).days


def date_diff_now() -> int:
    current_date = datetime.now().date()
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (current_date - reference_date).days


def date_diff_to_date(date_diff: int) -> str:
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (reference_date + timedelta(days=int(date_diff))).strftime("%Y-%m-%d")


def get_first_day_of_month_as_int() -> int:
    # Get the current date
    today = datetime.now()

    # Extract the year and month, set the day to 1
    first_day_of_month = datetime(today.year, today.month, 1)

    # Calculate the difference from the base date (1900-01-01)
    base_date = datetime(1900, 1, 1)
    diff = (first_day_of_month - base_date).days

    return diff


def get_previous_month_dates() -> tuple:
    now = datetime.now()
    current_month_start = now.replace(day=1)
    previous_month_end = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_end.replace(day=1)

    return previous_month_start.strftime('%Y-%m-%d'), previous_month_end.strftime('%Y-%m-%d')


def generate_job_name(table_name, additional_suffix: Optional[str] = None, additional_prefix: Optional[str] = None):
    """
    Generates a job name based on the module and table name.
    Args:
        table_name (str): The name of the table.
        additional_suffix (Optional[str]): Additional suffix to append to the job name.
        additional_prefix (Optional[str]): Additional prefix to prepend to the job name.
    Returns:
        str: The generated job name.
    """
    # Determine the module from which the function is called
    caller_frame = inspect.stack()[1]
    caller_module = inspect.getmodule(caller_frame[0])
    module_name = caller_module.__name__.split('.')[0]

    # Get the prefix for the current module
    if module_name in job_prefixes:
        job_name = job_prefixes[module_name]

        # Add additional prefix if provided
        if additional_prefix:
            job_name += additional_prefix

        # Add table name
        job_name += table_name

        # Add additional suffix if provided
        if additional_suffix:
            job_name += additional_suffix

        return job_name
    else:
        raise ValueError(f"No prefix found for module: {module_name}. "
                         f"Please add a prefix to the module in data_collections.py -> job_prefixes.")
