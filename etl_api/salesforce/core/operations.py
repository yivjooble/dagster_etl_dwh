import pandas as pd
from typing import Optional, List, Dict, Any
from simple_salesforce.api import Salesforce
from etl_api.utils.messages import send_dwh_alert_slack_message


class SalesforceOperations:
    """
    Provides bulk data operations for Salesforce objects using Bulk API V2.

    Features:
        - Bulk API operations (insert, update, upsert, delete)
        - Detailed operation results and error reporting

    Example:
        >>> records = [{"Id1C__c": "123", "Name": "Test"}]
        >>> results = SalesforceOperations.upload_to_salesforce_csv(
        ...     records=records,
        ...     sf_client=sf_client,
        ...     object_name="Account",
        ...     operation="upsert",
        ...     external_id_field="Id1C__c",
        ...     context=context
        ... )
    """

    @staticmethod
    def upload_to_salesforce_csv(records: List[Dict[str, Any]],
                                 sf_client: Salesforce,
                                 object_name: str,
                                 operation: str = 'upsert',
                                 external_id_field: Optional[str] = None,
                                 context=None) -> list:
        """
        Upload data to Salesforce using CSV format with Bulk API.
        """
        # Convert records to DataFrame
        df = pd.DataFrame.from_records(records)

        if df.empty:
            context.log.warning("No records to upload")
            raise ValueError("Records list is empty")

        # Validate operation
        valid_operations = ['insert', 'update', 'upsert', 'delete']
        if operation not in valid_operations:
            context.log.error(f"Invalid operation: {operation}")
            raise ValueError(f"Invalid operation. Must be one of {valid_operations}")

        if operation == 'upsert':
            if not external_id_field:
                context.log.error("external_id_field is required for upsert operation")
                raise ValueError("external_id_field is required for upsert operation")
            if external_id_field not in df.columns:
                context.log.error(f"External ID field '{external_id_field}' not found in records")
                raise ValueError(f"External ID field '{external_id_field}' not found in records")
            if df[external_id_field].isnull().any():
                context.log.error(f"External ID field '{external_id_field}' contains null values")
                raise ValueError(f"External ID field '{external_id_field}' contains null values")

        context.log.info(f"Total records to process: {len(records)}")

        try:
            # Get bulk handler
            bulk = sf_client.bulk2
            bulk_handler = getattr(bulk, object_name)
            batch_size = 10000

            context.log.info(f"Starting CSV [{operation}] operation on [{object_name}]")

            # Perform operation with CSV data
            if operation == 'upsert':
                job = bulk_handler.upsert(
                    records=records,
                    external_id_field=external_id_field,
                    batch_size=batch_size,
                )
            elif operation == 'insert':
                job = bulk_handler.insert(
                    records=records,
                    batch_size=batch_size,
                )
            elif operation == 'update':
                job = bulk_handler.update(
                    records=records,
                    batch_size=batch_size,
                )
            elif operation == 'delete':
                job = bulk_handler.delete(
                    records=records,
                    batch_size=batch_size,
                )

            context.log.info(f"Operation completed for [{object_name}]")
            context.log.debug(f"Job: {job}")
            return job

        except AttributeError as e:
            context.log.error(f"Invalid object name: {object_name}")
            context.log.error(f"Error details: {str(e)}")
            raise ValueError(f"Invalid object name: {object_name}. Error: {str(e)}")
        except Exception as e:
            context.log.error(f"Error in bulk upload: {str(e)}")
            raise

    @staticmethod
    def check_bulk_job_errors(job_results: List[Dict],
                              sf_client: Salesforce,
                              object_name: str,
                              job_name: str,
                              context=None) -> None:
        """
        Check for errors in bulk job results and send aggregated Slack notifications.

        Args:
            job_results: List of job results from bulk operations
            sf_client: Authenticated Salesforce client instance
            object_name: API name of the Salesforce object
            job_name: Name of the dagster job
            context: Dagster execution context for logging
        """
        try:
            failed_jobs = []
            total_failed = 0
            total_processed = 0

            # Collect statistics from all jobs
            for job in job_results:
                total_processed += job['numberRecordsTotal']
                if job.get('numberRecordsFailed', 0) > 0:
                    total_failed += job['numberRecordsFailed']
                    failed_jobs.append(job)

            if failed_jobs:
                # Prepare error message
                error_msg = (
                    f":salesforce-1:  *Salesforce Errors*\n"
                    f"SF Object: *{object_name}*\n"
                    f"Job: *{job_name}*\n"
                    f"> Total Records Processed: *{total_processed}*\n"
                    f"> Total Failed Records: *{total_failed}*\n"
                    # f"<!subteam^S02ETK2JYLF|dwh.analysts>\n\n"
                    "*Failed Jobs Details:*\n"
                )

                # Add failed Job details
                for job in failed_jobs:
                    job_id = job['job_id']
                    error_msg += (
                        f"> Job ID: {job_id}\n"
                        f"> Failed: {job['numberRecordsFailed']}"
                        f" / {job['numberRecordsTotal']}\n"
                    )

                # Send aggregated Slack alert
                send_dwh_alert_slack_message(error_msg)

                context.log.warning(error_msg)

        except Exception as e:
            context.log.error(f"Error checking job results: {str(e)}")
            raise
