import pandas as pd
from utility_hub import DwhOperations
from utility_hub.core_tools import fetch_gitlab_data


def unified_check_data_discrepancy(
    context,
    procedure_name: str,
    time_check: float = None,
    time_condition: bool = True
) -> bool:
    """
    Unified function to check data discrepancies across different operations.
    
    Args:
        context: Dagster context
        procedure_name: Name of the procedure to check (e.g., 'session_abtest_agg', 'click_data_agg')
        time_check: Optional time check value
        time_condition: Optional time condition flag
    
    Returns:
        bool: True if check completed or skipped, None if time condition not met
    """
    if time_check is not None and not time_condition:
        context.log.info(f"Time check condition: {time_check}.")
        return None

    check_discrepancy = context.resources.globals["check_discrepancy"]
    if check_discrepancy:
        destination_db = context.resources.globals["destination_db"]
        reload_date_start = context.resources.globals["reload_date_start"]
        reload_date_end = context.resources.globals["reload_date_end"]

        check_discrepancy_procedure_call = f'call dwh_test.check_discrepancy_{procedure_name}(%s);'
        check_discrepancy_gitlab_ddl_q, check_discrepancy_gitlab_ddl_url = fetch_gitlab_data(
            config_key="default",
            manual_object_type="discrepancy_check",
            dir_name="dwh_team",
            file_name=f"check_discrepancy_{procedure_name}",
        )

        context.log.info(f"DDL URL check discrepancy:\n{check_discrepancy_gitlab_ddl_url}")
        date_range = pd.date_range(start=reload_date_start, end=reload_date_end)

        for date in date_range:
            formatted_date = date.strftime('%Y-%m-%d')
            DwhOperations.execute_on_dwh(
                context=context,
                query=check_discrepancy_procedure_call,
                ddl_query=check_discrepancy_gitlab_ddl_q,
                params=(formatted_date,),
                destination_db=destination_db
            )

    return True
