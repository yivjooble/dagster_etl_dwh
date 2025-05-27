import pandas as pd

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput,
)
from datetime import datetime, timedelta

# module import
from ..utils.io_manager_path import get_io_manager_path
from ..utils.dwh_db_operations import start_query_on_dwh_db
from ..utils.date_format_settings import get_datediff

JOB_NAME = 'mobile_app_session_data'
SCHEMA = 'mobile_app'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
PROCEDURES_LIST = [
    "call mobile_app.prc_mobile_app_session_revenue(%s);",
    "call mobile_app.prc_mobile_app_session_account(%s);",
    "call mobile_app.ins_mob_session_account_by_placement(%s);"
]


@op(out=DynamicOut(), required_resource_keys={'globals'})
def mobile_app_session_data_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on target db using Dagster multitasking
    Args:
        context (_type_): logs
    Yields:
        dict: dict with params to start query
    """
    prc_list = context.resources.globals["procedures_list"]

    start_date = context.resources.globals["reload_date"]
    start_date_int = get_datediff(start_date)

    context.log.info('Getting SQL instances...\n')

    for prc in prc_list:
        prc_name = prc.split('(')[0].split('.')[1]
        yield DynamicOutput(
            value={'query': prc,
                   'prc_name': prc_name,
                   'to_sqlcode_start_date_int': start_date_int,
                   'to_sqlcode_start_date': start_date,
                   },
            mapping_key='procedure_' + prc_name
        )


@op(required_resource_keys={'globals'})
def mobile_app_session_data_query_on_db(context, sql_instance: dict):
    prc_name = sql_instance.get('prc_name')

    if prc_name == 'ins_mob_session_account_by_placement':
        reload_date = datetime.strptime(context.resources.globals["reload_date"], '%Y-%m-%d')
        reload_date_minus_one_day = reload_date - timedelta(1)

        date_range = pd.date_range(reload_date_minus_one_day, reload_date)

        for date in date_range:
            params = {
                'query': sql_instance['query'],
                'to_sqlcode_start_date_int': get_datediff(date.strftime('%Y-%m-%d')),
            }

            start_query_on_dwh_db(context, params)

    elif prc_name == 'prc_mobile_app_session_account':
        reload_date = datetime.strptime(context.resources.globals["reload_date"], '%Y-%m-%d')
        reload_date_minus_one_day = reload_date - timedelta(1)

        date_range = pd.date_range(reload_date_minus_one_day, reload_date)

        for date in date_range:
            params = {
                'query': sql_instance['query'],
                'to_sqlcode_start_date_int': get_datediff(date.strftime('%Y-%m-%d')),
            }

            start_query_on_dwh_db(context, params)

    elif prc_name == 'prc_mobile_app_session_revenue':
        params = {
            'query': sql_instance['query'],
            'to_sqlcode_start_date_int': sql_instance['to_sqlcode_start_date_int'],
        }

        start_query_on_dwh_db(context, params)


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date=Field(str, default_value=YESTERDAY_DATE),
                                                   procedures_list=Field(list, default_value=PROCEDURES_LIST))},
    name='dwh__' + JOB_NAME,
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "procedures_list": f"{PROCEDURES_LIST}",
        "destination_db": "dwh",
        "target_tables": """[mobile_app.session_account],
                            [mobile_app.session_revenue],
                            [mobile_app.session_account_by_placement]
                            """,
    },
    description=f'{SCHEMA}.{JOB_NAME}'
)
def mobile_app_session_data_job():
    instances = mobile_app_session_data_get_sqlinstance()
    instances.map(mobile_app_session_data_query_on_db)
