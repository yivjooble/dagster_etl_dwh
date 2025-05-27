from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    DynamicOutput,
    Field,
)

# module import
from utility_hub import (
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from utility_hub.db_configs import repstat_dbs
from ..utils.io_manager_path import get_io_manager_path


JOB_NAME = "insert_email_tables"
SCHEMA = "an"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
CURRENT_DATE = datetime.now().date().strftime('%Y-%m-%d')

PROCEDURES_LIST = [
    "call an.prc_insert_email_sent(%s);",
    "call an.prc_insert_email_account_test(%s);",
    "call an.prc_email_account_test_settings_snapshot();",
    "call an.prc_email_alert_snapshot();"
]


@op(out=DynamicOut(), required_resource_keys={'globals'})
def emails_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    """
    reload_countries = context.resources.globals["reload_countries"]
    launch_countries = {country.strip('_').lower() for country in reload_countries}
    procedures_list = context.resources.globals["procedures_list"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Date: {context.resources.globals["reload_date_start"]}\n'
                     )

    for cluster in repstat_dbs.values():
        cluster_info = {
            'host': cluster['host'],
            'db_type': cluster['db_type'],
            'credential_key': cluster['credential_key']
        }
        for country in cluster['dbs']:
            if country in launch_countries:
                for procedure in procedures_list:
                    procedure_name = procedure.split('(')[0].split('.')[1]
                    gitlab_ddl_q, _ = fetch_gitlab_data(
                        config_key="repstat",
                        dir_name=procedure_name,
                        file_name=procedure_name,
                    )
                    yield DynamicOutput(
                        value={
                            'host': cluster_info['host'],
                            'db_name': country.lower().strip(),
                            'db_type': cluster_info['db_type'],
                            'credential_key': cluster_info['credential_key'],
                            'ddl_query': gitlab_ddl_q,
                            'procedure_call': procedure,
                        },
                        mapping_key=procedure_name + '_' + country
                    )


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def emails_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    date_start = context.resources.globals["reload_date_start"]
    operation_date_diff = get_datediff(date_start)

    if sql_instance_country_query['procedure_call'] in ['call an.prc_insert_email_sent(%s);', 'call an.prc_insert_email_account_test(%s);']:
        sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff
        context.log.info(f"--> Starting sql-script on: {date_start}")
        DbOperations.call_procedure(context, sql_instance_country_query)
    else:
        DbOperations.call_procedure(context, sql_instance_country_query)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   procedures_list=Field(list, default_value=PROCEDURES_LIST),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(JOB_NAME, "_yesterday_date"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "procedures_location": "https://gitlab.jooble.com/an/separate-statistic-db-nl-us/-/tree/master/an/routines",
        "procedures_list": f"{', '.join(pr.split('(')[0].split('.')[1] for pr in PROCEDURES_LIST)}",
        "destination_db": "rpl",
        "target_tables": """[an.email_sent],
                            [an.email_account_test],
                            [an.email_account_test_settings],
                            [an.email_alert]""",
    },
    description="""Full rewrite tables: email_account_test_settings, email_alert.
                   Insert data for date: email_sent, email_account_test"""
)
def insert_emails_job_yesterday_date():
    """
    Start procedures on replica's db.
    Procedures:
        - email_sent
        - email_account_test
        - email_account_test_settings
        - email_alert
    """
    instances = emails_get_sqlinstance()
    instances.map(emails_query_on_db)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_start=Field(str, default_value=CURRENT_DATE),
                                                   procedures_list=Field(list, default_value=PROCEDURES_LIST),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(JOB_NAME, "_current_date"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{CURRENT_DATE}",
        "procedures_location": "https://gitlab.jooble.com/an/separate-statistic-db-nl-us/-/tree/master/an/routines",
        "procedures_list": f"{', '.join(pr.split('(')[0].split('.')[1] for pr in PROCEDURES_LIST)}",
        "destination_db": "rpl",
        "target_tables": """[an.email_sent],
                            [an.email_account_test],
                            [an.email_account_test_settings],
                            [an.email_alert]""",
    },
    description="""Full rewrite tables: email_account_test_settings, email_alert.
                   Insert data for date: email_sent, email_account_test"""
)
def insert_emails_job_current_date():
    """
    Start procedures on replica's db.
    Procedures:
        - email_sent
        - email_account_test
        - email_account_test_settings
        - email_alert
    """
    instances = emails_get_sqlinstance()
    instances.map(emails_query_on_db)
