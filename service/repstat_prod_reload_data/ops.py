from datetime import datetime, timedelta
from typing import List

import pandas as pd
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    Out,
    DynamicOut,
    DynamicOutput
)
from sqlalchemy import text

from .repstat_prod_reload_data_utils.prod_db_connections import prod_conn_sqlalchemy, get_prod_db_host
from .repstat_prod_reload_data_utils.repstat_db_operations import save_to_repstat_db
from ..utils.date_format_settings import get_datediff
# project import
from ..utils.io_manager_path import get_io_manager_path
from ..utils.rplc_config import clusters, all_countries_list
from ..utils.rplc_connections import connect_to_replica
# module import
from ..utils.rplc_job_config import retry_policy, job_config
from ..utils.utils import job_prefix

JOB_NAME = "repstat_prod_reload_data"
JOB_PREFIX = job_prefix()
DESCRIPTION = 'Reload data from prod dbs.'
DATE_COLUMN_NAME = 'date_diff'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def repstat_prod_reload_data_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''

    reload_schema = context.resources.globals["reload_schema"]
    table_prefix = context.resources.globals["table_prefix"]
    launch_countries = context.resources.globals["reload_countries"]

    # iterate over sql instances
    for cluster_info in clusters.values():
        for country in cluster_info['dbs']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(country).lower() in str(launch_country).strip('_').lower():
                    #  'to_sqlcode' > will pass any value to .sql file which starts with it
                    yield DynamicOutput(
                        value={'repstat_sql_instance_host': cluster_info['host'],
                               'country_db': str(country).lower().strip(),
                               'reload_schema': reload_schema,
                               'table_prefix': table_prefix
                               },
                        mapping_key='refresh_from_prd_' + country
                    )


@op(out=Out(dict),
    required_resource_keys={'globals'})
def get_schema_tables(context, sql_instance_country_query: dict):
    if len(context.resources.globals["tables_list"]) > 0:
        tables_list = context.resources.globals["tables_list"]
        table_query_info = {
            'country_db': sql_instance_country_query['country_db'],
            'repstat_sql_instance_host': sql_instance_country_query['repstat_sql_instance_host'],
            'tables': tables_list
        }

        return table_query_info
    else:
        try:
            date_column_name = context.resources.globals["date_column_name"]
            is_like = '%' if context.resources.globals["is_like"] else ''

            with connect_to_replica(sql_instance_country_query['repstat_sql_instance_host'],
                                    sql_instance_country_query['country_db']) as con:
                with con.cursor() as cur:
                    # Replace with the correct SQL query for fetching the table names from the current schema
                    cur.execute(f'''
                                select distinct
                                    case
                                        when table_name ~ '_p[0-9]+$' then substring(table_name from '^(.*)(?=_p[0-9]+)')
                                        else table_name
                                    end as base_table_name
                                from information_schema.columns
                                where table_schema = '{sql_instance_country_query['reload_schema']}'
                                    and table_name ~~ '{sql_instance_country_query['table_prefix']}{is_like}'
                                    and column_name = '{date_column_name}'
                                    and table_name !~~ '%_default';
                                ''')
                    tables = [row[0] for row in cur.fetchall()]

                    table_query_info = {
                        'country_db': sql_instance_country_query['country_db'],
                        'repstat_sql_instance_host': sql_instance_country_query['repstat_sql_instance_host'],
                        'tables': tables
                    }

                    return table_query_info
        except Exception as e:
            context.log.error(f"error for: {sql_instance_country_query['country_db']}\n{e}")
            raise e


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def define_info_to_process_table(context, tables_info: List[dict]):
    try:

        # define dates
        reload_date_diff_start = get_datediff(context.resources.globals["reload_date_start"])
        reload_date_diff_end = get_datediff(context.resources.globals["reload_date_end"])
        reload_schema = context.resources.globals["reload_schema"]

        context.log.info(f"selected tables: {tables_info[0]['tables']}")

        for table_query_info in tables_info:
            prod_db_host = get_prod_db_host(table_query_info['country_db'])

            for table in table_query_info['tables']:
                yield DynamicOutput(
                    value={
                        'table': table,
                        'prod_db_host': prod_db_host,
                        'reload_date_diff_start': reload_date_diff_start,
                        'reload_date_diff_end': reload_date_diff_end,
                        'reload_schema': reload_schema,
                        'repstat_sql_instance_host': table_query_info['repstat_sql_instance_host'],
                        'repstat_country_db': table_query_info['country_db'],
                    },
                    mapping_key='tables_info_' + table_query_info['country_db'] + '_' + table
                )

    except Exception as e:
        context.log.error(f"error\n{e}")
        raise e


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def process_table(context, process_table_info: dict):
    # create prod_db engine
    date_column_name = context.resources.globals["date_column_name"]
    engine = prod_conn_sqlalchemy(process_table_info['prod_db_host'],
                                  'Job_' + str(process_table_info['repstat_country_db']).upper())

    # create prod dataframe of current table
    prod_df = pd.read_sql_query(sql=text(f'''select * from dbo.{process_table_info['table']} 
                                             where {date_column_name} between {process_table_info['reload_date_diff_start']} and {process_table_info['reload_date_diff_end']};'''),
                                con=engine)

    # delete date range from repstat db and save prod_df
    with connect_to_replica(process_table_info['repstat_sql_instance_host'],
                            process_table_info['repstat_country_db']) as con:
        with con.cursor() as cur:
            cur.execute(f'''delete from {process_table_info['reload_schema']}.{process_table_info['table']}
                            where {date_column_name} between '{process_table_info['reload_date_diff_start']}' and '{process_table_info['reload_date_diff_end']}';''')
            con.commit()

            save_to_repstat_db(
                prod_df,
                process_table_info['table'],
                process_table_info['reload_schema'],
                process_table_info['repstat_sql_instance_host'],
                process_table_info['repstat_country_db']
            )

    context.log.info(f"repstat_{process_table_info['repstat_country_db']}\n"
                     f"successfully saved to: {process_table_info['reload_schema']}.{process_table_info['table']}\n"
                     f"dates range: [{context.resources.globals['reload_date_start']}/-/{context.resources.globals['reload_date_end']}]")


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_schema=Field(str, default_value='public'),
                                                    table_prefix=Field(str, default_value=''),
                                                    tables_list=Field(list, default_value=[]),
                                                    reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                    date_column_name=Field(str, default_value=DATE_COLUMN_NAME),
                                                    is_like=Field(bool, default_value=False)
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + JOB_NAME,
     description=DESCRIPTION)
def repstat_prod_reload_data_job():
    # start procedure on replica
    # repstat_prod_reload_data_get_sqlinstance().map(get_schema_tables).map(defire_info_to_process_table).map(process_table)
    collect_tables_info = repstat_prod_reload_data_get_sqlinstance().map(get_schema_tables).collect()
    define_info_to_process_table(collect_tables_info).map(process_table)
