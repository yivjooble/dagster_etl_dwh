import os
import re
import json
import time

import pandas as pd

from  .pentaho_requests import *

from pathlib import Path
from os.path import join, exists, dirname, basename
from typing import List
from dagster import (
    op, job
)

from .parsers import parse_schedules, parse_job, parse_transformation
from .dwh_utils import write_to_dwh
from ..utils.messages import send_dwh_alert_slack_message



CREDS_DIR = Path(__file__).parent / 'credentials'

with open(f'{CREDS_DIR}/pentaho_cred.json') as json_file:
    pentaho_cred = json.load(json_file)

CREDS = (pentaho_cred['user'], pentaho_cred['password'])


def _get_pentaho_jobs_paths(df) -> List[str]:
    """:returns list of paths to pentaho jobs"""
    pentaho_files_paths = df['input_file'].tolist()
    paths = []
    for file_path in pentaho_files_paths:
        if file_path.startswith('/'):
            file_path = file_path[1:]
        file_path = file_path.replace(' ', '+')
        file_path = file_path.replace(':', '%3A')
        file_path = file_path.replace('(', '%28').replace(')', '%29')
        paths.append(join(os.getcwd(), 'downloaded_content', 'pentaho_repo', file_path))

    assert len(pentaho_files_paths) == len(paths)
    return paths


def _get_pentaho_transformations_paths(context, df):
    """:returns list of paths to pentaho transformation"""
    def _create_path(row):
        t_name = row['trans_name'] if row['trans_name'].endswith('.ktr') else row['trans_name'] + '.ktr'
        return join(os.getcwd(), 'downloaded_content', 'pentaho_repo', row['trans_directory'], t_name)

    path_df = df[['trans_directory', 'trans_name']].copy()

    path_df['trans_directory'] = path_df['trans_directory'].apply(lambda x: x[1:] if x.startswith('/') else x)
    path_df = path_df.applymap(lambda x: x.replace(' ', '+'))
    paths = path_df.apply(lambda row: _create_path(row), axis=1).tolist()
    assert len(paths) == len(df['trans_name'])

    for p in paths:
        if not exists(p):
            paths.remove(p)
            p = p.replace('(', '%28').replace(')', '%29')
            paths.append(p)

    for p in paths:
        if not exists(p):
            paths.remove(p)
            context.log.warning(f'{p} file is not in downloaded repository')

    return paths


def _parse_sql_from_sql_exec_el(df):
    def _parse_tables(sql_):
        tables_ = []

        tables_.extend(re.findall(r'create\stable\s(\w+.\w+)', sql_))
        tables_.extend(re.findall(r'insert\sinto\s(\w+.\w+)', sql_))

        return tables_

    def _parse_procedures(sql_):
        return re.findall(r'call\s(\w+.\w+)', sql_)

    dict_df = df.to_dict(orient='records')

    tables_dfs = []
    procedures_dfs = []
    for row in dict_df:
        parsed_tables = _parse_tables(row['sql_script'])
        if len(parsed_tables) > 0:
            is_truncate = []
            schemas = []
            tables = []
            for t in parsed_tables:
                sch, t_name = t.split('.')
                schemas.append(sch)
                tables.append(t_name)
                if f'truncate {t}' in row['sql_script'] \
                        or f'truncate table {t}' in row['sql_script'] \
                        or f'drop table {t}' in row['sql_script'] \
                        or f'drop table if exists {t}' in row['sql_script']:
                    is_truncate.append(1)
                else:
                    is_truncate.append(0)
            row['target_table'] = tables
            row['target_schema'] = schemas
            row['is_truncate'] = is_truncate
            tables_dfs.append(pd.DataFrame(row))

        parsed_procedures = _parse_procedures(row['sql_script'])
        if len(parsed_procedures) > 0:
            p_schemas = []
            procs = []
            for proc in parsed_procedures:
                sch, p = proc.split('.')
                p_schemas.append(sch)
                procs.append(p)

            row['procedure_schema'] = p_schemas
            row['procedure_name'] = procs
            procedures_dfs.append(pd.DataFrame(row))

    table_df = pd.concat(tables_dfs, ignore_index=True).drop(columns=['sql_script']).drop_duplicates()
    procedure_df = pd.concat(procedures_dfs, ignore_index=True).drop(columns=['sql_script']).drop_duplicates()
    return table_df, procedure_df


def pentaho_metadata_collection(context):
    context.log.info('Starting pentaho repo download')
    # downloads pentaho repository to ./downloaded_content/pentaho_repo/
    download_pentaho_repo(CREDS)
    context.log.info('repo downloaded')

    # get scheduled pentaho jobs and write to dwh
    schedules_data = get_pentaho_schedules(CREDS)
    schedules_df = pd.DataFrame(parse_schedules(schedules_data))
    schedules_df['recurrence'] = schedules_df['recurrence'].str.lower()
    write_to_dwh(
        schedules_df.drop(columns=['input_file']),
        'pentaho_schedules'
    )
    context.log.info('pentaho_schedules is written to dwh')

    # parse scheduled jobs files and write to dwh
    jobs_paths = _get_pentaho_jobs_paths(schedules_df)
    jobs_list = []
    for job_file in jobs_paths:
        jobs_list += parse_job(job_file)
    jobs_df = pd.DataFrame(jobs_list)
    write_to_dwh(
        jobs_df,
        'pentaho_jobs',
    )
    context.log.info('pentaho_jobs is written to dwh')

    # parse transformations and write to dwh

    # get paths to transformations from scheduled jobs
    trans_paths = [
        path for path in _get_pentaho_transformations_paths(context, jobs_df)
        if basename(dirname(path)) not in ['analyze', 'del_data', 'del_old_data', 'update_field']
    ]
    trans_dfs = []
    sql_exec_dfs = []
    for p in trans_paths:
        trans_df, sql_exec_df = parse_transformation(p)
        if trans_df is not None:
            trans_dfs.append(trans_df)
        if sql_exec_df is not None:
            sql_exec_dfs.append(sql_exec_df)

    union_sql_exec_df = pd.concat(sql_exec_dfs, ignore_index=True)

    # parse sql scripts from ExecSQL steps
    parsed_tables_df, parsed_procedures_df = _parse_sql_from_sql_exec_el(union_sql_exec_df)

    trans_dfs.append(parsed_tables_df)
    union_trans_df = pd.concat(trans_dfs, ignore_index=True)

    write_to_dwh(
        union_trans_df.drop(columns=['sql_script']),
        'pentaho_transformations',
    )
    context.log.info('pentaho_transformations is written to dwh')

    write_to_dwh(
        parsed_procedures_df,
        'pentaho_called_procedures',
    )
    context.log.info('pentaho_called_procedures is written to dwh')

    transformation_script_df = pd.concat([
        union_trans_df[
            ['trans_name', 'source_connection_name', 'source_connection_server', 'source_connection_db', 'sql_script']
        ],
        union_sql_exec_df[
            ['trans_name', 'source_connection_name', 'source_connection_server', 'source_connection_db', 'sql_script']
        ]
    ])
    write_to_dwh(
        transformation_script_df,
        'pentaho_transformation_script',
    )
    context.log.info('pentaho_transformation_script is written to dwh')

    send_dwh_alert_slack_message(
        ':white_check_mark: *pentaho-metadata-collection DONE*'
    )



@op
def pentaho_metadata_collection_op(context):
    pentaho_metadata_collection(context)


@job(
    name='api__pentaho_metadata_collection',
    description='get pentaho metadata'
)
def pentaho_metadata_collection_job():
    pentaho_metadata_collection_op()