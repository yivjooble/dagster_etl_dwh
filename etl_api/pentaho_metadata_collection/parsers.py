import xml.etree.ElementTree as ET
import pandas as pd

from typing import List


def parse_schedules(data: dict) -> List[dict]:
    jobs = []

    for job_schedule in data['job']:
        if job_schedule['jobName'] == 'PentahoSystemVersionCheck':
            continue
        job = {}
        for job_param in job_schedule['jobParams']['jobParams']:
            if job_param['name'] == 'ActionAdapterQuartzJob-StreamProvider-InputFile':
                job['input_file'] = job_param['value']
            if job_param['name'] == 'job':
                job['job_name'] = job_param['value']
            if job_param['name'] == 'directory':
                job['job_directory'] = job_param['value']

        job['cron_string'] = job_schedule['jobTrigger'].get('cronString')
        job['recurrence'] = job_schedule['jobTrigger'].get('uiPassParam')
        job['last_run_datetime'] = job_schedule.get('lastRun')
        job['next_run_datetime'] = job_schedule.get('nextRun')

        jobs.append(job)

    return jobs


def parse_job(path_to_job_file: str) -> List[dict]:
    data = []

    job_tree = ET.parse(path_to_job_file)
    job_root = job_tree.getroot()

    job_data = {
        'job_name': job_root.find('name').text,
        'job_directory': job_root.find('directory').text,
        'job_modified_datetime': job_root.find('modified_date').text,
    }

    disabled_jobs = []
    for hop in job_root.find('hops'):
        if hop.find('enabled').text == 'N':
            disabled_jobs.append(hop.find('to').text)

    job_entries = [el for el in job_root.find('entries') if el.find('name').text not in disabled_jobs]

    for entry in job_entries:
        d = job_data.copy()

        if entry.find('type').text == 'TRANS':
            d['trans_name'] = entry.find('transname').text
            d['trans_directory'] = entry.find('directory').text.replace(
                '${Internal.Entry.Current.Directory}',
                d.get('job_directory')
            )
            data.append(d)
    return data


def parse_transformation(tr_path: str):
    """:returns two dataframes. First with TableInput and TableOutput/InsertUpdate steps. Second with ExecSQL steps"""
    tr_tree = ET.parse(tr_path)
    tr_root = tr_tree.getroot()
    tr_data = {
        'trans_name': tr_root.find('info').find('name').text,
        'trans_directory': tr_root.find('info').find('directory').text,
        'trans_modified_datetime': tr_root.find('info').find('modified_date').text,
    }
    if '_set_var_' in tr_data.get('trans_name') or '_set_date_var_' in tr_data.get('trans_name') or 'set_date_diff' in tr_data.get('trans_name'):
        return (None, None)

    # parse transformation`s connections
    tr_connections = {}
    for conn in tr_root.findall('connection'):
        tr_connections[conn.find('name').text] = {
            'type': conn.find('type').text,
            'server': conn.find('server').text,
            'db': conn.find('database').text
        }

    # filter disabled steps in transformation
    disabled_select_steps = []
    for hop in tr_root.find('order').findall('hop'):
        if hop.find('enabled').text == 'N':
            disabled_select_steps.append(hop.find('from').text)

    # divide steps into 3 groups
    input_steps = []
    output_steps = []
    sql_exec_steps = []
    for step in tr_root.findall('step'):
        if step.find('type').text == 'TableInput' and step not in disabled_select_steps:
            input_steps.append(step)
        elif step.find('type').text in ('TableOutput', 'InsertUpdate'):
            output_steps.append(step)
        elif step.find('type').text == 'ExecSQL':
            sql_exec_steps.append(step)

    input_data = []
    for step in input_steps:
        d = tr_data.copy()
        conn_name = step.find('connection').text
        d['source_connection_name'] = conn_name
        d['source_connection_type'] = tr_connections[conn_name].get('type')
        d['source_connection_server'] = tr_connections[conn_name].get('server')
        d['source_connection_db'] = tr_connections[conn_name].get('db')
        d['sql_script'] = step.find('sql').text
        input_data.append(d)

    output_data = []
    for step in output_steps:
        d = {
            'trans_name': tr_data['trans_name']
        }
        conn_name = step.find('connection').text
        d['target_connection_name'] = conn_name
        d['target_connection_type'] = tr_connections[conn_name].get('type')
        d['target_connection_server'] = tr_connections[conn_name].get('server')
        d['target_connection_db'] = tr_connections[conn_name].get('db')
        if step.find('type').text == 'TableOutput':
            d['target_schema'] = step.find('schema').text
            d['target_table'] = step.find('table').text
            d['is_truncate'] = 1 if step.find('truncate').text == 'Y' else 0
        if step.find('type').text == 'InsertUpdate':
            d['target_schema'] = step.find('lookup').find('schema').text
            d['target_table'] = step.find('lookup').find('table').text
            d['is_truncate'] = 0
        output_data.append(d)

    trans_df = None
    if len(input_steps) != 0 and len(output_steps) == 0:
        trans_df = pd.DataFrame(input_data)
    elif len(input_steps) != 0 and len(output_steps) != 0:
        trans_df = pd.DataFrame(input_data).merge(pd.DataFrame(output_data), on='trans_name', how='outer')

    sql_exec_df = None
    if len(sql_exec_steps) != 0:
        sql_exec_data = []
        for step in sql_exec_steps:
            d = tr_data.copy()
            conn_name = step.find('connection').text
            d['source_connection_name'] = conn_name
            d['source_connection_type'] = tr_connections[conn_name].get('type')
            d['source_connection_server'] = tr_connections[conn_name].get('server')
            d['source_connection_db'] = tr_connections[conn_name].get('db')
            d['sql_script'] = step.find('sql').text
            sql_exec_data.append(d)

        sql_exec_df = pd.DataFrame(sql_exec_data)

    return trans_df, sql_exec_df
