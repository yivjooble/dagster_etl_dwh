import glob
import os
from typing import List

import gitlab
import sqlparse


def delete_pkl_files(context, PATH_TO_DATA):
    files = glob.glob(PATH_TO_DATA + '/*')
    for f in files:
        os.remove(f)
    context.log.info('deleted .pkl files')


def map_country_to_id(map_country_code_to_id, countries: List[str]) -> List[int]:
    countries_id = []
    for country in countries:
        for country_name, country_id in map_country_code_to_id.items():
            if str(country).strip('_').lower() in country_name:
                countries_id.append(country_id)
    return countries_id


def job_prefix():
    return 'service__'


def get_project_id():
    """
    Get project id from gitlab
    """
    return '1140'


def get_file_path(file_name: str):
    """
    Get file path from gitlab
    """
    return f'dwh_team/{file_name}.sql'


def get_gitlab_file_content(project_id: str, file_path: str, ref: str = 'master'):
    api_token = os.environ.get('GITLAB_PRIVATE_TOKEN_RPL')
    try:
        url = 'https://gitlab.jooble.com'
        gl = gitlab.Gitlab(url, private_token=api_token)
        project = gl.projects.get(project_id)
        file_content = project.files.get(file_path=file_path, ref=ref).decode()
        formatted_ddl = sqlparse.format(file_content, reindent=True, keyword_case='upper')
        ddl_url = f'{project.web_url}/-/blob/{ref}/{file_path}'
    except Exception as e:
        raise Exception(f'Error while getting file content from gitlab: {e}')

    return formatted_ddl, ddl_url
