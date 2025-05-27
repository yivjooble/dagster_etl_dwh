import os
import io
import requests
import zipfile
import shutil

from os.path import join, exists
from typing import Tuple


def download_pentaho_repo(auth: Tuple[str, str]):
    """makes request to Pentaho API to download repository"""
    pentaho_repo_dir = join(os.getcwd(), 'downloaded_content', 'pentaho_repo')
    if exists(pentaho_repo_dir):
        shutil.rmtree(pentaho_repo_dir)

    r = requests.get(
        'http://dwh.jooble.com:8080/pentaho/api/repo/files/home/download',
        auth=auth
    )
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(pentaho_repo_dir)


def get_pentaho_schedules(auth: Tuple[str, str]) -> dict:
    """makes request to Pentaho API to get scheduled jobs as json"""
    response = requests.get(
        'http://dwh.jooble.com:8080/pentaho/api/scheduler/getJobs',
        auth=auth
    )

    return response.json()
