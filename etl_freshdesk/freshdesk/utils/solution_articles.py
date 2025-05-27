from etl_freshdesk.freshdesk.utils.database_io import insert_to_db, execute_query
from etl_freshdesk.freshdesk.utils.utils import write_update_log, get_credentials, UnsuccessfulRequest
from etl_freshdesk.freshdesk.utils.sql_scripts import q_delete_ids
import pandas as pd
import requests
from datetime import datetime


def get_article_api_list():
    domain, api_key, schema, api_pass = get_credentials('external')

    categories_url = 'https://{d}.freshdesk.com//api/v2/solutions/categories'.format(d=domain)
    r = requests.get(categories_url, auth=(api_key, api_pass))
    if r.status_code == 200:
        categories_dict = {x['id']: x['name'] for x in r.json()}
    else:
        raise UnsuccessfulRequest(r.status_code)

    folders_dict = {}
    for cat_id in categories_dict.keys():
        folders_url = 'https://{d}.freshdesk.com//api/v2/solutions/categories/{cat_id}/folders'.format(d=domain,
                                                                                                       cat_id=cat_id)
        r = requests.get(folders_url, auth=(api_key, api_pass))
        if r.status_code == 200:
            fold_dict = {x['id']: x['name'] for x in r.json()}
            folders_dict.update(fold_dict)
        else:
            raise UnsuccessfulRequest(r.status_code)

    article_data = []
    for folder_id in folders_dict.keys():
        articles_url = 'https://{d}.freshdesk.com//api/v2/solutions/folders/{folder_id}/articles'.format(
            d=domain, folder_id=folder_id)
        r = requests.get(articles_url, auth=(api_key, api_pass))

        if r.status_code == 200:
            articles = [{
                'id': x['id'],
                'title': x['title'],
                'category_id': x['category_id'],
                'folder_id': x['folder_id'],
                'status': x['status'],
                'is_deleted': False
            } for x in r.json()]
            article_data += articles
        else:
            raise UnsuccessfulRequest(r.status_code)

    df = pd.DataFrame.from_records(article_data)
    df['category_name'] = df['category_id'].map(categories_dict)
    df['folder_name'] = df['folder_id'].map(folders_dict)

    return df[['id', 'title', 'category_id', 'category_name', 'folder_id', 'folder_name', 'status', 'is_deleted']]


def update_solution_articles(fd_type):
    if fd_type == 'external':
        domain, api_key, schema, api_pass = get_credentials('external')
        ut = datetime.utcnow()
        try:
            api_df = get_article_api_list()
            api_ids = api_df['id'].to_list()
            execute_query(q_delete_ids.format(schema=schema, table='solution_articles',
                                              id_list=', '.join([str(x) for x in api_ids])))
            execute_query('update {}.solution_articles set is_deleted=True'.format(schema))
            insert_to_db(schema, api_df, 'solution_articles')
            write_update_log(schema, ut, 200, 5, 1)

        except UnsuccessfulRequest as e:
            write_update_log(schema, ut, e.status_code, 5, 0)
        except Exception as e:
            write_update_log(schema, ut, 200, 5, 0)
            raise e
