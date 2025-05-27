from etl_freshdesk.freshdesk.utils.utils import write_update_log, paginate_api_requests, get_date_string, \
    get_update_time, get_credentials, UnsuccessfulRequest
from etl_freshdesk.freshdesk.utils.database_io import insert_to_db
from etl_freshdesk.freshdesk.utils.parent_tables import get_parent_data

# from etl_freshdesk.freshdesk.utils.database_io import select_to_dataframe, execute_query
# from etl_freshdesk.freshdesk.utils.sql_scripts import q_select_ids, q_delete_ids

from datetime import datetime
import pandas as pd
# import delighted
#
# DELIGHTED_API_KEY = ""


def update_satisfaction_surveys() -> None:
    """Collect data on satisfaction surveys from external freshdesk API and insert them to the database.
    """
    fd_type = 'external'
    domain, api_key, schema, api_pass = get_credentials(fd_type)
    field_list = ['agents', 'groups']
    update_time = get_update_time(schema, 4)
    ut = datetime.utcnow()
    parent_dictionaries = get_parent_data(schema, field_list)

    request_url = 'https://{d}.freshdesk.com//api/v2/surveys/satisfaction_ratings?created_since={t}' \
                  '&per_page=100&page='.format(d=domain, t=update_time)
    status_codes = [0]
    try:
        result, status_codes = paginate_api_requests(request_url, fd_type)
        score_list = [x['ratings']['default_question'] for x in result]
        if len(score_list) > 0:
            df = pd.DataFrame(columns=['id', 'agent_id', 'group_id', 'ticket_id', 'created_at', 'score'])
            df['id'] = [x['id'] for x in result]
            df['agent_id'] = [parent_dictionaries[0].get(x['agent_id']) for x in result]
            df['group_id'] = [parent_dictionaries[1].get(x['group_id']) for x in result]
            df['ticket_id'] = [x['ticket_id'] for x in result]
            df['created_at'] = [get_date_string(x['created_at']) for x in result]
            df['score'] = score_list

            insert_to_db(schema, df, 'satisfaction_surveys')

        write_update_log(schema, ut, max(status_codes), 4, 1)
    except UnsuccessfulRequest as e:
        write_update_log(schema, ut, e.status_code, 4, 0)
    except Exception as e:
        write_update_log(schema, ut, max(status_codes), 4, 0)
        raise e


def process_response(resp: dict) -> dict:
    """Using Delighted API CSAT review record get the necessary fields in the correct format to insert to the database.
    Args:
        resp (dict): dictionary with CSAT review properties obtained from the Delighted API.
    Returns:
        dict: dictionary with necessary CSAT review properties in the correct format to insert it to the database.
    """
    resp_info = {
        'id': int(resp['id']),
        'score': int(resp['score']),
        'comment': resp['comment'],
        'created_at': datetime.fromtimestamp(resp['created_at']),
        'ticket_id': int(resp['person_properties']['ticket_url'].split('/')[-1])
    }
    return resp_info


# def collect_delighted_reviews() -> list:
#     """Collect all CSAT reviews available at the Delighted platform.
#     """
#     delighted.api_key = DELIGHTED_API_KEY
#
#     csat_responses = []
#     for i in range(1, 10 ** 10):
#         filtered_survey_responses = delighted.SurveyResponse.all(
#             per_page=100,
#             page=i
#         )
#         csat_responses += filtered_survey_responses
#         if len(filtered_survey_responses) < 100:
#             break
#     return csat_responses


# def update_csat_responses() -> None:
#     """Collect data on CSAT surveys for internal freshdesk using Delighted API and insert them to the database.
#     The data update will occur only if last update was more than 24 hours ago. Reviews deleted from Delighted platform
#     are to be deleted from the database.
#     """
#     fd_type = 'internal'
#     table_name = 'csat'
#     domain, api_key, schema, api_pass = get_credentials(fd_type)
#     update_time = get_update_time(schema, 4)
#     ut = datetime.utcnow()
#     try:
#         csat_responses = collect_delighted_reviews()
#         api_ids = [int(r['id']) for r in csat_responses]
#         db_ids = select_to_dataframe(q_select_ids.format(schema=schema, table=table_name))['id'].to_list()
#
#         ids_to_del = [x for x in db_ids if x not in api_ids]
#         if len(ids_to_del) > 0:
#             q_del = q_delete_ids.format(schema=schema, table=table_name,
#                                         id_list=', '.join([str(x) for x in ids_to_del]))
#             execute_query(q_del)
#
#         resp_to_add = [r for r in csat_responses if int(r['id']) not in db_ids]
#         response_list = [process_response(resp) for resp in resp_to_add]
#         df = pd.DataFrame.from_records(response_list)
#         insert_to_db(schema, df, table_name)
#         write_update_log(schema, ut, 200, 4, 1)
#     except Exception as e:
#         write_update_log(schema, ut, 200, 4, 0)
#         raise e


def update_surveys(fd_type: str) -> None:
    """Depending on the type of freshdesk system we update data on, call different function to update customer reviews.

    Args:
        fd_type (str): ticket system type (either 'internal' or 'external').
    """
    if fd_type == 'internal':
        pass
        # update_csat_responses()
    else:
        update_satisfaction_surveys()
