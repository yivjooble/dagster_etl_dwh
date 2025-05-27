from utils import *
from messages_and_logs import send_minzdrav_slack_message, get_logger

logger = get_logger(__name__, 'logs/script.log')

with open('config.json', 'r') as f:
    config = json.load(f)

dwh_schema = config['dwh_schema']
campaign_table = config['campaign_table']
country_table = config['country_table']
cluster_labels_list = [int(k) for k in config['clusters_start_percentage'].keys()]
click_percent_bins = [int(v)/100 for v in config['clusters_start_percentage'].values()] + [1]

if __name__ == "__main__":
    try:
        date_start = ((datetime.now().date().replace(day=1) - timedelta(days=1)).replace(day=23)).strftime('%Y-%m-%d')
        date_end = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')

        data = collect_data(date_start, date_end)
        clustered_data = cluster_data(data, click_percent_bins, cluster_labels_list)
        insert_data(clustered_data, dwh_schema, country_table, campaign_table)

        send_minzdrav_slack_message(f':white_check_mark: *cpc_segmentation_script DONE*')
        logger.info('cpc_segmentation_script DONE')
    except Exception:
        send_minzdrav_slack_message(f':x: *cpc_segmentation_script FAILED*')
        logger.exception('cpc_segmentation_script FAILED')
