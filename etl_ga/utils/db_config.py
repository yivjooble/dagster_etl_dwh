import json

with open('etl_ga/ga4/credentials/dwh_cred.json') as json_file:
    dwh_cred = json.load(json_file)

USER = dwh_cred['user']
PASSWORD = dwh_cred['password']
HOST = dwh_cred['host']
PORT = dwh_cred['port']
DATABASE = dwh_cred['database']
