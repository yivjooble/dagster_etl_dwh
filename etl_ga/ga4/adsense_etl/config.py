import json

from pathlib import Path


CREDS_PATH = Path(__file__).parent.parent / 'credentials'


with open(f'{CREDS_PATH}/adsense_id.json') as json_file:
    adsense_id = json.load(json_file)

CLIENT_ID = adsense_id['ADSENSE_CLIENT_ID']

# SERVICE_ACCOUNT_FILE = f'{CREDS_PATH}/adsensedataextraction.json'
CLIENT_SECRETS_PATH = f'{CREDS_PATH}/client_secret_key.json'
SCOPES = "https://www.googleapis.com/auth/adsense.readonly"


# key: Table name in DWH
# values: dimensions and metrics - https://developers.google.com/adsense/management/metrics-dimensions
# filters: optional - https://developers.google.com/adsense/management/reporting/filtering
request_params = {
    # 'adsense_product_revenue_afc': {
    #     'dimensions': [
    #         'DATE',
    #         'COUNTRY_NAME',
    #         'COUNTRY_CODE',
    #         'PRODUCT_NAME',
    #         'PRODUCT_CODE',
    #         'PLATFORM_TYPE_NAME',
    #         'PLATFORM_TYPE_CODE',
    #         'AD_UNIT_NAME',
    #         'AD_UNIT_ID',
    #     ],
    #     'metrics': [
    #         'ESTIMATED_EARNINGS',
    #     ],
    #     'filters': [
    #         'PRODUCT_CODE==AFC',
    #     ],
    # },

    # 'adsense_product_revenue_afs': {
    #     'dimensions': [
    #         'DATE',
    #         'COUNTRY_NAME',
    #         'COUNTRY_CODE',
    #         'PRODUCT_NAME',
    #         'PRODUCT_CODE',
    #         'PLATFORM_TYPE_NAME',
    #         'PLATFORM_TYPE_CODE',
    #     ],
    #     'metrics': [
    #         'ESTIMATED_EARNINGS',
    #     ],
    #     'filters': [
    #         'PRODUCT_CODE==AFS',
    #     ],
    # },

    'adsense_custom_channels_revenue': {
        'dimensions': [
            'DATE',
            'COUNTRY_NAME',
            'COUNTRY_CODE',
            'CUSTOM_CHANNEL_NAME',
            'CUSTOM_CHANNEL_ID',
            'PLATFORM_TYPE_NAME',
            'PLATFORM_TYPE_CODE',
        ],
        'metrics': [
            'ESTIMATED_EARNINGS',
        ],
    },

    'adsense_afc': {
        'dimensions': [
            'DATE',
            'DOMAIN_CODE',
        ],
        'metrics': [
            'PAGE_VIEWS',
            'IMPRESSIONS',
            'CLICKS',
            'PAGE_VIEWS_RPM',
            'IMPRESSIONS_RPM',
            'ACTIVE_VIEW_VIEWABILITY',
            'ESTIMATED_EARNINGS',
        ],
        'filters': [
            'PRODUCT_CODE==AFC',
        ]
    },
    'adsense_afs': {
        'dimensions': [
            'DATE',
            'COUNTRY_NAME',
        ],
        'metrics': [
            'PAGE_VIEWS',
            'IMPRESSIONS',
            'CLICKS',
            'PAGE_VIEWS_RPM',
            'IMPRESSIONS_RPM',
            'ACTIVE_VIEW_VIEWABILITY',
            'ESTIMATED_EARNINGS',
        ],
        'filters': [
            'PRODUCT_CODE==AFS',
        ]
    },
    'adsense_csa': {
        'dimensions': [
            'DATE',
            'CUSTOM_CHANNEL_NAME',
            'COUNTRY_NAME',
        ],
        'metrics': [
            'IMPRESSIONS',
            'CLICKS',
            'IMPRESSIONS_RPM',
            'ACTIVE_VIEW_VIEWABILITY',
            'ESTIMATED_EARNINGS',
        ],
        'filters': [
            'CUSTOM_CHANNEL_NAME=@_fb,CUSTOM_CHANNEL_NAME=@marketing_,CUSTOM_CHANNEL_NAME=@_g',
        ]
    },
    'adsense_adv': {
        'dimensions': [
            'DATE',
        ],
        'metrics': [
            'IMPRESSIONS',
            'CLICKS',
            'IMPRESSIONS_RPM',
            'ACTIVE_VIEW_VIEWABILITY',
            'ESTIMATED_EARNINGS',
        ],
        'filters': [
            'CUSTOM_CHANNEL_NAME==afc_paid,CUSTOM_CHANNEL_NAME==afc_all_direct,CUSTOM_CHANNEL_NAME==afc_all_return',
        ]
    },
}
