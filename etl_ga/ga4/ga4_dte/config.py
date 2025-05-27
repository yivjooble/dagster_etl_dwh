import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# Set up the credentials
# credentials_path = os.path.joinos.path.pardir(os.path.abspath(__file__)), os.path.join("credentials", "service_key.json"))
creds = Credentials.from_service_account_file('etl_ga/ga4/credentials/service_key.json')

# Set up the Analytics Admin API
analytics = build('analyticsdata', 'v1beta', credentials=creds)


countries_list = [
    ["ua.jooble.org", 1], 
    ["hu.jooble.org", 10],
    ["ro.jooble.org", 11],
]

countries_list_rs = [
    ["rs.jooble.org", 34]
]

country_domain_to_country_id = dict(countries_list)
country_domain_to_country_id_rs = dict(countries_list_rs)

# (domain, property_id)
property_list = [
    ["ua.jooble.org", "properties/375771174"],
    ["hu.jooble.org", "properties/364864984"],
    ["ro.jooble.org", "properties/375781570"],
]

property_list_rs = [
    ["rs.jooble.org", "properties/357446398"]
]

country_domain_to_property_id = dict(property_list)
country_domain_to_property_id_rs = dict(property_list_rs)



# key: Table name in DWH
request_params = {
    'ga4_dte_ea_reg': {
        'dimensions': [
            {'name': 'date'},
            #{'name': 'dateHour'},
            {'name': 'customUser:employerId'},
            {'name': 'sessionDefaultChannelGrouping'},
            {'name': 'sessionMedium'},
            {'name': 'sessionCampaignName'},
        ],
        'metrics': [
            {'name': 'eventCount'}
        ],
        'dimensionFilterClauses': [
            {
                'filters': [
                    {
                        'dimensionName': 'eventName',
                        'operator': 'EXACT',
                        'expressions': ['DTE_Reg'],
                    }
                ]
            }
        ],
    },

    'ga4_dte_ea_sha_reg': {
        'dimensions': [
            #{'name': 'date'},
            {'name': 'dateHour'},
            {'name': 'customUser:employerId'},
            {'name': 'sessionDefaultChannelGrouping'},
            {'name': 'sessionMedium'},
            {'name': 'sessionCampaignName'},
        ],
        'metrics': [
            {'name': 'eventCount'}
        ],
        'dimensionFilterClauses': [
            {
                'filters': [
                    {
                        'dimensionName': 'eventName',
                        'operator': 'EXACT',
                        'expressions': ['EasyShadow_Reg'],
                    }
                ]
            }
        ],
    },

    'ga4_dte_traffic_channel': {
        'dimensions': [
            {'name': 'date'},
            #{'name': 'dateHour'},
            {'name': 'sessionDefaultChannelGrouping'},
            {'name': 'pagePath'},
        ],
        'metrics': [
            {'name': 'totalUsers'},
            {'name': 'newUsers'},
            {'name': 'sessions'},
            # {'name': 'ga:goalCompletionsAll'},
        ]
    },

}


request_params_rs = {
    'ga4_dte_ea_sha_reg_rs': {
        'dimensions': [
            #{'name': 'date'},
            {'name': 'dateHour'},
            {'name': 'customUser:employerId'},
            {'name': 'sessionDefaultChannelGrouping'},
            {'name': 'sessionMedium'},
            {'name': 'sessionCampaignName'},
        ],
        'metrics': [
            {'name': 'eventCount'}
        ],
        'dimensionFilterClauses': [
            {
                'filters': [
                    {
                        'dimensionName': 'eventName',
                        'operator': 'EXACT',
                        'expressions': ['EasyShadow_Reg'],
                    }
                ]
            }
        ],
    },
}