tableau_object_uid = {
    "workbook_by_name": {
        "": "",
    },
    "datasource_by_name": {
        "aggregation_paid_acquisition_prod_metrics": "6fff8661-1ee7-4b9b-bd35-5451375bcedf",
        "mobile_app_push_metrics": "fee036b8-9187-4f7d-8a3c-76169dcf2816",
        "mobile_app_v_aggregated_mobile_metrics": "dda56baf-1323-4ce3-8aa8-aebdd5ffe041",
        "aggregation_v_de_goal_2023": "57d68d27-06cb-44af-917c-da572ddc98ea",
        "aggregation_v_account_revenue": "d4e3df0f-d8d8-4244-8e7d-a3696043ff3c",
        "aggregation_v_click_data_agg": "81fd1b77-c1d0-48ea-be1e-38ab39690547",
        "aggregation_v_budget_and_revenue": "9d8abf05-9b95-4120-b6a7-ff6b43b24367"
    }
}


db_credential_keys = {
    "dwh": {
        "user": "DWH_USER",
        "password": "DWH_PASSWORD"
    },
    "prod": {
        "user": "DWH_USER",
        "password": "DWH_PASSWORD"
    },
    "dagster": {
        "user": "DAGSTER_STORAGE_USER",
        "password": "DAGSTER_STORAGE_PASSWORD"
    },
    "repstat": {
        "user": "REPLICA_USER",
        "password": "REPLICA_PASSWORD"
    },
    "citus": {
        "user": "CITUS_USER",
        "password": "CITUS_PASSWORD"
    },
    "cloudberry": {
        "user": "CLOUDBERRY_USER",
        "password": "CLOUDBERRY_PASSWORD"
    },
    "new_pg": {
        "user": "REPLICA_USER",
        "password": "REPLICA_PASSWORD"
    },
    "outreach": {
        "user": "LTF_DB_USERNAME",
        "password": "LTF_DB_PASS"
    },
    "jobexporter": {
        "user": "JOB_EXPORTER_USER",
        "password": "JOB_EXPORTER_PASSWORD"
    },
    "affiliate": {
        "user": "AFF_USER",
        "password": "AFF_PASSWORD"
    },
    "conversion": {
        "user": "CONVERSION_USER",
        "password": "CONVERSION_PASSWORD"
    },
    "jobxx": {
        "user": "JOBXX_USER",
        "password": "JOBXX_PASSWORD"
    },
    "internal": {
        "user": "INTERNAL_USER",
        "password": "INTERNAL_PASSWORD"
    },
    "soska": {
        "user": "S_USER",
        "password": "S_PASSWORD"
    },
    "seo_server": {
        "user": "GBC_USER",
        "password": "GBC_PASSWORD"
    },
    "clickhouse": {
        "user": "CLICKHOUSE_USER",
        "password": "CLICKHOUSE_PASSWORD"
    },
    "history": {
        "user": "HISTORY_USER",
        "password": "HISTORY_PASSWORD"
    },
    "1c": {
        "user": "1C_USER",
        "password": "1C_PASSWORD"
    },
    "dbt": {
        "user": "DBT_USER",
        "password": "DBT_PASSWORD"
    }
}


gitlab_projects_config = {
    'default': {
        'project_id': '864',
        'api_token_env': 'GITLAB_PRIVATE_TOKEN_PROD',
        'object_type': 'tables',
        'core_dir': '',
    },
    'repstat': {
        'project_id': '1140',
        'api_token_env': 'GITLAB_PRIVATE_TOKEN_RPL',
        'object_type': 'routines',
        'core_dir': 'an',
    },
    'clickhouse': {
        'project_id': '1387',
        'api_token_env': 'GITLAB_PRIVATE_TOKEN_CH',
        'object_type': 'tables',
        'core_dir': 'an',
    },
}


job_prefixes = {
    'etl_production': 'prd__',
    'etl_repstat': 'rpl__',
    'etl_jooble_internal': 'internal__',
    'etl_api': 'api__',
    'etl_dwh': 'dwh__',
    'etl_affiliate': 'aff__',
    'etl_freshdesk': 'freshdesk__',
    'etl_clickhouse': 'ch__',
    'service': 'service__',
    'etl_dwh_sublocation': 'dwh__',
    'etl_ea_dte': 'ea_dte__',
    # 'etl_ga': 'ga__',
    # 'dbt_core': 'dbt__',
}


abroad_regions = {
    'de': [82444, 82445, 82446, 82447, 82448, 82449, 82450, 82453, 82454, 82455, 82456, 82457, 82458, 82459, 82460,
           82462, 82463, 84454, 84455, 84456, 84457, 84458, 84459, 84460, 85454, 85455, 85457, 85458, 85459, 85460,
           85461, 85462, 85463, 85464, 85831, 85832, 85833, 85834, 85835, 85836, 85837, 85838, 85839, 85840, 85841,
           85842, 85843, 85844, 85845, 85846, 85847, 85848, 85849, 85850, 85851, 85852, 85853, 85854, 85855],

    'fr': [43108, 43109, 43110, 43111, 43112, 43113, 44109, 44110, 44111, 44112, 44113, 44114, 44115, 44116, 45121,
           45122, 45123, 45124, 45126, 45127, 45128, 45211, 45214, 45215, 45216, 45217, 45218, 45219, 45220, 45221,
           45222, 45223, 45224, 45225, 45226, 45227, 45228, 45229, 45230, 45231, 45232, 45233, 45234, 45235, 45236,
           45237, 45238, 45239, 45240, 45241, 45242, 45244, 45245, 45246, 45247, 45248, 45249, 45250, 45251, 45252,
           45253, 45254, 45255],

    'at': [19771, 19772, 19775, 19777, 19805, 19808, 19809, 19824, 19825, 19826, 19831, 19833, 19835, 19850, 19851,
           19852, 19853, 19854, 19855, 19856, 19857, 19858, 19859, 19860, 19861, 19862, 19863, 19864, 19865, 19866,
           19867, 19868, 19870, 19871, 19872, 19873, 19874, 19875, 19876, 19877, 19879, 19881, 19883],

    'uk': [40904, 40864, 40875, 40876, 40877, 40878, 40879, 40880, 40881, 40882, 40883, 40884, 40885, 40886, 40887,
           40888, 40889, 40890, 40891, 40892, 40893, 40894, 40895, 40896, 40897, 40898, 40899, 40900, 40901, 40902,
           40903, 40905, 40906, 40907, 41477, 41478, 41479, 41480, 41553, 41554, 41555, 41556, 41566, 41567, 41616,
           41620, 41627, 41628, 41629, 41630, 41631, 41632, 41633, 41634, 41635, 41636, 41637],

    'be': [3413, 3414, 4418, 4420, 4452, 4453, 4454, 4455, 4456, 4457, 4458, 4459, 4460, 4461, 4462, 4463, 4464, 4465,
           4466, 4467, 4468, 4469, 4470, 4471, 4472, 4473, 4474, 4475, 4476, 4477, 4478, 4479, 4480, 4481, 4483, 4484],

    'ca': [7531, 8531, 8534, 8535, 8536, 8537, 8538, 8539, 8540, 8541, 8542, 8543, 8544, 8545, 8547, 8548, 8549, 8550,
           8551, 8552, 8553, 8554, 8646, 8647, 8648, 8649, 8650, 8651, 8652, 8653, 8654, 8655, 8656, 8657, 8658, 8659,
           8660, 8661, 8662, 8663, 8664, 8665, 8671],

    'ch': [9943, 9946, 9949, 9952, 9955, 9958, 9961, 9968, 9969, 9970, 9971, 9972, 9973, 10079, 10081, 10082, 10083,
           10084, 10085, 10086, 10087, 10088, 10089, 10090, 10091, 10092, 10093, 10094, 10095, 10096, 10097, 10098,
           10099, 10100, 10101, 10102, 10103, 10104, 10105, 10106, 10107],

    'hu': [4357, 4384, 4385, 4386, 4387, 4388, 4389, 4390, 4391, 4392, 4393, 4394, 4400, 4401, 4402, 4404, 4405, 4406,
           4409, 4411, 4397, 4429, 4430, 4431, 4432, 4398, 4399, 4433, 4434, 4435, 4437],

    'nl': [2912, 2913, 2914, 2916, 2918, 2920, 2921, 2937, 2938, 2941, 3112, 2980, 2981, 3113, 3089, 3090, 3091, 3092,
           3094, 3095, 3096, 3097, 3098, 3099, 3100, 3101, 3102, 3103, 3104, 3105, 3106, 3107, 3108, 3109, 3110, 3111,
           3114, 3115, 3116, 3123, 3126, 3128, 3138, 3139, 3140, 3141, 3142, 3143, 3145],

    'pl': [53253, 53426, 53425, 53254, 53255, 53256, 53276, 53277, 53278, 53279, 53280, 53281, 53282, 53283, 53284,
           53285, 53286, 53287, 53288, 53289, 53290, 53291, 53292, 53293, 53294, 53295, 53296, 53297, 53298, 53299,
           53300, 53559, 53561, 53562, 53569, 53570, 53571, 53572, 53573, 53574, 53575, 53576, 53577, 53578, 53579,
           53580, 53581, 53585],

    'ro': [17109, 17065, 17066, 17067, 17068, 17069, 17070, 17071, 17072, 17073, 17074, 17075, 17076, 17077, 17078,
           17079, 17080, 17082, 17083, 17084, 17085, 17086, 17087, 17088, 17089, 17090, 17091, 17092, 17093, 17094,
           17095, 17096, 17097, 17099, 17100, 17101, 17102, 17103, 17104, 17105, 17106, 17107, 17108, 17110, 17111,
           17112, 17113, 17114, 17115, 17116, 17118, 17119, 17122],

    'rs': [4654, 4655, 4656, 4657, 4658, 4659, 4660, 4661, 4666, 4667, 4671, 4672, 4673, 4696, 4711, 4716, 4717, 4718,
           4719],

    'us': [55124, 55125, 55126, 55127, 55128, 55129, 55130, 55131, 55132, 55133, 55134, 55135, 55136, 55137, 55138,
           55146, 55147, 55148, 55149, 55150, 55151, 55152, 55153, 55154, 55158, 55160, 55161, 55162, 55163, 55164,
           55165, 55166, 55167, 55168, 55169, 55170, 55171, 55172, 55173, 55174, 55175, 55176, 55177, 55178, 55179,
           55180, 55181, 55182, 55183, 55184, 55185, 55186, 55187, 55188]
}


map_country_code_to_id = {
    'ua': 1,
    'de': 2,
    'uk': 3,
    'fr': 4,
    'ca': 5,
    'us': 6,
    'id': 7,
    'pl': 9,
    'hu': 10,
    'ro': 11,
    'es': 12,
    'at': 13,
    'be': 14,
    'br': 15,
    'ch': 16,
    'cz': 17,
    'in': 18,
    'it': 19,
    'nl': 20,
    'tr': 21,
    'cl': 23,
    'co': 24,
    'gr': 25,
    'sk': 26,
    'th': 27,
    'tw': 28,
    'bg': 30,
    'hr': 31,
    'kz': 32,
    'no': 33,
    'rs': 34,
    'se': 35,
    'nz': 36,
    'ng': 37,
    'ar': 38,
    'mx': 39,
    'pe': 40,
    'cn': 41,
    'hk': 42,
    'kr': 43,
    'ph': 44,
    'pk': 45,
    'jp': 46,
    'pr': 48,
    'sv': 49,
    'cr': 50,
    'au': 51,
    'do': 52,
    'uy': 53,
    'ec': 54,
    'sg': 55,
    'az': 56,
    'fi': 57,
    'ba': 58,
    'pt': 59,
    'dk': 60,
    'ie': 61,
    'my': 62,
    'za': 63,
    'ae': 64,
    'qa': 65,
    'sa': 66,
    'kw': 67,
    'bh': 68,
    'eg': 69,
    'ma': 70,
    'uz': 71,
}


country_id_to_code = {
    1: "ua",
    2: "de",
    3: "uk",
    4: "fr",
    5: "ca",
    6: "us",
    7: "id",
    9: "pl",
    10: "hu",
    11: "ro",
    12: "es",
    13: "at",
    14: "be",
    15: "br",
    16: "ch",
    17: "cz",
    18: "in",
    19: "it",
    20: "nl",
    21: "tr",
    23: "cl",
    24: "co",
    25: "gr",
    26: "sk",
    27: "th",
    28: "tw",
    30: "bg",
    31: "hr",
    32: "kz",
    33: "no",
    34: "rs",
    35: "se",
    36: "nz",
    37: "ng",
    38: "ar",
    39: "mx",
    40: "pe",
    41: "cn",
    42: "hk",
    43: "kr",
    44: "ph",
    45: "pk",
    46: "jp",
    48: "pr",
    49: "sv",
    50: "cr",
    51: "au",
    52: "do",
    53: "uy",
    54: "ec",
    55: "sg",
    56: "az",
    57: "fi",
    58: "ba",
    59: "pt",
    60: "dk",
    61: "ie",
    62: "my",
    63: "za",
    64: "ae",
    65: "qa",
    66: "sa",
    67: "kw",
    68: "bh",
    69: "eg",
    70: "ma",
    71: "uz"
}


all_countries_list = [
    'ua', 'de', 'uk', 'fr', 'ca', 'us', 'id', 'pl', 'hu', 'ro', 'es', 'at', 'be', 'br', 'ch', 'cz', 'in', 'it',
    'nl', 'tr', 'cl', 'co', 'gr', 'sk', 'th', 'tw', 'bg', 'hr', 'kz', 'no_', 'rs', 'se', 'nz', 'ng', 'ar', 'mx',
    'pe', 'cn', 'hk', 'kr', 'ph', 'pk', 'jp', 'pr', 'sv', 'cr', 'au', 'do', 'uy', 'ec', 'sg', 'az', 'fi', 'ba',
    'pt', 'dk', 'ie', 'my', 'za', 'ae', 'qa', 'sa', 'kw', 'bh', 'eg', 'ma', 'uz'
]


# Catalog mapping for Trino
trino_catalog_map = {
    # NL cluster
    "de": "prod_de",
    "fr": "prod_fr",
    "pl": "prod_pl",
    "at": "prod_at",
    "bh": "prod_bh",
    "ba": "prod_ba",
    "nl": "prod_nl",
    "pt": "prod_pt",
    "es": "prod_es",
    "qa": "prod_qa",
    "it": "prod_it",
    "in": "prod_in",
    "hr": "prod_hr",
    "az": "prod_az",
    "kw": "prod_kw",
    "eg": "prod_eg",
    "ma": "prod_ma",
    "ie": "prod_ie",
    "kz": "prod_kz",
    "ng": "prod_ng",
    "pk": "prod_pk",
    "ro": "prod_ro",
    "rs": "prod_rs",
    "uk": "prod_uk",
    "uz": "prod_uz",
    "ae": "prod_ae",
    "sa": "prod_sa",
    "ch": "prod_ch",
    "ua": "prod_ua",
    "fi": "prod_fi",
    "bg": "prod_bg",
    "tr": "prod_tr",
    "be": "prod_be",
    "no": "prod_no",
    "dk": "prod_dk",
    "sk": "prod_sk",
    "cz": "prod_cz",
    "gr": "prod_gr",
    "hu": "prod_hu",
    "se": "prod_se",    
    # US cluster
    "ph": "prod_ph",
    "hk": "prod_hk",
    "kr": "prod_kr",
    "cn": "prod_cn",
    "co": "prod_co",
    "za": "prod_za",
    "ar": "prod_ar",
    "au": "prod_au",
    "br": "prod_br",
    "cr": "prod_cr",
    "my": "prod_my",
    "tw": "prod_tw",
    "ca": "prod_ca",
    "th": "prod_th",
    "sv": "prod_sv",
    "pr": "prod_pr",
    "jp": "prod_jp",
    "pe": "prod_pe",
    "ec": "prod_ec",
    "sg": "prod_sg",
    "us": "prod_us",
    "cl": "prod_cl",
    "do": "prod_do",
    "uy": "prod_uy",
    "nz": "prod_nz",
    "mx": "prod_mx",
    "id": "prod_id"
}

trino_rpl_catalog = [
    "rpl_ae",
    "rpl_ar",
    "rpl_at",
    "rpl_au",
    "rpl_az",
    "rpl_ba",
    "rpl_be",
    "rpl_bg",
    "rpl_bh",
    "rpl_br",
    "rpl_ca",
    "rpl_ch",
    "rpl_cl",
    "rpl_cn",
    "rpl_co",
    "rpl_cr",
    "rpl_cz",
    "rpl_de",
    "rpl_dk",
    "rpl_do",
    "rpl_ec",
    "rpl_eg",
    "rpl_es",
    "rpl_fi",
    "rpl_fr",
    "rpl_gr",
    "rpl_hk",
    "rpl_hr",
    "rpl_hu",
    "rpl_id",
    "rpl_ie",
    "rpl_in",
    "rpl_it",
    "rpl_jp",
    "rpl_kr",
    "rpl_kw",
    "rpl_kz",
    "rpl_ma",
    "rpl_mx",
    "rpl_my",
    "rpl_ng",
    "rpl_nl",
    "rpl_no",
    "rpl_nz",
    "rpl_pe",
    "rpl_ph",
    "rpl_pk",
    "rpl_pl",
    "rpl_pr",
    "rpl_pt",
    "rpl_qa",
    "rpl_ro",
    "rpl_rs",
    "rpl_sa",
    "rpl_se",
    "rpl_sg",
    "rpl_sk",
    "rpl_sv",
    "rpl_th",
    "rpl_tr",
    "rpl_tw",
    "rpl_ua",
    "rpl_uk",
    "rpl_us",
    "rpl_uy",
    "rpl_uz",
    "rpl_za",
]