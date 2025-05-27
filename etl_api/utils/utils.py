import os
import glob
from typing import List

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
    return 'api__'
