import json
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
import requests
import time
import os
from authlib.jose import jwt, JsonWebToken
from itunes_app_scraper.scraper import AppStoreScraper
from itunes_app_scraper.util import AppStoreException
from etl_freshdesk.reviews.utils.utils import list_to_db_array
from etl_freshdesk.reviews.utils.config import APP_STORE_COUNTRY_MAP, APP_STORE_MARKETS

creds_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "credentials")
with open(os.path.join(creds_dir, 'app_store_config.json'), 'r') as f:
    config = json.load(f)


def get_appstore_token() -> JsonWebToken:
    """Using appstore key file, generate AppStoreConnect API JSON Web Token string to send requests.

    Returns:
        JsonWebToken: AppStoreConnect API JSON Web Token string.
    """
    key_id = config['key_id']
    expiration_time = int(round(time.time() + (20.0 * 60.0)))
    with open(os.path.join(creds_dir, config['key_filename']), 'r') as key_file:
        private_key = key_file.read()

    header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    payload = {"sub": "user", "exp": expiration_time, "aud": "appstoreconnect-v1"}
    app_store_token = jwt.encode(header, payload, private_key)
    return app_store_token


def collect_apple_review_response(headers: dict, url: str) -> str:
    """Using given headers and URL collect review reply date for the given review (specified in URL).

    Args:
        headers (dict): dictionary with API request headers.
        url (str): URL to use in the API request.

    Returns:
        str: reply date ('lastModifiedDate') for the given review.
    """
    r = requests.get(url, headers=headers)
    try:
        return r.json()['data']['attributes']['lastModifiedDate']
    except TypeError:
        pass
    except KeyError:
        pass


def localize_datetime(dt_str: str) -> str:
    """Convert datetime string with time zone to datetime, convert to local time zone and convert back to string.

    Args:
        dt_str (str): datetime string with specified timezone.

    Returns:
        str: datetime string without time zone (converted to Kyiv time).
    """
    if dt_str:
        localized_dt = pd.to_datetime(dt_str).tz_convert('Europe/Kiev')
        return localized_dt.strftime('%Y-%m-%d %H:%M:%S')


def convert_country_code(country_3_code: str) -> str:
    """Convert 3-letter country code to 2-letter country code using country_map_dict. If 3-letter country code is absent
    in dict, return it as output.

    Args:
        country_3_code (str): 3-letter country code.

    Returns:
        str: 2-letter country code or 3-letter country code (if 3-letter country code is absent in country_map_dict).
    """
    if country_3_code in APP_STORE_COUNTRY_MAP:
        country_2_code = APP_STORE_COUNTRY_MAP.get(country_3_code)
        return country_2_code
    return country_3_code


def collect_apple_reviews(context, app_store_token: JsonWebToken) -> pd.DataFrame:
    """Collect info on App Store reviews using App Store Connect API.
    Returns dataframe with review info: 2 as platform type, review_id, country_code, score, review text,
    date published and date replied.

    Args:
        app_store_token (JsonWebToken): AppStoreConnect API JSON Web Token string.

    Returns:
        pd.DataFrame: dataframe with all collected reviews for all countries available in the App Store.
    """
    context.log.debug('Collecting App Store reviews')
    platform_type = 2
    headers = {'Authorization': 'Bearer ' + app_store_token.decode()}
    url_reviews = 'https://api.appstoreconnect.apple.com/v1/apps/1605813568/customerReviews'
    r = requests.get(url_reviews, params={'limit': 200, 'sort': '-createdDate'}, headers=headers)
    try:
        response_json = r.json()
        reviews_json = response_json['data']

        while 'next' in response_json['links']:
            url_reviews = response_json['links']['next']
            r = requests.get(url_reviews, headers=headers)
            response_json = r.json()
            reviews_json += response_json['data']

        reviews_info = [
            {
                'platform_type': platform_type,
                'review_id': x['id'],
                'language_code': None,
                'country_code': convert_country_code(x['attributes']['territory']),
                'score': x['attributes']['rating'],
                'review_text': x['attributes']['title'] + '. ' + x['attributes']['body'],
                'review_translation': None,
                'published_date': localize_datetime(x['attributes']['createdDate']),
                'response_date': localize_datetime(
                    collect_apple_review_response(headers, x['relationships']['response']['links']['related'])),
                'is_deleted': False,
                'ticket_id': None
            } for x in reviews_json
        ]
        return pd.DataFrame.from_records(reviews_info)
    except KeyError as exc:
        context.log.error(f'App Store reviews collection failed: {exc}\n'
                          f'App Store response: {r.text}')


def parse_rating_histogram(s: str) -> list:
    """Parses rating histogram into a list with 5 numbers corresponding to number of ratings by each score.

    Args:
        s (str): string with score histogram listed as '1 star: X1, 2 star: X2, ...'.

    Returns:
        list: list with integer values of rating count for each score (from 1 to 5).
    """
    rating_list = s.split(',')
    return [int(x.split(' star: ')[1]) for x in rating_list]


def collect_apple_country_stat(country: str) -> dict:
    """Collect App Store summary statistics (score, ratings count, score histogram) for the given country.

    Args:
        country (str): country code to collect data for.

    Returns:
        dict: dictionary with data from App Store. Keys:
            country: country, which data if collected for.
            score: average score displayed in the App Store.
            rating_cnt: total number of ratings for the app.
            score_histogram: ratings breakdown by the type of ratings (stars count).
    """
    try:
        scraper = AppStoreScraper()
        app_details = scraper.get_app_details(1605813568, country=country.lower(), add_ratings=True)
        return {
            'country': country,
            'score': round(app_details['averageUserRating'], 2),
            'rating_cnt': app_details['userRatingCount'],
            'score_histogram': parse_rating_histogram(app_details['user_ratings'])
        }
    except AppStoreException:
        pass


def collect_apple_summary_stats(reviews_cnt: int) -> dict:
    """Collect App Store summary statistics (score, ratings count, score histogram) for all countries.

    Args:
        reviews_cnt (int): total number of reviews for the app (all countries).

    Returns:
        dict: dictionary with data from App Store. Keys:
            install_cnt: None (unavailable in App Store, just for consistency with Google Play results).
            score: average total score, calculated is weighted average using country score and ratings count.
            rating_cnt: total number of ratings for the app (all countries).
            review_cnt: total number of reviews for the app (all countries).
            score_histogram: ratings breakdown by the type of ratings (stars count) for all countries.
            country_details: dictionary with country codes as keys and dictionary with score and rating_cnt keys as
                value.
    """
    platform_type = 2
    with ThreadPoolExecutor(8) as executor:
        results_iter = executor.map(collect_apple_country_stat, list(APP_STORE_MARKETS.keys()))

    result = list(results_iter)
    country_stat = [x for x in result if x and x['score'] > 0]

    total_histogram = list(np.sum(np.array([x['score_histogram'] for x in country_stat]), 0))
    total_ratings = sum([x['rating_cnt'] for x in country_stat])
    total_score = round(sum([x['rating_cnt'] * x['score'] for x in country_stat]) / total_ratings, 2)

    summary_statistics = {
        'install_cnt': None,
        'score': total_score,
        'rating_cnt': total_ratings,
        'review_cnt': reviews_cnt,
        'score_histogram': list_to_db_array(total_histogram),
        'country_details': json.dumps({x['country'].lower(): {"score": x['score'], "rating_cnt": x['rating_cnt']} for x
                                       in country_stat}),
        'platform_type': platform_type
    }
    return summary_statistics


def collect_app_store_data(context) -> tuple:
    """Collect summary statistics using collect_apple_summary_stats function and reviews using collect_apple_reviews
        functions. Returns dictionary with summary statistics and dataframe with reviews."""
    token = get_appstore_token()
    df_reviews = collect_apple_reviews(context, token)
    summary_statistics = collect_apple_summary_stats(df_reviews.shape[0])

    return summary_statistics, df_reviews
