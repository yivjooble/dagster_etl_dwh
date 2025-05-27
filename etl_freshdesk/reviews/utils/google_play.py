import json
from concurrent.futures import ThreadPoolExecutor
from google_play_scraper import app, reviews, Sort
import pandas as pd
from etl_freshdesk.reviews.utils.utils import list_to_db_array
from etl_freshdesk.reviews.utils.config import GOOGLE_COUNTRIES, GOOGLE_LANGUAGES


def collect_google_country_stat(country: str) -> dict:
    """Collect data for Jooble mobile app from Google Play page for the given country.
    Args:
        country (str): country code to collect data for.

    Returns:
        dict: dictionary with app details for the given country. Keys:
            country_code: country, from which data is collected.
            install_cnt: total installs of the app (showed for all countries on each page).
            score: score that is displayed in the given country.
            rating_cnt: total number of ratings for the app (showed for all countries on each page).
            review_cnt: number of reviews for the app in the given country.
                In countries with low number of ratings, worldwide total is displayed instead.
            score_histogram: ratings breakdown by the type of ratings (inaccurate, sums up into total worldwide ratings
                count). In countries with low number of ratings, worldwide total is displayed instead.
    """
    app_stat = app('jooble.org', country=country)
    if app_stat['score']:
        return {
            'country_code': country,
            'install_cnt': app_stat['realInstalls'],
            'rating_cnt': app_stat['ratings'],
            'score_histogram': list_to_db_array(app_stat['histogram']),
            'score': round(app_stat['score'], 2),
            'review_cnt': app_stat['reviews']
        }


def collect_google_summary_stats() -> dict:
    """Collect data for Jooble mobile app from Google Play page for all countries. By determining mode values for score,
        determine reviews average worldwide score, total worldwide ratings, reviews and histogram.

    Returns:
        dict: dictionary with data from Google Play. Keys:
            install_cnt: total installs of the app (all countries).
            score: average total score that is displayed in countries where ratings count is low.
            rating_cnt: total number of ratings for the app (all countries).
            review_cnt: total number of reviews for the app (all countries).
            score_histogram: ratings breakdown by the type of ratings (stars count) for all countries.
            country_details: dictionary with country codes as keys and dictionary with score and reviews keys as value.
    """
    country_list = [x['country_code'] for x in GOOGLE_COUNTRIES]
    platform_type = 1
    summary_col_list = ['install_cnt', 'score', 'rating_cnt', 'review_cnt', 'score_histogram']
    with ThreadPoolExecutor(8) as executor:
        results = executor.map(collect_google_country_stat, country_list)
    df = pd.DataFrame.from_records([x for x in results if x])
    df = df[df['score'] != 0].copy()

    mode_score = df['score'].mode().iloc[0]
    mode_row = df[df['score'] == mode_score].drop_duplicates(subset=summary_col_list)
    df_country = df[(df['score'] != mode_score)].copy()

    summary_statistics = mode_row[summary_col_list].to_dict('index')[0]
    summary_statistics['country_details'] = json.dumps(df_country.set_index(
        'country_code')[['score', 'review_cnt']].to_dict('index'))
    summary_statistics['platform_type'] = platform_type
    return summary_statistics


def collect_google_reviews_single(lang: str, max_results: int = 1000) -> list:
    """Collect reviews from google play for the given language.

    Args:
        lang (str): language code.
        max_results (int): number of results collected by single google_play_scraper.reviews call (default=1000).

    Returns:
        list: list of dicts with reviews info: 1 as platform_type, language, id, text, score, date of publish and reply.
    """
    platform_type = 1
    result, continuation_token = reviews('jooble.org', lang=lang, sort=Sort.NEWEST, count=max_results)
    n_reviews = len(result)
    while n_reviews == max_results:
        result_new, continuation_token = reviews('jooble.org', continuation_token=continuation_token)
        result += result_new
        n_reviews = len(result_new)

    reviews_list = [{
        'platform_type': platform_type,
        'review_id': x['reviewId'],
        'language_code': lang,
        'country_code': None,
        'score': x['score'],
        'review_text': x['content'],
        'review_translation': None,
        'published_date': x['at'],
        'response_date': x['repliedAt'],
        'is_deleted': False,
        'ticket_id': None
    } for x in result]

    return reviews_list


def collect_google_reviews_many(language_list: list) -> pd.DataFrame:
    """Collect reviews from google play for the given list of languages using collect_google_reviews_single function.

    Args:
        language_list (list): list of languages to collect data for.

    Returns:
        pd.DataFrame: dataframe with review info.
    """
    with ThreadPoolExecutor(8) as executor:
        results_iter = executor.map(collect_google_reviews_single, language_list)

    results = [y for x in results_iter for y in x]
    df_reviews = pd.DataFrame.from_records(results)
    return df_reviews


def collect_google_reviews_all() -> pd.DataFrame:
    """Collect reviews from google play for all the languages using collect_google_reviews_many function. As single
        review may display on google play pages for multiple languages, first collect reviews for most important
        languages, then for other languages. Finally, drop duplicated review ids and keep first row for each id.

    Returns:
        pd.DataFrame: dataframe with all collected reviews for all languages available in google play.
    """
    lang_list = [x['language_code'] for x in GOOGLE_LANGUAGES]
    top_langs = ['es', 'en', 'id', 'pt', 'ru', 'uk', 'ro', 'tr', 'sr', 'fr', 'de', 'it', 'nl', 'ar']
    other_langs = [x for x in lang_list if x not in top_langs]

    df_reviews = pd.concat([
        collect_google_reviews_many(top_langs),
        collect_google_reviews_many(other_langs)
    ], ignore_index=True)

    df_reviews = df_reviews.drop_duplicates(subset=['review_id'], keep='first')
    return df_reviews


def collect_google_play_data(context):
    """Collect summary statistics using collect_google_summary_stats function and reviews using
        collect_google_reviews_all functions. Returns dictionary with summary statistics and dataframe with reviews.
    """
    summary_statistics = collect_google_summary_stats()
    df_reviews = collect_google_reviews_all()
    context.log.debug('Google Play data collected')

    return summary_statistics, df_reviews
