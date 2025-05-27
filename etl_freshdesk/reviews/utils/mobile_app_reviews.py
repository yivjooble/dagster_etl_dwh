from datetime import datetime
import pandas as pd
from deep_translator import GoogleTranslator, single_detection
from retrying import retry


from etl_freshdesk.reviews.utils.app_store import collect_app_store_data
from etl_freshdesk.reviews.utils.google_play import collect_google_play_data
from etl_freshdesk.reviews.utils.utils import insert_to_db, execute_query, select_to_dataframe
from etl_freshdesk.reviews.utils.slack_review_sender import SlackReviewSender
from etl_freshdesk.reviews.utils.create_ticket import send_review_to_freshdesk
from utility_hub.core_tools import get_creds_from_vault

SLACK_TOKEN = get_creds_from_vault('DWH_ALERTS_TOKEN')
LANG_DETECTION_API_KEY = get_creds_from_vault('LANG_DETECTION_API_KEY')
SLACK_CHANNEL = "C04Q0L7NP7S"
SCHEMA = 'freshdesk_external'


@retry(stop_max_attempt_number=10, wait_fixed=5000)
def translate_review(text: str, translator=GoogleTranslator(source='auto', target='en')) -> tuple:
    """Translates review text. In case if review language is not in (en, ru, uk), return detected language and review
        text translated to English.

    Args:
        text (str): review original text.
        translator (deep_translator.google.GoogleTranslator): translator object.

    Returns:
        tuple: detected language (str), translated text (str if language is not in (en, uk, ru), None otherwise).
    """
    try:
        source_lang = single_detection(text, api_key=LANG_DETECTION_API_KEY)
    except:
        source_lang = None
    translation_text = None
    if source_lang is None or source_lang not in ['en', 'ru', 'uk']:
        translation_text = translator.translate(text)
    return source_lang, translation_text


def insert_summary_statistics(summary_stats_df: pd.DataFrame) -> None:
    """Insert collected summary statistics from two platforms to the database.

    Args:
        summary_stats_df (pd.DataFrame): summary statistics collected from Google Play and App Store.
    """
    summary_stats_df_copy = summary_stats_df.copy()
    record_date = datetime.now().date().strftime('%Y-%m-%d')
    summary_stats_df_copy['record_date'] = record_date
    execute_query("delete from {}.mobile_statistics_snapshot where record_date = '{}'".format(SCHEMA, record_date))
    insert_to_db(SCHEMA, summary_stats_df_copy, 'mobile_statistics_snapshot')


def add_new_reviews_to_db(review_df: pd.DataFrame, review_idx: list, return_result: bool = False) -> pd.DataFrame:
    """Translate new reviews, fill missing language values with detected language, insert review records into the
        database.

    Args:
        review_df (pd.DataFrame): dataframe with new reviews to insert into the database.
        return_result (bool): True if function should return transformed review_df.
        review_idx (list): list of tuples with platform_type and review_id to be deleted.

    Returns:
        pd.DataFrame or None: returns transformed review_df if return_result is True and None otherwise.
    """
    if review_df.shape[0] > 0:
        translator = GoogleTranslator(source='auto', target='en')
        df = review_df.copy()

        df['trans_source_lang'], df['review_translation'] = zip(
            *df['review_text'].apply(lambda x: translate_review(x, translator)))
        df['language_code'] = df['language_code'].fillna(df['trans_source_lang'])
        df.drop('trans_source_lang', axis=1, inplace=True)

        for r in review_idx:
            q = "delete from {}.mobile_reviews where platform_type = {} and review_id = '{}'".format(SCHEMA, r[0], r[1])
            execute_query(q)
        insert_to_db(SCHEMA, df, 'mobile_reviews')

        if return_result:
            return df


def upd_deleted_reviews(review_idx: list) -> None:
    """Update given reviews from the database - set is_deleted to True.

    Args:
        review_idx (list): list of tuples with platform_type and review_id to be updated.
    """
    for r in review_idx:
        q = "update {}.mobile_reviews set is_deleted = true where platform_type = {} and review_id = '{}'".format(
            SCHEMA, r[0], r[1])
        execute_query(q)


def update_reviews_data(df_reviews: pd.DataFrame) -> pd.DataFrame:
    """Compare collected reviews with the ones in the database to find new reviews (to insert into the database),
        updated ones (to remove from the database and insert with the updated values), deleted ones (to set is_deleted
        to True in the database).

    Args:
        df_reviews (pd.DataFrame): dataframe with collected reviews.

    Returns:
        pd.DataFrame: dataframe with new reviews.
    """
    for col in ['published_date', 'response_date']:
        df_reviews[col] = pd.to_datetime(df_reviews[col])
    db_reviews = select_to_dataframe('select * from {}.mobile_reviews'.format(SCHEMA))

    db_reviews = db_reviews.set_index(['platform_type', 'review_id'])
    df_reviews = df_reviews.set_index(['platform_type', 'review_id']).drop('ticket_id', axis=1).merge(
        db_reviews[['ticket_id']], left_index=True, right_index=True, how='left')

    new_reviews_idx = [x for x in df_reviews.index if x not in db_reviews.index]
    ex_reviews_idx = [x for x in df_reviews.index if x in db_reviews.index]
    del_reviews_idx = [x for x in db_reviews.index if x not in df_reviews.index]

    check_cols = ['country_code', 'score', 'review_text', 'published_date', 'response_date', 'is_deleted']
    updated_reviews_mask = pd.DataFrame(df_reviews.loc[ex_reviews_idx, check_cols].fillna(0) == db_reviews.loc[
        ex_reviews_idx, check_cols].fillna(0)).apply(sum, axis=1) < len(check_cols)
    updated_reviews_df = df_reviews.loc[ex_reviews_idx, :][updated_reviews_mask]
    new_reviews_df = df_reviews.loc[new_reviews_idx, :]

    upd_deleted_reviews(del_reviews_idx)
    add_new_reviews_to_db(updated_reviews_df.reset_index(), updated_reviews_df.index)
    new_reviews = add_new_reviews_to_db(new_reviews_df.reset_index(), [], True)
    return new_reviews


def send_new_app_store_reviews_to_fd():
    """
    Finds new App Store reviews without associated ticket id, creates a new ticket for the review and updates the
    mobile_reviews.ticket_id column with the id of the created ticket.
    """
    q = 'select * from {}.mobile_reviews where platform_type = 2 and ticket_id is null'.format(SCHEMA)
    db_reviews = select_to_dataframe(q)
    for review in db_reviews.to_dict('records'):
        ticket_id = send_review_to_freshdesk(review)
        q = "update {}.mobile_reviews set ticket_id = {} where platform_type = 2 and review_id = '{}'".format(
            SCHEMA, ticket_id, review['review_id'])
        execute_query(q)


def collect_reviews_data(context) -> None:
    """Collect summary statistics and reviews data from Google Play and App Store, update reviews data in the database.
    Send new reviews to Slack channel. Send new App Store reviews to Freshdesk.
    """
    g_summary_statistics, g_reviews_df = collect_google_play_data(context)
    context.log.info('Google Play data collected')

    a_summary_statistics, a_reviews_df = collect_app_store_data(context)
    context.log.info('App Store data collected')

    summary_stats_df = pd.DataFrame.from_records([g_summary_statistics, a_summary_statistics])
    insert_summary_statistics(summary_stats_df)

    df_reviews = pd.concat([g_reviews_df, a_reviews_df], ignore_index=True)
    new_reviews = update_reviews_data(df_reviews)
    if new_reviews is not None:
        new_reviews['published_date'] = new_reviews['published_date'].dt.strftime('%Y-%m-%d %X')
        new_reviews_dicts = new_reviews.to_dict('records')

        review_sender = SlackReviewSender(token=SLACK_TOKEN)
        for review in new_reviews_dicts:
            review_sender.send(channel=SLACK_CHANNEL, review_info=review)

    send_new_app_store_reviews_to_fd()
