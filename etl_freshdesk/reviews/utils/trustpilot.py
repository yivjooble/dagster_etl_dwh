import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from datetime import datetime
import json
from etl_freshdesk.reviews.utils.utils import execute_query, insert_to_db

SCHEMA = 'freshdesk_external'


def get_date_string(date_dt: str) -> datetime or None:
    """Convert string (%Y-%m-%dT%H:%M:%S.%fZ format) to datetime."""
    if pd.isnull(date_dt):
        return None
    return datetime.strptime(date_dt, '%Y-%m-%dT%H:%M:%S.%fZ')


def parse_trustpilot_page() -> BeautifulSoup:
    """Parse trustpilot page and return BeautifulSoup object for future steps."""
    url = 'https://www.trustpilot.com/review/jooble.org?languages=all&sort=recency'
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        page_content = page.content()
        soup = BeautifulSoup(page_content, 'lxml')
        browser.close()
    return soup


def collect_trustpilot_score(soup: BeautifulSoup) -> pd.DataFrame:
    """Parse current trustpilot score from BeautifulSoup object and return dataframe with single row with current date
     and score."""
    current_trust_score = [t for t in soup.find_all('div')
                           if 'data-rating-component' in t.attrs][0].find('p').contents[0]
    current_trust_score_df = pd.DataFrame({
        'date': datetime.now().strftime('%Y-%m-%d'), 'score': current_trust_score
    }, index=[0])

    return current_trust_score_df


def collect_trustpilot_reviews(soup: BeautifulSoup) -> pd.DataFrame:
    """Parse trustpilot reviews from BeautifulSoup object. Returns dataframe with most recent 20 reviews. Dataframe
    columns:
        - id: review id,
        - date_published: publish date for the review, UTC,
        - date_replied: agent reply date for the review, UTC,
        - score: score (1-5), specified in review,
        - country_code: country of review author,
        - language_code: language of review.
    """
    json_object = json.loads(soup.find("script", {'id': "__NEXT_DATA__"}).contents[0])
    reviews = json_object['props']['pageProps']['reviews']
    review_cnt = len(reviews)

    review_info_dict = {
        'id': [reviews[i]['id'] for i in range(0, review_cnt)],
        'date_published': [get_date_string(reviews[i].get('dates').get('publishedDate'))
                           if not pd.isnull(reviews[i].get('dates')) else None for i in range(0, review_cnt)],
        'date_replied': [get_date_string(reviews[i].get('reply').get('publishedDate'))
                         if not pd.isnull(reviews[i].get('reply')) else None for i in range(0, review_cnt)],
        'score': [reviews[i]['rating'] for i in range(0, review_cnt)],
        'country_code': [reviews[i]['consumer']['countryCode'] for i in range(0, review_cnt)],
        'language_code': [reviews[i]['language'] for i in range(0, review_cnt)]
    }
    review_info_df = pd.DataFrame.from_dict(review_info_dict)
    return review_info_df


def delete_existing_reviews(review_ids: list) -> None:
    """Delete reviews from review_ids list from the database (trustpilot_reviews table)."""
    del_q = '''
        DELETE FROM {}.trustpilot_reviews
        WHERE id in ({});
        '''.format(SCHEMA, ', '.join(list(['\'{}\''.format(r) for r in review_ids])))
    execute_query(del_q)


def collect_trustpilot_data() -> tuple:
    """Parse trustpilot page, collect and insert to the database our current score; collect 20 recent reviews and
    rewrite data on them in the database."""
    soup = parse_trustpilot_page()
    current_trust_score_df = collect_trustpilot_score(soup)
    review_info_df = collect_trustpilot_reviews(soup)

    review_ids = review_info_df['id'].to_list()
    delete_existing_reviews(review_ids)

    insert_to_db(SCHEMA, review_info_df, 'trustpilot_reviews')
    execute_query('delete from {}.trustpilot_score_snapshot where date = \'{}\''.format(
        SCHEMA, datetime.now().strftime('%Y-%m-%d')))
    insert_to_db(SCHEMA, current_trust_score_df, 'trustpilot_score_snapshot')
    return review_info_df, current_trust_score_df
