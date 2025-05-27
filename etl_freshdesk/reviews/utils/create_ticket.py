import requests
import json
from utility_hub.core_tools import get_creds_from_vault

EXT_API_KEY = get_creds_from_vault('FD_EXT_API_KEY')
EXT_DOMAIN = get_creds_from_vault('FD_EXT_DOMAIN')

DOMAIN, API_KEY, API_PASS = EXT_DOMAIN, EXT_API_KEY, 'xxx'

desc_translation_block_template = '<b>Translated text:</b><br><blockquote>{trans_text}</blockquote><br>'
ticket_desc_template = '''
    <b>Review published on:</b> {pub_date}<br>
    <b>User country:</b> {country}<br>
    <b>Score:</b> {score}<br>
    <b>Original text:</b><br><blockquote>{orig_text}</blockquote><br>
    {trans_block}
    <b>Language:</b> {lang}<br>
    <b>Review id: </b>{review_id}
'''
ticket_title_template = 'New {score}-star review in the App Store!'

ticket_fields = {
    'email': 'noreply.applereviews@gmail.com',
    'priority': 3,
    'status': 2,
    'group_id': 60000405008,
    'type': 'Feedback',
    'tags': ['applereviews'],
    'custom_fields': {
        'cf_rand938752': 'Jobseeker',
        'cf_rand987839': 'Reviews',
        'cf_rand749641': 'Impossible to determine'
    }
}


def send_review_to_freshdesk(review_info: dict) -> int:
    """Create Freshdesk ticket using API for the given review.

    Args:
        review_info (dict): dictionary with review info.

    Returns:
        int: created ticket identifier.
    """
    ticket_title = ticket_title_template.format(score=review_info['score'])
    desc_translation_block = ''
    if review_info['review_translation'] is not None:
        desc_translation_block = desc_translation_block_template.format(trans_text=review_info['review_translation'])

    ticket_desc = ticket_desc_template.format(
        pub_date=review_info['published_date'],
        country=review_info['country_code'],
        score=review_info['score'],
        orig_text=review_info['review_text'],
        trans_block=desc_translation_block,
        lang=review_info['language_code'],
        review_id=review_info['review_id']
    )
    ticket_data = ticket_fields.copy()
    ticket_data['subject'] = ticket_title
    ticket_data['description'] = ticket_desc

    r = requests.post(
        "https://{domain}.freshdesk.com/api/v2/tickets".format(domain=DOMAIN),
        auth=(API_KEY, API_PASS),
        headers={'Content-Type': 'application/json'},
        data=json.dumps(ticket_data)
    )
    if r.status_code >= 400:
        raise Exception('Encountered an error while ticket creation.\nStatus code: {}\n{}'.format(
            r.status_code, r.text))
    return r.json()['id']
