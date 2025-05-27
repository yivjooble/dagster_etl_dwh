import slack_sdk
from yaml import dump


def bold(text):
    return "*{}*".format(str(text))


def code(text):
    return "```{}```".format(str(text))


def quote(text):
    return ">_{}_".format(str(text))


def mrkdwn_block(markdown_text=None):
    _markdown_text = ""
    if markdown_text is not None:
        _markdown_text = str(markdown_text)

    return {"type": "section", "text": {"type": "mrkdwn", "text": _markdown_text}}


def divider_block():
    return {"type": "divider"}


def construct_text_block(emoji, title, text=None):
    _review_text = ""
    if text is not None:
        _review_text = str(text)

    _block = mrkdwn_block(
        emoji + ' ' + bold(title) + '\n' + quote(_review_text)
    )
    return _block


def construct_metadata_block(review_info):
    metadata = {x: y for x, y in review_info.items() if y is not None and x not in
                ['platform_type', 'score', 'review_text', 'review_translation', 'is_deleted', 'response_date']}
    _metadata = dump(metadata, indent=4, default_flow_style=False, sort_keys=False)

    _block = mrkdwn_block(":gear: " + bold("Metadata:") + "\n" + code(_metadata))
    return _block


class SlackReviewSender:
    """
    Python message transporter for Slack
    """

    COLORS = {
        1: "#FE0A0A",
        2: "#F3CE03",
        3: "#EBFF0A",
        4: "#86E62C",
        5: "#209D05"
    }

    PLATFORMS = {
        1: "Google Play",
        2: "App Store"
    }

    EMOJIS = {
        "Google Play": ':google:',
        "App Store": ':apple:'
    }

    def __init__(self, token):
        if token is None:
            raise ValueError("The field token cannot be:", token)
        self.token = str(token)

        self.slack = slack_sdk.WebClient(token=self.token)

    def _construct_title_block(self, rating, platform):
        _emoji = self.EMOJIS.get(platform)
        _title_text = "New {}-star review on {}".format(rating, platform)
        title = _emoji + " " + bold(_title_text)

        _block = mrkdwn_block(title)
        return _block

    def send(self, channel, review_info):
        _color = self.COLORS.get(review_info['score'])
        _platform = self.PLATFORMS.get(review_info['platform_type'])

        # The final list of all the blocks to be sent in the notification
        _blocks = list()

        _title_block = self._construct_title_block(review_info['score'], _platform)
        _blocks.append(_title_block)

        _review_orig_text_block = construct_text_block(emoji=':memo:', title='Original review text:',
                                                       text=review_info['review_text'])
        _blocks.append(_review_orig_text_block)

        if review_info['review_translation'] is not None:
            _review_trans_text_block = construct_text_block(emoji=':flag-gb:', title='Translated review text:',
                                                            text=review_info['review_translation'])
            _blocks.append(_review_trans_text_block)

        _blocks.append(divider_block())
        _metadata_block = construct_metadata_block(review_info)
        _blocks.append(_metadata_block)

        payload = {
            "channel": channel,
            "attachments": [{"color": _color, "blocks": _blocks}],
        }

        response = self.slack.chat_postMessage(**payload)
        return response
