import json
import logging

import requests

try:
    from scribereader import scribereader
except ImportError:
    scribereader = None

SLACK_WEBHOOK_STREAM = 'stream_slack_incoming_webhook'
SCRIBE_ENV = 'uswest1-prod'
log = logging.getLogger(__name__)


def get_slack_blocks_for_initial_deployment(message):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message,
            },
        },
        {
            "type": "actions",
            "block_id": "rollback_block1",
            "elements": [
                {
                    "type": "button",
                    "text": {
                            "type": "plain_text",
                            "text": "Roll Back (Not Implemented)",
                    },
                    "value": "rollback",
                },
                {
                    "type": "button",
                    "text": {
                            "type": "plain_text",
                            "text": "Continue (Not Implemented)",
                    },
                    "value": "continue",
                },
            ],
        },
    ]
    return blocks


class ButtonPress():
    def __init__(self, event):
        self.event = event
        self.username = event["user"]["username"]
        self.response_url = event["response_url"]
        # TODO: Handle multiple actions?
        self.action = event["actions"][0]

    def __repr__(self):
        return self.event

    def ack(self):
        # Implements responding to button presses
        # https://api.slack.com/messaging/interactivity/enabling#responding-to-interactions
        # But isn't the api_call method per-se
        # https://github.com/slackapi/python-slackclient/issues/270

        # Acking a button means updating the original message.
        # Currently this function just appends to the message with an ack.
        blocks = self.event["message"]["blocks"]
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section", "text": {
                    "type": "mrkdwn",
                    "text": f"Got it. You pressed '{self.action['value']}'. This isn't implemented yet.",
                },
            },
        )
        requests.post(self.response_url, json={"blocks": blocks})


def event_to_buttonpress(event):
    return ButtonPress(event=event)


def parse_webhook_event_json(line):
    event = json.loads(line)
    log.debug(event)
    return event


def is_relevant_event(event):
    # TODO: Implement filtering
    return True


def watch_for_slack_webhooks():
    host_and_port = scribereader.get_env_scribe_host(SCRIBE_ENV, True)
    host = host_and_port['host']
    port = host_and_port['port']
    tailer = scribereader.get_stream_tailer(SLACK_WEBHOOK_STREAM, host, port)
    for line in tailer:
        event = parse_webhook_event_json(line)
        if is_relevant_event(event):
            buttonpress = event_to_buttonpress(event)
            buttonpress.ack()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    watch_for_slack_webhooks()
