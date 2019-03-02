import json
import logging

import requests

from paasta_tools.slack import get_slack_client
try:
    from scribereader import scribereader
except ImportError:
    scribereader = None

SLACK_WEBHOOK_STREAM = 'stream_slack_incoming_webhook'
SCRIBE_ENV = 'uswest1-prod'
log = logging.getLogger(__name__)


def get_slack_blocks_for_initial_deployment(message, last_action=None, status=None, active_button=None):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Status: {status}\nLast action: {last_action}",
            },
        },
        {
            "type": "actions",
            "block_id": "deployment_actions",
            "elements": get_button_elements(["rollback", "forward"], active_button=active_button),
        },
    ]
    return blocks


def get_button_elements(buttons, active_button=None):
    elements = []
    for button in buttons:
        if button == "rollback" and active_button == "rollback":
            elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Rolling Back :zombocom: (Not Implemented)",
                        "emoji": True,
                    },
                    "value": "rollback",
                },
            )
        elif button == "rollback" and active_button != "rollback":
            elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Roll Back :arrow_backward: (Not Implemented)",
                        "emoji": True,
                    },
                    "value": "rollback",
                    "confirm": get_confirmation_object(button),
                },
            )
        elif button == "forward" and active_button == "forward":
            elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Rolling Forward :zombocom: (Not Implemented)",
                        "emoji": True,
                    },
                    "value": "forward",
                },
            )
        elif button == "forward" and active_button != "forward":
            elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Continue Forward :arrow_forward: (Not Implemented)",
                        "emoji": True,
                    },
                    "value": "forward",
                    "confirm": get_confirmation_object(button),
                },
            )
    return elements


def get_confirmation_object(action):
    return {
        "title": {
            "type": "plain_text",
            "text": "Are you sure?",
        },
        "text": {
            "type": "mrkdwn",
            "text": f"Did you mean to press {action}?",
        },
        "confirm": {
            "type": "plain_text",
            "text": "Yes. Do it!",
        },
        "deny": {
            "type": "plain_text",
            "text": "Stop, I've changed my mind!",
        },
    }


class ButtonPress():
    def __init__(self, event):
        self.event = event
        self.username = event["user"]["username"]
        self.response_url = event["response_url"]
        # TODO: Handle multiple actions?
        self.action = event["actions"][0]["value"]
        self.thread_ts = event["container"]["thread_ts"]
        self.channel = event["channel"]["name"]

    def __repr__(self):
        return self.event

    def update(self, blocks):
        # Implements responding to button presses
        # https://api.slack.com/messaging/interactivity/enabling#responding-to-interactions
        # But isn't the api_call method per-se
        # https://github.com/slackapi/python-slackclient/issues/270
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


def watch_for_slack_webhooks(sc):
    host_and_port = scribereader.get_env_scribe_host(SCRIBE_ENV, True)
    host = host_and_port['host']
    port = host_and_port['port']
    tailer = scribereader.get_stream_tailer(SLACK_WEBHOOK_STREAM, host, port)
    for line in tailer:
        event = parse_webhook_event_json(line)
        if is_relevant_event(event):
            buttonpress = event_to_buttonpress(event)
            followup_message = f"Got it. {buttonpress.username} pressed {buttonpress.action}"
            sc.post(channels=[buttonpress.channel], message=followup_message, thread_ts=buttonpress.thread_ts)
            action = buttonpress.action
            blocks = get_slack_blocks_for_initial_deployment(
                message="New Message", last_action=action, status=f"Taking action on the {action} button",
                active_button=action,
            )
            buttonpress.update(blocks)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    sc = get_slack_client()
    watch_for_slack_webhooks(sc)
