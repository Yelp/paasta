import json
import logging
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
from typing import Collection
from typing import Dict
from typing import List

import requests

from paasta_tools.automatic_rollbacks.slo import SLOWatcher

try:
    from scribereader import scribereader
except ImportError:
    scribereader = None

SLACK_WEBHOOK_STREAM = 'stream_slack_incoming_webhook'
SCRIBE_ENV = 'uswest1-prod'
log = logging.getLogger(__name__)


def get_slack_blocks_for_deployment(
    deployment_name: str,
    message,
    last_action=None,
    status=None,
    progress=None,
    active_button=None,
    available_buttons=["rollback", "forward"],
    from_sha=None,
    to_sha=None,
    slo_watchers=None,
) -> List[Dict]:

    button_elements = get_button_elements(
        available_buttons, active_button=active_button,
        from_sha=from_sha, to_sha=to_sha,
    )
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{deployment_name}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"State machine: `{status}`\nProgress: {progress}\nLast operator action: {last_action}",
            },
        },
    ]

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": get_slo_text(slo_watchers),
        },
    })

    if button_elements != []:
        blocks.append({
            "type": "actions",
            "block_id": "deployment_actions",
            "elements": button_elements,
        })
    return blocks


def get_slo_text(slo_watchers: Collection[SLOWatcher]) -> str:
    if slo_watchers is not None and len(slo_watchers) > 0:
        num_failing = len([w for w in slo_watchers if w.failing])
        if num_failing > 0:
            slo_text = f":alert: {num_failing} of {len(slo_watchers)} SLOs are failing"
        else:

            num_unknown = len([w for w in slo_watchers if w.bad_before_mark is None or w.bad_after_mark is None])
            num_bad_before_mark = len([w for w in slo_watchers if w.bad_before_mark])
            slo_text_components = []
            if num_unknown > 0:
                slo_text_components.append(f":thinking_face: {num_unknown} SLOs are missing data. ")
            if num_bad_before_mark > 0:
                slo_text_components.append(
                    f":grimacing: {num_bad_before_mark} SLOs were failing before deploy, and will be ignored.",
                )

            remaining = len(slo_watchers) - num_unknown - num_bad_before_mark

            if remaining == len(slo_watchers):
                slo_text = f":ok_hand: All {len(slo_watchers)} SLOs are currently passing."
            else:
                if remaining > 0:
                    slo_text_components.append(f"The remaining {remaining} SLOs are currently passing.")
                slo_text = ' '.join(slo_text_components)
    else:
        slo_text = "No SLOs defined for this service."

    return slo_text


def get_button_element(button, is_active, from_sha, to_sha):
    active_button_texts = {
        "rollback": f"Rolling Back to {from_sha[:8]} :zombocom:",
        "forward": f"Rolling Forward to {to_sha[:8]} :zombocom:",
    }

    inactive_button_texts = {
        "rollback": f"Roll Back to {from_sha[:8]} :arrow_backward:",
        "forward": f"Continue Forward to {to_sha[:8]} :arrow_forward:",
        "complete": f"Complete deploy to {to_sha[:8]} :white_check_mark:",
        "abandon": f"Abandon deploy, staying on {from_sha[:8]} :x:",
        "snooze": f"Reset countdown",
        "enable_auto_rollbacks": "Enable auto rollbacks :eyes:",
        "disable_auto_rollbacks": "Disable auto rollbacks :close_eyes_monkey:",
    }

    if is_active is True:
        confirm = False
        text = active_button_texts[button]
    else:
        confirm = get_confirmation_object(button)
        text = inactive_button_texts[button]

    element = {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": text,
            "emoji": True,
        },
        "confirm": confirm,
        "value": button,
    }
    if not confirm:
        del element["confirm"]
    return element


def get_button_elements(buttons, active_button=None, from_sha=None, to_sha=None):
    elements = []
    for button in buttons:
        is_active = button == active_button
        elements.append(
            get_button_element(button=button, is_active=is_active, from_sha=from_sha, to_sha=to_sha),
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
        self.thread_ts = event["container"].get("thread_ts", None)
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


def get_slack_events():
    if scribereader is None:
        logging.error("Scribereader unavailable. Not tailing slack events.")
        return

    def scribe_tail(queue):
        host_and_port = scribereader.get_env_scribe_host(SCRIBE_ENV, True)
        host = host_and_port['host']
        port = host_and_port['port']
        tailer = scribereader.get_stream_tailer(SLACK_WEBHOOK_STREAM, host, port)
        for line in tailer:
            queue.put(line)

    # Tailing scribe is not thread-safe, therefore we must use a Multiprocess-Queue-based
    # approach, with paasta logs as prior art.
    queue = Queue()
    kw = {'queue': queue}
    process = Process(target=scribe_tail, daemon=True, kwargs=kw)
    process.start()
    while True:
        try:
            line = queue.get(block=True, timeout=0.1)
            event = parse_webhook_event_json(line)
            if is_relevant_event(event):
                yield event
        except Empty:
            pass
