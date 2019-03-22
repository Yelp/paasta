import abc
import asyncio
import json
import logging
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
from typing import Collection
from typing import Iterator
from typing import Mapping
from typing import TYPE_CHECKING

import requests
import transitions.extensions
from mypy_extensions import TypedDict

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


def get_button_element(button, is_active):
    active_button_texts = {
        "rollback": "Rolling Back :zombocom: (Not Impl.)",
        "forward": "Rolling Forward :zombocom: (Not Impl.)",
    }

    inactive_button_texts = {
        "rollback": "Roll Back :arrow_backward: (Not Impl.)",
        "forward": "Continue Forward :arrow_forward: (Not Impl.)",
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


def get_button_elements(buttons, active_button=None):
    elements = []
    for button in buttons:
        is_active = button == active_button
        elements.append(
            get_button_element(button=button, is_active=is_active),
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


def get_slack_events():
    if scribereader is None:
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
    process = Process(target=scribe_tail, kwargs=kw)
    process.start()
    while True:
        try:
            line = queue.get(block=True, timeout=0.1)
            event = parse_webhook_event_json(line)
            if is_relevant_event(event):
                yield line
        except Empty:
            pass


def watch_for_slack_webhooks(sc):
    for event in get_slack_events():
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


class TransitionDefinition(TypedDict):
    trigger: str
    source: str
    dest: str


class DeploymentProcess(abc.ABC):
    if TYPE_CHECKING:
        # These attributes need to be defined in this `if TYPE_CHECKING` block, because if they exist at runtime then
        # transitions will refuse to overwrite them.
        state: str

        def trigger(self, *args, **kwargs):
            ...

    def __init__(
        self,
    ):

        self.event_loop = asyncio.get_event_loop()
        self.finished_event = asyncio.Event(loop=self.event_loop)

        self.machine = transitions.extensions.LockedMachine(
            model=self,
            states=list(self.states()),
            transitions=list(self.valid_transitions()),
            initial=self.start_state(),
            after_state_change=self.after_state_change,
            queued=True,
        )

    @abc.abstractmethod
    def status_code_by_state(self) -> Mapping[str, int]:
        raise NotImplementedError()

    @abc.abstractmethod
    def states(self) -> Collection['str']:
        raise NotImplementedError()

    @abc.abstractmethod
    def valid_transitions(self) -> Iterator[TransitionDefinition]:
        raise NotImplementedError()

    @abc.abstractmethod
    def start_transition(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def start_state(self):
        raise NotImplementedError()

    def finish(self):
        self.finished_event.set()

    def run(self):
        return self.event_loop.run_until_complete(self.run_async())

    async def run_async(self) -> int:
        self.trigger(self.start_transition())
        await self.finished_event.wait()
        return self.status_code_by_state().get(self.state, 3)

    def after_state_change(self):
        if self.state in self.status_code_by_state():
            self.event_loop.call_soon_threadsafe(self.finished_event.set)
