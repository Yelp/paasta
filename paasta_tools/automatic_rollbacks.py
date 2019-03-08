import abc
import asyncio
import json
import logging
from typing import Collection
from typing import Iterator
from typing import Mapping
from typing import TYPE_CHECKING

import requests
import transitions.extensions
from mypy_extensions import TypedDict

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

        for state in list(self.states()):
            # Call e.g. on_enter_start(self.enter_start)
            for hook_type in ['enter', 'exit']:
                try:
                    hook = getattr(self, f'{hook_type}_{state}')
                except AttributeError:
                    continue
                else:
                    add_hook_fn = getattr(self.machine, f'on_{hook_type}_{state}')
                    add_hook_fn(hook)

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
