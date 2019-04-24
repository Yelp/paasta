import abc
import asyncio
import datetime
import json
import logging
import time
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
from typing import Collection
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

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
):

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
    if button_elements != []:
        blocks.append({
            "type": "actions",
            "block_id": "deployment_actions",
            "elements": button_elements,
        })
    return blocks


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


class TransitionDefinition(TypedDict):
    trigger: str
    source: Union[str, List[str], Tuple[str, ...]]
    dest: Union[str, List[str], Tuple[str, ...]]


class DeploymentProcess(abc.ABC):
    if TYPE_CHECKING:
        # These attributes need to be defined in this `if TYPE_CHECKING` block, because if they exist at runtime then
        # transitions will refuse to overwrite them.
        state: str

        def trigger(self, *args, **kwargs):
            ...

    run_timeout: Optional[float] = None  # in normal operation, this will be None, but this lets tests set a max time.

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
            before_state_change=self.before_state_change,
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
        await asyncio.wait_for(self.finished_event.wait(), timeout=self.run_timeout)
        return self.status_code_by_state().get(self.state, 3)

    def after_state_change(self):
        if self.state in self.status_code_by_state():
            self.event_loop.call_soon_threadsafe(self.finished_event.set)

    def start_timer(self, timeout, trigger, message_verb):
        self.cancel_timer()

        timer_start = time.time()
        timer_end = timer_start + timeout
        formatted_time = datetime.datetime.fromtimestamp(timer_end)

        self.update_slack_thread(f"Will {message_verb} in {timeout} seconds, (at {formatted_time})")

        def times_up():
            self.update_slack_thread(f"Time's up, will now {message_verb}.")
            self.trigger(trigger)

        def schedule_callback():
            """Unfortunately, call_at is not threadsafe, and there's no call_at_threadsafe, so we need to schedule the
            call to call_at with call_soon_threadsafe."""
            self.timer_handle = self.event_loop.call_later(timeout, times_up)

        self.event_loop.call_soon_threadsafe(schedule_callback)

    def cancel_timer(self):
        try:
            handle = self.timer_handle
        except AttributeError:
            return
        handle.cancel()

    def before_state_change(self):
        self.cancel_timer()
