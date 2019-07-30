import abc
import asyncio
import json
import logging
import traceback
from typing import Dict
from typing import List
from typing import Optional

import requests
import transitions
from slackclient import SlackClient

from paasta_tools.automatic_rollbacks.state_machine import DeploymentProcess


try:
    from scribereader import scribereader
    from clog.readers import construct_conn_msg
except ImportError:
    scribereader = None
    construct_conn_msg = None

SLACK_WEBHOOK_STREAM = "stream_slack_incoming_webhook"
SCRIBE_ENV = "uswest1-prod"
log = logging.getLogger(__name__)


class ButtonPress:
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


async def get_slack_events():
    if scribereader is None:
        logging.error("Scribereader unavailable. Not tailing slack events.")
        return

    host_and_port = scribereader.get_env_scribe_host(SCRIBE_ENV, True)
    host = host_and_port["host"]
    port = host_and_port["port"]

    while True:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        writer.write(construct_conn_msg(stream=SLACK_WEBHOOK_STREAM).encode("utf-8"))
        await writer.drain()

        while True:
            line = await reader.readline()
            log.debug(f"got log line {line.decode('utf-8')}")
            if not line:
                break
            event = parse_webhook_event_json(line)
            if is_relevant_event(event):
                yield event

        log.warning("Lost connection to scribe host; reconnecting")


class SlackDeploymentProcess(DeploymentProcess, abc.ABC):
    def __init__(self) -> None:
        super().__init__()
        self.human_readable_status = "Initializing..."
        self.slack_client = self.get_slack_client()
        self.last_action = None
        self.summary_blocks_str = ""
        self.detail_blocks_str = ""
        self.send_initial_slack_message()

        asyncio.ensure_future(self.listen_for_slack_events(), loop=self.event_loop)
        asyncio.ensure_future(self.periodically_update_slack(), loop=self.event_loop)

    @abc.abstractmethod
    def get_slack_client(self) -> SlackClient:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_slack_channel(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_deployment_name(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_progress(self, summary=False) -> str:
        raise NotImplementedError()

    def get_active_button(self) -> Optional[str]:
        return None

    @abc.abstractmethod
    def get_button_text(self, button, is_active) -> str:
        raise NotImplementedError()

    def get_button_element(self, button, is_active):
        element = {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": self.get_button_text(button, is_active),
                "emoji": True,
            },
            "value": button,
        }
        if not is_active:
            element["confirm"] = self.get_confirmation_object(button)
        return element

    def get_summary_blocks_for_deployment(self) -> List[Dict]:
        deployment_name = self.get_deployment_name()
        progress = self.get_progress(summary=True)
        button_elements = self.get_button_elements()

        summary_parts = [deployment_name, progress]
        summary_parts.extend(self.get_extra_summary_parts_for_deployment())

        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": " | ".join(summary_parts)},
            }
        ]
        if button_elements != []:
            blocks.append(
                {
                    "type": "actions",
                    "block_id": "deployment_actions",
                    "elements": button_elements,
                }
            )
        return blocks

    def get_detail_slack_blocks_for_deployment(self) -> List[Dict]:
        status = getattr(self, "state", None) or "Uninitialized"
        deployment_name = self.get_deployment_name()
        message = self.human_readable_status
        progress = self.get_progress()
        last_action = self.last_action

        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"{deployment_name}"},
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"State machine: `{status}`\nProgress: {progress}\nLast operator action: {last_action}",
                },
            },
        ]

        blocks.extend(self.get_extra_blocks_for_deployment())
        return blocks

    def get_extra_blocks_for_deployment(self) -> List:
        return []

    def get_extra_summary_parts_for_deployment(self) -> List[str]:
        return []

    def get_button_elements(self):
        elements = []
        active_button = self.get_active_button()
        for button in self.get_available_buttons():
            is_active = button == active_button
            elements.append(self.get_button_element(button=button, is_active=is_active))
        return elements

    def get_confirmation_object(self, action):
        return {
            "title": {"type": "plain_text", "text": "Are you sure?"},
            "text": {"type": "mrkdwn", "text": f"Did you mean to press {action}?"},
            "confirm": {"type": "plain_text", "text": "Yes. Do it!"},
            "deny": {"type": "plain_text", "text": "Stop, I've changed my mind!"},
        }

    def get_available_buttons(self) -> List[str]:
        buttons = []

        if self.is_terminal_state(self.state):
            # If we're about to exit, always clear the buttons, since once we exit the buttons will stop working.
            return []

        for trigger in self.machine.get_triggers(self.state):
            suffix = "_button_clicked"
            if trigger.endswith(suffix):
                if all(
                    cond.target
                    == self.machine.resolve_callable(
                        cond.func,
                        transitions.EventData(
                            self.state, self, self.machine, self, args=(), kwargs={}
                        ),
                    )()
                    for transition in self.machine.get_transitions(
                        source=self.state, trigger=trigger
                    )
                    for cond in transition.conditions
                ):
                    buttons.append(trigger[: -len(suffix)])

        return buttons

    def update_slack_thread(self, message, color=None):
        if self.slack_client is None:
            print(f"Would update the slack thread with: {message}")
            return
        else:
            print(f"Updating slack thread with: {message}")
        if color:
            resp = self.slack_client.api_call(
                "chat.postMessage",
                channel=self.slack_channel,
                attachments=[{"text": message, "color": color}],
                thread_ts=self.slack_ts,
            )
        else:
            resp = self.slack_client.api_call(
                "chat.postMessage",
                channel=self.slack_channel,
                text=message,
                thread_ts=self.slack_ts,
            )

        if resp["ok"] is not True:
            log.error(f"Posting to slack failed: {resp['error']}")

    def send_initial_slack_message(self):
        if self.slack_client is None:
            return
        summary_blocks = self.get_summary_blocks_for_deployment()
        detail_blocks = self.get_detail_slack_blocks_for_deployment()
        resp = self.slack_client.api_call(
            "chat.postMessage", blocks=summary_blocks, channel=self.slack_channel
        )
        self.slack_ts = resp["message"]["ts"] if resp and resp["ok"] else None
        self.slack_channel_id = resp["channel"]
        if resp["ok"] is not True:
            log.error(f"Posting to slack failed: {resp['error']}")

        resp = self.slack_client.api_call(
            "chat.postMessage",
            blocks=detail_blocks,
            channel=self.slack_channel,
            thread_ts=self.slack_ts,
        )
        self.detail_slack_ts = resp["message"]["ts"] if resp and resp["ok"] else None

        if resp["ok"] is not True:
            log.error(f"Posting detail to slack failed: {resp['error']}")
            if resp["error"] == "invalid_blocks":
                log.error(f"Blocks: {detail_blocks!r}")

    def update_slack(self):
        if self.slack_client is None:
            return
        summary_blocks = self.get_summary_blocks_for_deployment()
        detail_blocks = self.get_detail_slack_blocks_for_deployment()

        summary_blocks_str = json.dumps(summary_blocks, sort_keys=True)
        detail_blocks_str = json.dumps(detail_blocks, sort_keys=True)

        if self.summary_blocks_str != summary_blocks_str:
            resp = self.slack_client.api_call(
                "chat.update",
                channel=self.slack_channel_id,
                blocks=summary_blocks,
                ts=self.slack_ts,
            )
            if resp["ok"]:
                self.old_summary_blocks_str = summary_blocks_str
            else:
                self.old_summary_blocks_str = ""  # So we retry next time.
                log.error(f"Posting to slack failed: {resp['error']}")

        if self.detail_blocks_str != detail_blocks_str:
            resp = self.slack_client.api_call(
                "chat.update",
                channel=self.slack_channel_id,
                blocks=detail_blocks,
                ts=self.detail_slack_ts,
            )
            if resp["ok"]:
                self.old_detail_blocks_str = detail_blocks_str
            else:
                self.old_detail_blocks_str = ""  # So we retry next time.
                log.error(f"Posting detail to slack failed: {resp['error']}")

    def update_slack_status(self, message):
        self.human_readable_status = message
        self.update_slack()

    async def periodically_update_slack(self):
        while self.state not in self.status_code_by_state():
            self.update_slack()
            await asyncio.sleep(20)

    def is_relevant_buttonpress(self, buttonpress):
        return self.slack_ts == buttonpress.thread_ts

    async def listen_for_slack_events(self):
        log.debug("Listening for slack events...")
        try:
            async for event in get_slack_events():
                log.debug(f"Got slack event: {event}")
                buttonpress = event_to_buttonpress(event)
                if self.is_relevant_buttonpress(buttonpress):
                    self.update_slack_thread(
                        f"{buttonpress.username} pressed {buttonpress.action}"
                    )
                    self.last_action = buttonpress.action

                    try:
                        self.trigger(f"{buttonpress.action}_button_clicked")
                    except (transitions.core.MachineError, AttributeError):
                        self.update_slack_thread(f"Error: {traceback.format_exc()}")
                else:
                    log.debug(
                        "But it was not relevant to this instance of mark-for-deployment"
                    )
        except Exception:
            log.error(f"Saw error in listen_for_slack_events: {traceback.format_exc()}")

    def notify_users(self, message):
        self.update_slack_thread(message)
