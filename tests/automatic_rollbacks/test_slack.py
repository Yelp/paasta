from unittest import mock

from slackclient import SlackClient

from paasta_tools.automatic_rollbacks import slack

REAL_ROLLBACK_PRESS = {
    "type": "block_actions",
    "team": {"id": "T0289TLJY", "domain": "yelp"},
    "user": {"id": "UA6JBNA0Z", "username": "kwa", "team_id": "T0289TLJY"},
    "api_app_id": "AAJ7PL9ST",
    "token": "WYj864vvUYGtcV4pyfmO4rOQ",
    "container": {
        "type": "message",
        "message_ts": "1551306063.241500",
        "channel_id": "CA05GTDB9",
        "is_ephemeral": False,
        "thread_ts": "1551306063.241500",
    },
    "trigger_id": "562162233904.2281938644.c15af7fa5b7e10836c6db7ece2f53eab",
    "channel": {"id": "CA05GTDB9", "name": "paasta"},
    "message": {
        "type": "message",
        "subtype": "bot_message",
        "text": "This content can't be displayed.",
        "ts": "1551306063.241500",
        "username": "PaaSTA",
        "bot_id": "BAJ8JMV9V",
        "thread_ts": "1551306063.241500",
        "reply_count": 1,
        "reply_users_count": 1,
        "latest_reply": "1551306064.241600",
        "reply_users": ["BAJ8JMV9V"],
        "replies": [{"user": "BAJ8JMV9V", "ts": "1551306064.241600"}],
        "subscribed": False,
        "blocks": [
            {
                "type": "section",
                "block_id": "K15S",
                "text": {
                    "type": "mrkdwn",
                    "text": "*compute-infra-test-service* - Marked *baaf4d7b2ddf* for deployment on *mesosstage.everything*.\n",
                    "verbatim": False,
                },
            },
            {
                "type": "actions",
                "block_id": "rollback_block1",
                "elements": [
                    {
                        "type": "button",
                        "action_id": "cLjFK",
                        "text": {
                            "type": "plain_text",
                            "text": "Roll Back (Not Implemented)",
                            "emoji": True,
                        },
                        "value": "rollback",
                    },
                    {
                        "type": "button",
                        "action_id": "Grjq",
                        "text": {
                            "type": "plain_text",
                            "text": "Continue (Not Implemented)",
                            "emoji": True,
                        },
                        "value": "continue",
                    },
                ],
            },
        ],
    },
    "response_url": "https://hooks.slack.com/actions/T0289TLJY/562866820372/0lRlD5JFQlLPPvqrelCpJlF9",
    "actions": [
        {
            "action_id": "cLjFK",
            "block_id": "rollback_block1",
            "text": {
                "type": "plain_text",
                "text": "Roll Back (Not Implemented)",
                "emoji": True,
            },
            "value": "rollback",
            "type": "button",
            "action_ts": "1551306127.199355",
        }
    ],
}  # noqa E501


class DummySlackDeploymentProcess(slack.SlackDeploymentProcess):
    """A minimum-viable SlackDeploymentProcess subclass."""

    def status_code_by_state(self):
        return {}

    def states(self):
        return ["_begin"]

    def valid_transitions(self):
        return []

    def start_transition(self):
        raise NotImplementedError()

    def start_state(self):
        return "_begin"

    def get_slack_client(self):
        mock_client = mock.Mock(spec=SlackClient)
        mock_client.api_call.return_value = {
            "ok": True,
            "message": {"ts": 10},
            "channel": "test",
        }
        return mock_client

    def get_slack_channel(self):
        return "#test"

    def get_deployment_name(self):
        return "deployment name"

    def get_progress(self, summary=False):
        return "progress%"

    def get_button_text(self, button, is_active):
        return f"{button} {is_active}"


class ErrorSlackDeploymentProcess(DummySlackDeploymentProcess):
    default_slack_channel = "#dne"

    def get_slack_client(self):
        mock_client = mock.Mock(spec=SlackClient)
        mock_client.api_call.return_value = {"ok": False, "error": "uh oh"}
        return mock_client


def test_slack_errors_no_exceptions():
    sdp = ErrorSlackDeploymentProcess()
    # Make sure slack methods don't fail.
    sdp.update_slack()
    sdp.update_slack_thread("Hello world")


def test_get_detail_slack_blocks_for_deployment_happy_path():

    sdp = DummySlackDeploymentProcess()
    blocks = sdp.get_detail_slack_blocks_for_deployment()
    assert blocks[0]["text"]["text"] == "deployment name"
    assert blocks[1]["text"]["text"] == "Initializing..."
    assert (
        blocks[2]["text"]["text"]
        == "State machine: `_begin`\nProgress: progress%\nLast operator action: None"
    )


def test_event_to_buttonpress_rollback():
    actual = slack.event_to_buttonpress(REAL_ROLLBACK_PRESS)
    assert actual.username == "kwa"
    assert actual.action == "rollback"
