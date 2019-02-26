from paasta_tools import automatic_rollbacks


def test_get_slack_blocks_for_initial_deployment_happy_path():
    blocks = automatic_rollbacks.get_slack_blocks_for_initial_deployment("test")
    assert blocks[0]["text"]["text"] == "test"
