from unittest import mock

import pytest
from botocore.exceptions import EndpointConnectionError

from paasta_tools import deploy_slack_threads
from paasta_tools.utils import DeploySlackThreadsConfig
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import SystemPaastaConfigDict

TABLE_CONFIG: DeploySlackThreadsConfig = {
    "table_name": "deploy-slack-threads",
    "region": "us-west-2",
}


@pytest.fixture
def mock_boto3_client():
    with mock.patch(
        "paasta_tools.deploy_slack_threads.boto3.client", autospec=True
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_time():
    with mock.patch(
        "paasta_tools.deploy_slack_threads.time.time",
        autospec=True,
        return_value=1791500000.123,
    ):
        yield


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("BUILD_URL", raising=False)
    monkeypatch.delenv(deploy_slack_threads.TABLE_NAME_ENV_VAR, raising=False)


def record(config_dict: SystemPaastaConfigDict) -> None:
    deploy_slack_threads.record_deploy_slack_thread(
        system_paasta_config=SystemPaastaConfig(config_dict, "/fake/dir"),
        service="fake_service",
        deploy_group="prod.everything",
        commit="abc123",
        channel_id="C0123",
        channel_name="#fake-channel",
        thread_ts="1791500000.123456",
        authors=["alice", "bob"],
    )


def test_build_item_jenkins(monkeypatch):
    monkeypatch.setenv("BUILD_URL", "https://jenkins.example.com/job/1/")
    item = deploy_slack_threads.build_item(
        service="fake_service",
        deploy_group="prod.everything",
        commit="abc123",
        channel_id="C0123",
        channel_name="#fake-channel",
        thread_ts="1791500000.123456",
        authors=["alice", "bob"],
        ttl_days=30,
        now_ms=1791500000123,
    )
    assert item == {
        "service_deploy_group": {"S": "fake_service#prod.everything"},
        "created_at_ms": {"N": "1791500000123"},
        "service": {"S": "fake_service"},
        "deploy_group": {"S": "prod.everything"},
        "channel": {"S": "C0123"},
        "channel_name": {"S": "#fake-channel"},
        "thread_ts": {"S": "1791500000.123456"},
        "sha": {"S": "abc123"},
        "initiator": {"S": "jenkins"},
        "initiated_by": {"S": "https://jenkins.example.com/job/1/"},
        "authors": {"L": [{"S": "alice"}, {"S": "bob"}]},
        "created_at": {"N": "1791500000"},
        "expires_at": {"N": str(1791500000 + 30 * 86400)},
    }


@mock.patch(
    "paasta_tools.deploy_slack_threads.get_username",
    autospec=True,
    return_value="alice",
)
def test_build_item_human_without_optional_fields(mock_get_username):
    item = deploy_slack_threads.build_item(
        service="fake_service",
        deploy_group="dev.everything",
        commit="abc123",
        channel_id="C0123",
        channel_name=None,
        thread_ts="1791500000.123456",
        authors=None,
        ttl_days=7,
        now_ms=1791500000123,
    )
    assert item["initiator"] == {"S": "human"}
    assert item["initiated_by"] == {"S": "alice"}
    assert item["expires_at"] == {"N": str(1791500000 + 7 * 86400)}
    assert "channel_name" not in item
    assert "authors" not in item


def test_record_writes_to_configured_table(mock_boto3_client, mock_time):
    record({"deploy_slack_threads": TABLE_CONFIG})

    assert mock_boto3_client.call_args[0] == ("dynamodb",)
    assert mock_boto3_client.call_args[1]["region_name"] == "us-west-2"
    put_item = mock_boto3_client.return_value.put_item
    put_item.assert_called_once()
    assert put_item.call_args[1]["TableName"] == "deploy-slack-threads"
    assert put_item.call_args[1]["ConditionExpression"] == (
        "attribute_not_exists(service_deploy_group)"
    )
    item = put_item.call_args[1]["Item"]
    assert item["service_deploy_group"] == {"S": "fake_service#prod.everything"}
    assert item["created_at_ms"] == {"N": "1791500000123"}
    assert item["thread_ts"] == {"S": "1791500000.123456"}


def test_record_env_var_overrides_table(mock_boto3_client, mock_time, monkeypatch):
    monkeypatch.setenv(
        deploy_slack_threads.TABLE_NAME_ENV_VAR, "deploy-slack-threads-test"
    )
    record({"deploy_slack_threads": TABLE_CONFIG})

    put_item = mock_boto3_client.return_value.put_item
    assert put_item.call_args[1]["TableName"] == "deploy-slack-threads-test"


def test_record_does_nothing_when_not_configured(mock_boto3_client):
    record({})

    mock_boto3_client.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        EndpointConnectionError(
            endpoint_url="https://dynamodb.us-west-2.amazonaws.com"
        ),
        Exception("boom"),
    ],
)
def test_record_swallows_errors(mock_boto3_client, mock_time, error):
    mock_boto3_client.return_value.put_item.side_effect = error

    with mock.patch.object(deploy_slack_threads.log, "warning", autospec=True) as warn:
        record({"deploy_slack_threads": TABLE_CONFIG})

    warn.assert_called_once()
