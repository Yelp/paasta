import time

import pytest
from mock import Mock
from mock import patch
from pysensu_yelp import Status

from paasta_tools.check_oom_events import compose_sensu_status
from paasta_tools.check_oom_events import latest_oom_events
from paasta_tools.check_oom_events import main
from paasta_tools.check_oom_events import OOMEvent
from paasta_tools.check_oom_events import read_oom_events_from_scribe


@pytest.fixture
def scribereader_output():
    time_now = int(time.time())
    return (
        (
            '{"timestamp": %d, "hostname": "hostname1", "container_id": "baaab5a3a9fa",'
            ' "cluster": "fake_cluster", "service": "fake_service1", '
            '"instance": "fake_instance1", "process_name": "uwsgi"}' % (time_now - 20)
        ),
        (
            '{"timestamp": %d, "hostname": "hostname2", "container_id": "8dc8b9aeebbe",'
            ' "cluster": "fake_cluster", "service": "fake_service2", '
            '"instance": "fake_instance2", "process_name": "uwsgi"}' % (time_now - 15)
        ),
        ("Non-JSON lines must be ignored."),
    )


@pytest.fixture
def instance_config():
    config = Mock()
    config.instance = "fake_instance"
    config.service = "fake_service"
    return config


def test_compose_sensu_status_ok(instance_config):
    assert compose_sensu_status(
        instance=instance_config, oom_events=[], is_check_enabled=True
    ) == (Status.OK, "No oom events for fake_service.fake_instance in the last minute.")


def test_compose_sensu_status_unknown(instance_config):
    assert compose_sensu_status(
        instance=instance_config, oom_events=[], is_check_enabled=False
    ) == (Status.OK, "This check is disabled for fake_service.fake_instance.")


def test_compose_sensu_status_not_ok(instance_config):
    assert compose_sensu_status(
        instance=instance_config,
        oom_events=[
            OOMEvent("hostname1", "container_id1", "proc_A"),
            OOMEvent("hostname3", "container_id3", "proc_C"),
            OOMEvent("hostname2", "container_id2", "proc_B"),
            OOMEvent("hostname2", "container_id2", ""),
        ],
        is_check_enabled=True,
    ) == (
        Status.CRITICAL,
        "The Out Of Memory killer killed 4 processes (proc_A,proc_B,proc_C)"
        " in the last minute in fake_service.fake_instance containers.",
    )


@patch("paasta_tools.check_oom_events.scribereader", autospec=True)
def test_read_oom_events_from_scribe(mock_scribereader, scribereader_output):
    mock_scribereader.get_default_scribe_hosts.return_value = [{"host": "", "port": ""}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    assert (
        len(
            [x for x in read_oom_events_from_scribe("fake_cluster", "fake_superregion")]
        )
        == 2
    )


@patch("paasta_tools.check_oom_events.scribereader", autospec=True)
def test_latest_oom_events(mock_scribereader, scribereader_output):
    mock_scribereader.get_default_scribe_hosts.return_value = [{"host": "", "port": ""}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    events = latest_oom_events("fake_cluster", "fake_superregion")
    assert len(events.get(("fake_service1", "fake_instance1"), [])) == 1
    assert len(events.get(("fake_service2", "fake_instance2"), [])) == 1
    assert len(events.get(("fake_service3", "fake_instance3"), [])) == 0


@patch("paasta_tools.check_oom_events.scribereader", autospec=True)
@patch("paasta_tools.check_oom_events.load_system_paasta_config", autospec=True)
@patch("paasta_tools.check_oom_events.get_services_for_cluster", autospec=True)
@patch("paasta_tools.check_oom_events.send_sensu_event", autospec=True)
@patch("paasta_tools.check_oom_events.get_instance_config", autospec=True)
def test_main(
    mock_get_instance_config,
    mock_send_sensu_event,
    mock_get_services_for_cluster,
    mock_load_system_paasta_config,
    mock_scribereader,
    scribereader_output,
):
    mock_scribereader.get_default_scribe_hosts.return_value = [{"host": "", "port": ""}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    mock_get_services_for_cluster.return_value = [
        ("fake_service1", "fake_instance1"),
        ("fake_service2", "fake_instance2"),
        ("fake_service3", "fake_instance3"),
    ]
    main(["", "-s", "some_superregion", "-d", "soa_dir"])
    assert mock_send_sensu_event.call_count == 3
