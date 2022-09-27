import time

import mock
import pytest
from pysensu_yelp import Status

from paasta_tools.check_oom_events import compose_sensu_status
from paasta_tools.check_oom_events import latest_oom_events
from paasta_tools.check_oom_events import main
from paasta_tools.check_oom_events import read_oom_events_from_scribe


@pytest.fixture(autouse=True)
def mock_scribereader():
    with mock.patch(
        "paasta_tools.check_oom_events.scribereader",
        autospec=True,
    ) as m:
        yield m


@pytest.fixture
def scribereader_output():
    time_now = int(time.time())
    return (
        (
            '{"timestamp": %d, "hostname": "hostname1", "container_id": "baaab5a3a9fa",'
            ' "cluster": "fake_cluster", "service": "fake_service1", '
            '"instance": "fake_instance1", "process_name": "uwsgi"}' % (time_now - 20)
        ),
        #  Same container, different process
        (
            '{"timestamp": %d, "hostname": "hostname1", "container_id": "baaab5a3a9fa",'
            ' "cluster": "fake_cluster", "service": "fake_service1", '
            '"instance": "fake_instance1", "process_name": "python"}' % (time_now - 20)
        ),
        (
            '{"timestamp": %d, "hostname": "hostname2", "container_id": "8dc8b9aeebbe",'
            ' "cluster": "fake_cluster", "service": "fake_service2", '
            '"instance": "fake_instance2", "process_name": "uwsgi"}' % (time_now - 15)
        ),
        (
            '{"timestamp": %d, "hostname": "hostname2", "container_id": "7dc8b9ffffff",'
            ' "cluster": "fake_cluster", "service": "fake_service2", '
            '"instance": "fake_instance2", "process_name": "uwsgi"}' % (time_now - 14)
        ),
        ("Non-JSON lines must be ignored."),
    )


@pytest.fixture
def instance_config():
    config = mock.Mock()
    config.instance = "fake_instance"
    config.service = "fake_service"
    return config


@pytest.fixture(autouse=True)
def mock_load_system_paasta_config():
    with mock.patch(
        "paasta_tools.check_oom_events.load_system_paasta_config",
        autospec=True,
    ) as mock_load:
        mock_load.return_value.get_cluster.return_value = "fake_cluster"
        mock_load.return_value.get_log_reader.return_value = {
            "options": {"cluster_map": {"fake_cluster": "fake_scribe_env"}},
        }
        yield mock_load


@pytest.fixture(autouse=True)
def mock_scribe_env_to_locations():
    with mock.patch(
        "paasta_tools.check_oom_events.scribe_env_to_locations",
        autospec=True,
    ) as m:
        m.return_value = {
            "ecosystem": "an_ecosystem",
            "region": "a_region",
            "superregion": "a_superregion",
        }
        yield m


def test_compose_sensu_status_ok(instance_config):
    assert compose_sensu_status(
        instance=instance_config,
        oom_events=[],
        is_check_enabled=True,
        alert_threshold=1,
        check_interval=1,
    ) == (
        Status.OK,
        "No oom events for fake_service.fake_instance in the last 1 minute(s).",
    )


def test_compose_sensu_status_unknown(instance_config):
    assert compose_sensu_status(
        instance=instance_config,
        oom_events=[],
        is_check_enabled=False,
        alert_threshold=1,
        check_interval=1,
    ) == (Status.OK, "This check is disabled for fake_service.fake_instance.")


def test_compose_sensu_status_not_ok(instance_config):
    assert compose_sensu_status(
        instance=instance_config,
        oom_events={"container_id1", "container_id2", "container_id3"},
        is_check_enabled=True,
        alert_threshold=2,
        check_interval=1,
    ) == (
        Status.CRITICAL,
        "The Out Of Memory killer killed processes for fake_service.fake_instance"
        " in the last 1 minute(s).",
    )


def test_compose_sensu_status_below_threshold(instance_config):
    assert (
        compose_sensu_status(
            instance=instance_config,
            oom_events={"container_id1", "container_id2", "container_id3"},
            is_check_enabled=True,
            alert_threshold=5,
            check_interval=1,
        )
        is None
    )


def test_read_oom_events_from_scribe(
    mock_scribereader,
    scribereader_output,
    mock_scribe_env_to_locations,
):
    mock_scribereader.get_tail_host_and_port.return_value = "localhost", 12345
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    assert (
        len(
            [x for x in read_oom_events_from_scribe("fake_cluster", "fake_superregion")]
        )
        == 4
    )
    assert mock_scribe_env_to_locations.call_args_list == [
        mock.call("fake_scribe_env"),
    ]


def test_latest_oom_events(mock_scribereader, scribereader_output):
    mock_scribereader.get_tail_host_and_port.return_value = "localhost", 12345
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    events = latest_oom_events("fake_cluster", "fake_superregion")
    # Events from the same container count as one
    assert len(events.get(("fake_service1", "fake_instance1"), [])) == 1
    assert len(events.get(("fake_service2", "fake_instance2"), [])) == 2
    assert len(events.get(("fake_service3", "fake_instance3"), [])) == 0


def test_latest_oom_events_interval(mock_scribereader, scribereader_output):
    mock_scribereader.get_tail_host_and_port.return_value = "localhost", 12345
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    events = latest_oom_events("fake_cluster", "fake_superregion", interval=10)
    # Scribereader mocks are more than 10 seconds ago, so no events should be returned
    assert len(events) == 0


@mock.patch("paasta_tools.check_oom_events.latest_oom_events", autospec=True)
@mock.patch("paasta_tools.check_oom_events.get_services_for_cluster", autospec=True)
@mock.patch("paasta_tools.check_oom_events.send_sensu_event", autospec=True)
@mock.patch("paasta_tools.check_oom_events.get_instance_config", autospec=True)
def test_main(
    mock_get_instance_config,
    mock_send_sensu_event,
    mock_get_services_for_cluster,
    mock_latest_oom_events,
    scribereader_output,
):
    mock_get_services_for_cluster.return_value = [
        ("fake_service1", "fake_instance1"),
        ("fake_service2", "fake_instance2"),
        ("fake_service3", "fake_instance3"),
    ]
    main(["", "-s", "some_superregion", "-d", "soa_dir", "--check-interval", "3"])
    assert mock_send_sensu_event.call_count == 3
    mock_latest_oom_events.assert_called_once_with(
        cluster="fake_cluster",
        superregion="some_superregion",
        interval=180,
    )
