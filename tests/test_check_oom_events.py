import time

import pytest
from mock import patch
from pysensu_yelp import Status

from paasta_tools.check_oom_events import compose_sensu_status
from paasta_tools.check_oom_events import latest_oom_events
from paasta_tools.check_oom_events import main
from paasta_tools.check_oom_events import oom_events


@pytest.fixture
def scribereader_output():
    time_now = int(time.time())
    return (
        ('{"timestamp": %d, "hostname": "hostname1", "container_id": "baaab5a3a9fa",'
         ' "cluster": "fake_cluster", "service": "fake_service1", '
         '"instance": "fake_instance1", "process_name": "uwsgi"}' % (time_now - 20)),
        ('{"timestamp": %d, "hostname": "hostname2", "container_id": "8dc8b9aeebbe",'
         ' "cluster": "fake_cluster", "service": "fake_service2", '
         '"instance": "fake_instance2", "process_name": "uwsgi"}' % (time_now - 15)),
        ('Non-JSON lines must be ignored.'),
    )


def test_compose_sensu_status_ok():
    assert compose_sensu_status(('service', 'instance'), []) \
        == (Status.OK, 'oom-killer is calm.')


def test_compose_sensu_status_1_process():
    assert compose_sensu_status(
        ('service', 'instance'),
        [('hostname', 'container_id', 'proc_A')],
    ) \
        == (Status.CRITICAL, 'killing 1 process/min (proc_A).')


def test_compose_sensu_status_more_than_1_process():
    assert compose_sensu_status(
        ('service', 'instance'), [
            ('hostname1', 'container_id1', 'proc_A'),
            ('hostname3', 'container_id3', 'proc_C'),
            ('hostname2', 'container_id2', 'proc_B'),
            ('hostname2', 'container_id2', ''),
        ],
    ) == (Status.CRITICAL, 'killing 4 processes/min (proc_A,proc_B,proc_C).')


@patch('paasta_tools.check_oom_events.scribereader', autospec=True)
def test_oom_events(mock_scribereader, scribereader_output):
    mock_scribereader.get_default_scribe_hosts.return_value = [{'host': '', 'port': ''}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    assert len([x for x in oom_events('fake_cluster', 'fake_superregion')]) == 2


@patch('paasta_tools.check_oom_events.scribereader', autospec=True)
def test_latest_oom_events(mock_scribereader, scribereader_output):
    mock_scribereader.get_default_scribe_hosts.return_value = [{'host': '', 'port': ''}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    events = latest_oom_events('fake_cluster', 'fake_superregion')
    assert len(events.get(('fake_service1', 'fake_instance1'), [])) == 1
    assert len(events.get(('fake_service2', 'fake_instance2'), [])) == 1
    assert len(events.get(('fake_service3', 'fake_instance3'), [])) == 0


@patch('paasta_tools.check_oom_events.scribereader', autospec=True)
@patch('paasta_tools.check_oom_events.load_system_paasta_config', autospec=True)
@patch('paasta_tools.check_oom_events.get_services_for_cluster', autospec=True)
@patch('paasta_tools.check_oom_events.send_sensu_event', autospec=True)
@patch('paasta_tools.check_oom_events.get_instance_config', autospec=True)
def test_main(
    mock_get_instance_config, mock_send_sensu_event, mock_get_services_for_cluster,
    mock_load_system_paasta_config, mock_scribereader, scribereader_output,
):
    mock_scribereader.get_default_scribe_hosts.return_value = [{'host': '', 'port': ''}]
    mock_scribereader.get_stream_tailer.return_value = scribereader_output
    mock_get_services_for_cluster.return_value = [
        ('fake_service1', 'fake_instance1'),
        ('fake_service2', 'fake_instance2'),
        ('fake_service3', 'fake_instance3'),
    ]
    main(['', '-s', 'some_superregion', '-d', 'soa_dir'])
    assert mock_send_sensu_event.call_count == 3
