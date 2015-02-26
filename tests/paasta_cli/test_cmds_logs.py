import contextlib
import mock
from pytest import raises
import Queue

from paasta_tools.paasta_cli.cmds import logs
from paasta_tools.utils import format_log_line
from paasta_tools.utils import get_log_name_for_service


def test_cluster_to_scribe_env_good():
    actual = logs.cluster_to_scribe_env('mesosstage')
    assert actual == 'env1'


def test_cluster_to_scribe_env_bad():
    with raises(SystemExit) as sys_exit:
        logs.cluster_to_scribe_env('dne')
        assert sys_exit.value.code == 1


def test_scribe_tail():
    env = 'fake_env'
    service = 'fake_service'
    components = ['build', 'deploy']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    queue = Queue.Queue()
    tailer = iter([
        format_log_line(
            'event',
            cluster,
            instance,
            'build',
            'level: event. component: build.',
        ),
        format_log_line(
            'debug',
            cluster,
            instance,
            'deploy',
            'level: debug. component: deploy.',
        ),
    ])
    with mock.patch('paasta_tools.paasta_cli.cmds.logs.scribereader', autospec=True) as mock_scribereader:
        mock_scribereader.get_env_scribe_host.return_value = ('fake_host', 'fake_port')
        mock_scribereader.get_stream_tailer.return_value = tailer
        logs.scribe_tail(
            env,
            service,
            components,
            cluster,
            queue,
        )
        assert mock_scribereader.get_env_scribe_host.call_count == 1
        mock_scribereader.get_stream_tailer.assert_called_once_with(
            get_log_name_for_service(service),
            'fake_host',
            'fake_port',
        )
        assert queue.qsize() == 2
        first_line = queue.get_nowait()
        queue.task_done()
        assert 'level: event. component: build.' in first_line
        second_line = queue.get_nowait()
        queue.task_done()
        assert 'level: debug. component: deploy.' in second_line


def test_tail_paasta_logs():
    service = 'fake_service'
    components = ['deploy', 'monitoring']
    cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
    ):
        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        logs.tail_paasta_logs(service, components, cluster)
        determine_scribereader_envs_patch.assert_called_once_with(components, cluster)
        scribe_tail_patch.assert_any_call('env1', service, components, cluster, mock.ANY)
        scribe_tail_patch.assert_any_call('env2', service, components, cluster, mock.ANY)
        scribe_tail_patch.call_count == 2


def test_determine_scribereader_envs():
    cluster = 'fake_cluster'
    components = ['build', 'monitoring']
    with mock.patch(
        'paasta_tools.paasta_cli.cmds.logs.cluster_to_scribe_env',
        autospec=True
    ) as cluster_to_scribe_env_patch:
        cluster_to_scribe_env_patch.return_value = 'fake_scribe_env'
        actual = logs.determine_scribereader_envs(components, cluster)
        cluster_to_scribe_env_patch.assert_called_with(cluster)
        assert actual == set(['env1', 'fake_scribe_env'])


def test_prefix():
    actual = logs.prefix('TEST STRING', 'deploy')
    assert 'TEST STRING' in actual
