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


def test_line_passes_filter_true():
    levels = ['fake_level1', 'fake_level2']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], cluster, instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, cluster) is True


def test_line_passes_filter_true_when_default_cluster():
    levels = ['fake_level1', 'fake_level2']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    # TODO: Use ANY_CLUSTER when tripy ships the branch that
    # introduces that constant instead of hardcoded 'N/A'
    formatted_line = format_log_line(levels[0], 'N/A', instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, cluster) is True


def test_line_passes_filter_false_when_wrong_level():
    levels = ['fake_level1', 'fake_level2']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line('BOGUS_LEVEL', cluster, instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, cluster) is False


def test_line_passes_filter_false_when_wrong_component():
    levels = ['fake_level1', 'fake_level2']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], cluster, instance, 'monitoring', line)
    assert logs.line_passes_filter(formatted_line, levels, components, cluster) is False


def test_scribe_tail_log_everything():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    queue = Queue.Queue()
    tailer = iter([
        format_log_line(
            levels[0],
            cluster,
            instance,
            'build',
            'level: first. component: build.',
        ),
        format_log_line(
            levels[1],
            cluster,
            instance,
            'deploy',
            'level: second. component: deploy.',
        ),
    ])
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribereader', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.line_passes_filter', autospec=True),
    ) as (
        mock_scribereader,
        mock_line_passes_filter,
    ):
        mock_scribereader.get_env_scribe_host.return_value = ('fake_host', 'fake_port')
        mock_scribereader.get_stream_tailer.return_value = tailer
        mock_line_passes_filter.return_value = True
        logs.scribe_tail(
            env,
            service,
            levels,
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
        assert 'level: first. component: build.' in first_line
        second_line = queue.get_nowait()
        queue.task_done()
        assert 'level: second. component: deploy.' in second_line


def test_scribe_tail_log_nothing():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    queue = Queue.Queue()
    tailer = iter([
        format_log_line(
            levels[0],
            cluster,
            instance,
            'build',
            'level: first. component: build.',
        ),
        format_log_line(
            levels[1],
            cluster,
            instance,
            'deploy',
            'level: second. component: deploy.',
        ),
    ])
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribereader', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.line_passes_filter', autospec=True),
    ) as (
        mock_scribereader,
        mock_line_passes_filter,
    ):
        mock_scribereader.get_env_scribe_host.return_value = ('fake_host', 'fake_port')
        mock_scribereader.get_stream_tailer.return_value = tailer
        mock_line_passes_filter.return_value = False
        logs.scribe_tail(
            env,
            service,
            levels,
            components,
            cluster,
            queue,
        )
        assert queue.qsize() == 0


def test_tail_paasta_logs():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
    ):
        determine_scribereader_envs_patch.return_value = ['env1', 'env2']

        def scribe_tail_side_effect(
            scribe_env,
            service,
            levels,
            components,
            cluster,
            queue,
        ):
            # The print here is just for debugging
            print 'fake log line added for %s' % scribe_env
            queue.put('fake log line added for %s' % scribe_env)
        scribe_tail_patch.side_effect = scribe_tail_side_effect

        logs.tail_paasta_logs(service, levels, components, cluster)

        determine_scribereader_envs_patch.assert_called_once_with(components, cluster)

        scribe_tail_patch.call_count == 2
        scribe_tail_patch.assert_any_call(
            scribe_env='env1',
            service=service,
            levels=levels,
            components=components,
            cluster=cluster,
            queue=mock.ANY,
        )
        scribe_tail_patch.assert_any_call(
            scribe_env='env2',
            service=service,
            levels=levels,
            components=components,
            cluster=cluster,
            queue=mock.ANY,
        )

        assert print_log_patch.call_count == 2
        print "#######################################################"
        print print_log_patch.call_args_list
        print_log_patch.assert_any_call('fake log line added for env1')
        print_log_patch.assert_any_call('fake log line added for env2')


# def test_tail_paasta_logs_extra_thread():
#     """Make sure we correctly handle the case where there are some other random
#     threads hanging around (kazoo? pyrasite?). Thanks to wtimoney for pointing
#     out this possibility.
#     """
#     service = 'fake_service'
#     levels = ['fake_level1', 'fake_level2']
#     components = ['deploy', 'monitoring']
#     cluster = 'fake_cluster'
#     with contextlib.nested(
#         mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
#         mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
#         mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
#         mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
#     ) as (
#         determine_scribereader_envs_patch,
#         scribe_tail_patch,
#         log_patch,
#         print_log_patch,
#     ):
#         determine_scribereader_envs_patch.return_value = ['env1', 'env2']
#
#         logs.tail_paasta_logs(service, levels, components, cluster)
#
#         assert print_log_patch.call_count == 2
#         print "#######################################################"
#         print print_log_patch.call_args_list
#         print_log_patch.assert_any_call('fake log line added for env1')
#         print_log_patch.assert_any_call('fake log line added for env2')


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
