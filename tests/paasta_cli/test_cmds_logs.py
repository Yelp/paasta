import contextlib
import json
import mock
from multiprocessing import Queue
from Queue import Empty

from pytest import raises

from paasta_tools.paasta_cli.cmds import logs
from paasta_tools.utils import ANY_CLUSTER
from paasta_tools.utils import format_log_line
from paasta_tools.utils import get_log_name_for_service
from scribereader.scribereader import StreamTailerSetupError


def test_cluster_to_scribe_env_good():
    actual = logs.cluster_to_scribe_env('mesosstage')
    assert actual == 'env1'


def test_cluster_to_scribe_env_bad():
    with raises(SystemExit) as sys_exit:
        logs.cluster_to_scribe_env('dne')
        assert sys_exit.value.code == 1


def test_line_passes_filter_true():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], clusters[0], instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, clusters) is True


def test_line_passes_filter_true_when_default_cluster():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], ANY_CLUSTER, instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, clusters) is True


def test_line_passes_filter_false_when_wrong_level():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line('BOGUS_LEVEL', clusters[0], instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, clusters) is False


def test_line_passes_filter_false_when_wrong_component():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], clusters[0], instance, 'monitoring', line)
    assert logs.line_passes_filter(formatted_line, levels, components, clusters) is False


def test_line_passes_filter_false_when_wrong_cluster():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], 'BOGUS_CLUSTER', instance, components[0], line)
    assert logs.line_passes_filter(formatted_line, levels, components, clusters) is False


def test_line_passes_filter_false_when_line_not_valid_json():
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    components = ['build', 'deploy']
    line = 'i am definitely not json'
    # component must be legit as well as not in the list of requested
    # components
    assert logs.line_passes_filter(line, levels, components, clusters) is False


def test_scribe_tail_log_everything():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    queue = Queue()
    tailer = iter([
        format_log_line(
            levels[0],
            clusters,
            instance,
            'build',
            'level: first. component: build.',
        ),
        format_log_line(
            levels[1],
            clusters,
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
        mock_scribereader.get_env_scribe_host.return_value = {
            'host': 'fake_host',
            'port': 'fake_port',
        }
        mock_scribereader.get_stream_tailer.return_value = tailer
        mock_line_passes_filter.return_value = True
        logs.scribe_tail(
            env,
            service,
            levels,
            components,
            clusters,
            queue,
        )
        assert mock_scribereader.get_env_scribe_host.call_count == 1
        mock_scribereader.get_stream_tailer.assert_called_once_with(
            get_log_name_for_service(service),
            'fake_host',
            'fake_port',
        )
        assert queue.qsize() == 2
        # Sadly, fetching with a timeout seems to be needed with
        # multiprocessing.Queue (this was not the case with Queue.Queue). It
        # failed 8/10 times with a get_nowait() vs 0/10 times with a 0.1s
        # timeout.
        first_line = queue.get(True, 0.1)
        assert 'level: first. component: build.' in first_line
        second_line = queue.get(True, 0.1)
        assert 'level: second. component: deploy.' in second_line


def test_scribe_tail_log_nothing():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    queue = Queue()
    tailer = iter([
        format_log_line(
            levels[0],
            clusters,
            instance,
            'build',
            'level: first. component: build.',
        ),
        format_log_line(
            levels[1],
            clusters,
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
        mock_scribereader.get_env_scribe_host.return_value = {
            'host': 'fake_host',
            'port': 'fake_port',
        }
        mock_scribereader.get_stream_tailer.return_value = tailer
        mock_line_passes_filter.return_value = False
        logs.scribe_tail(
            env,
            service,
            levels,
            components,
            clusters,
            queue,
        )
        assert queue.qsize() == 0


class FakeKeyboardInterrupt(KeyboardInterrupt):
    """Raising a real KeyboardInterrupt causes pytest to, y'know, stop."""
    pass


def test_scribe_tail_ctrl_c():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    queue = Queue()
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribereader', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.line_passes_filter', autospec=True),
    ) as (
        mock_scribereader,
        mock_line_passes_filter,
    ):
        # There's no reason this method is the one that raises the
        # KeyboardInterrupt. This just happens to be the first convenient place
        # to simulate the user pressing Ctrl-C.
        mock_scribereader.get_env_scribe_host.side_effect = FakeKeyboardInterrupt
        try:
            logs.scribe_tail(
                env,
                service,
                levels,
                components,
                clusters,
                queue,
            )
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


def test_scribe_tail_handles_StreamTailerSetupError():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1']
    components = ['build']
    clusters = ['fake_cluster1']
    queue = Queue()
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribereader', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.line_passes_filter', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
    ) as (
        mock_scribereader,
        mock_line_passes_filter,
        mock_log,
    ):
        mock_scribereader.get_stream_tailer.side_effect = StreamTailerSetupError('bla', 'unused1', 'unused2')
        with raises(StreamTailerSetupError):
            logs.scribe_tail(
                env,
                service,
                levels,
                components,
                clusters,
                queue,
            )
        mock_log.error.assert_any_call('Failed to setup stream tailing for stream_paasta_fake_service in fake_env')


def test_prettify_timestamp():
    timestamp = "2015-03-12T21:20:04.602002"
    actual = logs.prettify_timestamp(timestamp)
    # kwa and I tried to get python to recognize a hardcoded timezone
    # in TZ, even using tzset(), but it ignored us. So we're punting.
    assert "2015-03-12 " in actual
    assert ":20:04" in actual


def test_prettify_component_valid():
    component = "build"
    actual = logs.prettify_component(component)
    assert component in actual
    assert "UNPRETTIFIABLE COMPONENT" not in actual


def test_prettify_component_invalid():
    component = "non-existent component"
    actual = logs.prettify_component(component)
    assert component in actual
    assert "UNPRETTIFIABLE COMPONENT" in actual


def test_prettify_level_more_than_one_requested_levels():
    level = 'fake_level'
    requested_levels = ['fake_requested_level', 'fake_requested_level2']
    assert level in logs.prettify_level(level, requested_levels)


def test_prettify_level_less_than_or_equal_to_one_requested_levels():
    level = 'fake_level'
    requested_levels = []
    assert level not in logs.prettify_level(level, requested_levels)


def test_prettify_log_line_invalid_json():
    line = "i am not json"
    levels = []
    assert logs.prettify_log_line(line, levels) == "Invalid JSON: %s" % line


def test_prettify_log_line_valid_json_missing_key():
    line = json.dumps({
        "component": "fake_component",
        "oops_i_spelled_timestamp_rong": "1999-09-09",
    })
    levels = []
    actual = logs.prettify_log_line(line, levels)
    assert "JSON missing keys: %s" % line in actual


def test_prettify_log_line_valid_json():
    parsed_line = {
        "message": "fake_message",
        "component": "fake_component",
        "level": "fake_level",
        "cluster": "fake_cluster",
        "instance": "fake_instance",
        "timestamp": "2015-03-12T21:20:04.602002",
    }
    requested_levels = ['fake_requested_level1', 'fake_requested_level2']
    line = json.dumps(parsed_line)

    actual = logs.prettify_log_line(line, requested_levels)
    expected_timestamp = logs.prettify_timestamp(parsed_line['timestamp'])
    assert expected_timestamp in actual
    assert parsed_line['component'] in actual
    assert parsed_line['cluster'] in actual
    assert parsed_line['instance'] in actual
    assert parsed_line['level'] in actual
    assert parsed_line['message'] in actual


def test_prettify_log_line_valid_json_requested_level_is_only_event():
    requested_levels = ['fake_requested_level1']
    parsed_line = {
        "message": "fake_message",
        "component": "fake_component",
        "level": "event",
        "cluster": "fake_cluster",
        "instance": "fake_instance",
        "timestamp": "2015-03-12T21:20:04.602002",
    }
    line = json.dumps(parsed_line)

    actual = logs.prettify_log_line(line, requested_levels)
    assert parsed_line['level'] not in actual


def test_tail_paasta_logs_ctrl_c_in_queue_get():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    clusters = ['fake_cluster1', 'fake_cluster2']
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Process', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
    ):
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = FakeKeyboardInterrupt
        queue_patch.return_value = fake_queue
        try:
            logs.tail_paasta_logs(service, levels, components, clusters)
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


def test_tail_paasta_logs_ctrl_c_in_is_alive():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    clusters = ['fake_cluster1', 'fake_cluster2']
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Process', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
    ):
        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        fake_process = mock.MagicMock()
        fake_process.is_alive.side_effect = FakeKeyboardInterrupt
        process_patch.return_value = fake_process
        try:
            logs.tail_paasta_logs(service, levels, components, clusters)
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


def test_tail_paasta_logs_aliveness_check():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    clusters = ['fake_cluster1', 'fake_cluster2']
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Process', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
    ):
        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        fake_process = mock.MagicMock()
        is_alive_responses = [
            # First time: simulate both threads being alive.
            True, True,
            # Second time: simulate first thread is alive but second thread is now dead.
            True, False,
            # This gets us into the kill stanza, which calls is_alive() on each
            # thread again. We'll recycle our answers from the previous calls
            # to is_alive() where the first thread is alive but the second
            # thread is dead.
            True, False,
        ]
        fake_process.is_alive.side_effect = is_alive_responses
        process_patch.return_value = fake_process
        logs.tail_paasta_logs(service, levels, components, clusters)
        # is_alive() should be called on all the values we painstakingly provided above.
        assert fake_process.is_alive.call_count == len(is_alive_responses)
        # We only terminate the first thread, which is still alive. We don't
        # terminate the second thread, which was already dead.
        assert fake_process.terminate.call_count == 1


def test_tail_paasta_logs_empty_clusters():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    clusters = []
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.Process', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
    ):
        determine_scribereader_envs_patch.return_value = []
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        logs.tail_paasta_logs(service, levels, components, clusters)
        assert process_patch.call_count == 0
        assert print_log_patch.call_count == 0


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
