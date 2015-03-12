import contextlib
import json
import mock
from multiprocessing import Queue
from Queue import Empty
import time

from pytest import raises

from paasta_tools.paasta_cli.cmds import logs
from paasta_tools.utils import ANY_CLUSTER
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
    formatted_line = format_log_line(levels[0], ANY_CLUSTER, instance, components[0], line)
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


def test_line_passes_filter_false_when_line_not_valid_json():
    levels = ['fake_level1', 'fake_level2']
    cluster = 'fake_cluster'
    components = ['build', 'deploy']
    line = 'i am definitely not json'
    # component must be legit as well as not in the list of requested
    # components
    assert logs.line_passes_filter(line, levels, components, cluster) is False


def test_scribe_tail_log_everything():
    env = 'fake_env'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    queue = Queue()
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
    cluster = 'fake_cluster'
    instance = 'fake_instance'
    queue = Queue()
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
            cluster,
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
    cluster = 'fake_cluster'
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
                cluster,
                queue,
            )
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


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


def test_prettify_log_line_invalid_json():
    line = "i am not json"
    assert logs.prettify_log_line(line) == "Invalid JSON: %s" % line


def test_prettify_log_line_valid_json_missing_key():
    line = json.dumps({
        "component": "fake_component",
        "oops_i_spelled_timestamp_rong": "1999-09-09",
    })
    actual = logs.prettify_log_line(line)
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
    line = json.dumps(parsed_line)

    actual = logs.prettify_log_line(line)
    expected_timestamp = logs.prettify_timestamp(parsed_line['timestamp'])
    assert expected_timestamp in actual
    assert parsed_line['component'] in actual
    assert parsed_line['message'] in actual
    assert parsed_line['level'] in actual


def test_prettify_log_line_valid_json_level_is_event():
    parsed_line = {
        "message": "fake_message",
        "component": "fake_component",
        "level": "event",
        "cluster": "fake_cluster",
        "instance": "fake_instance",
        "timestamp": "2015-03-12T21:20:04.602002",
    }
    line = json.dumps(parsed_line)

    actual = logs.prettify_log_line(line)
    assert parsed_line['level'] not in actual


def test_tail_paasta_logs_let_threads_be_threads():
    """This test lets tail_paasta_logs() fire off processes to do work. We
    verify that the work was done, basically irrespective of how it was done.

    Because of its nature, this test is potentially prone to flakiness. If this
    becomes a problem, it should move to the integration test suite.
    """
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
            # I hate hate hate sleep in tests. This needs to move to the
            # integration suite since I don't think it will ever be stable
            # enough without the sleep and the sleep makes this test too slow
            # to be a unit test.
            time.sleep(0.05)
        scribe_tail_patch.side_effect = scribe_tail_side_effect

        logs.tail_paasta_logs(service, levels, components, cluster)
        determine_scribereader_envs_patch.assert_called_once_with(components, cluster)
        # NOTE: Assertions about scribe_tail_patch break under multiprocessing.
        # We think this is because the patched scribe_tail's attributes
        # (call_count, call_args, etc.) don't get updated here in the main
        # thread where we can inspect them. (The patched-in code does run,
        # however, since it prints debugging messages.)
        #
        # Instead, we'll rely on what we can see, which is the result of the
        # thread's work deposited in the shared queue.
        assert print_log_patch.call_count == 2
        print_log_patch.assert_any_call('fake log line added for env1', False)
        print_log_patch.assert_any_call('fake log line added for env2', False)


def test_tail_paasta_logs_ctrl_c_in_queue_get():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    cluster = 'fake_cluster'
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
            logs.tail_paasta_logs(service, levels, components, cluster)
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


def test_tail_paasta_logs_ctrl_c_in_is_alive():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    cluster = 'fake_cluster'
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
            logs.tail_paasta_logs(service, levels, components, cluster)
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


def test_tail_paasta_logs_aliveness_check():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    cluster = 'fake_cluster'
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
        fake_process.is_alive.side_effect = [
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
        process_patch.return_value = fake_process
        logs.tail_paasta_logs(service, levels, components, cluster)
        # is_alive returns True for each environment the first time through,
        # then False for one environment the second time through. The loop
        # stops there. Hence the total call_count is 4.
        assert fake_process.is_alive.call_count == 6
        # We only terminate the first thread, which is still alive. We don't
        # terminate the second thread, which was already dead.
        assert fake_process.terminate.call_count == 1


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
