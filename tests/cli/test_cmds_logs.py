# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import contextlib
import datetime
import json
from multiprocessing import Queue
from Queue import Empty

import isodate
import mock
import pytest
from pytest import raises

try:
    from paasta_tools.cli.cmds import logs
except ImportError:
    pass
from paasta_tools.cli.cli import parse_args
from paasta_tools.utils import ANY_CLUSTER
from paasta_tools.utils import format_log_line
try:
    from scribereader.scribereader import StreamTailerSetupError
    scribereader_available = True
except ImportError:
    scribereader_available = False
    pass


def test_cluster_to_scribe_env_good():
    with mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True):
        scribe_log_reader = logs.ScribeLogReader(cluster_map={'mesosstage': 'env1'})
        actual = scribe_log_reader.cluster_to_scribe_env('mesosstage')
        assert actual == 'env1'


def test_cluster_to_scribe_env_bad():
    with mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True):
        scribe_log_reader = logs.ScribeLogReader(cluster_map={})
        with raises(SystemExit) as sys_exit:
            scribe_log_reader.cluster_to_scribe_env('dne')
            assert sys_exit.value.code == 1


def test_check_timestamp_in_range_with_none_arguments():
    assert logs.check_timestamp_in_range(timestamp=None, start_time=None, end_time=None) is True
    assert logs.check_timestamp_in_range(datetime.datetime.utcnow(), None, None) is True


def test_check_timestamp_in_range_false():
    timestamp = datetime.datetime.utcnow()
    start_time, end_time = logs.generate_start_end_time("10m", "5m")

    assert logs.check_timestamp_in_range(timestamp, start_time, end_time) is False


def test_check_timestamp_in_range_true():
    timestamp = isodate.parse_datetime("2016-06-07T23:46:03+00:00")

    start_time = isodate.parse_datetime("2016-06-07T23:40:03+00:00")
    end_time = isodate.parse_datetime("2016-06-07T23:50:03+00:00")

    assert logs.check_timestamp_in_range(timestamp, start_time, end_time) is True


def test_paasta_log_line_passes_filter_true():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], clusters[0], service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is True


def test_paasta_log_line_passes_filter_true_when_default_cluster():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], ANY_CLUSTER, service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is True


def test_paasta_log_line_passes_filter_true_when_default_instance():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = None
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], ANY_CLUSTER, service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is True


def test_paasta_log_line_passes_filter_false_when_wrong_level():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line('BOGUS_LEVEL', clusters[0], service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is False


def test_paasta_log_line_passes_filter_false_when_wrong_component():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], clusters[0], service, instance, 'monitoring', line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is False


def test_paasta_log_line_passes_filter_false_when_wrong_cluster():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], 'BOGUS_CLUSTER', service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is False


def test_paasta_log_line_passes_filter_false_when_wrong_instance():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'non-existant_instance'
    instances = ['fake_instance']
    components = ['build', 'deploy']
    line = 'fake_line'
    # component must be legit as well as not in the list of requested
    # components
    formatted_line = format_log_line(levels[0], 'BOGUS_CLUSTER', service, instance, components[0], line)
    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters, instances) is False


def test_paasta_log_line_passes_filter_false_when_line_not_valid_json():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instances = ['fake_instance']
    components = ['build', 'deploy']
    line = 'i am definitely not json'
    # component must be legit as well as not in the list of requested
    # components
    assert logs.paasta_log_line_passes_filter(line, levels, service, components, clusters, instances) is False


def test_paasta_log_line_passes_filter_true_when_valid_time():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], clusters[0], service, instance, components[0], line,
                                     timestamp="2016-06-07T23:46:03+00:00")

    start_time = isodate.parse_datetime("2016-06-07T23:40:03+00:00")
    end_time = isodate.parse_datetime("2016-06-07T23:50:03+00:00")

    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters,
                                              instances, start_time=start_time, end_time=end_time) is True


def test_paasta_log_line_passes_filter_false_when_invalid_time():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    instances = [instance]
    components = ['build', 'deploy']
    line = 'fake_line'
    formatted_line = format_log_line(levels[0], clusters[0], service, instance, components[0], line,
                                     timestamp=isodate.datetime_isoformat(datetime.datetime.utcnow()))

    start_time, end_time = logs.generate_start_end_time(from_string="5m", to_string="3m")

    assert logs.paasta_log_line_passes_filter(formatted_line, levels, service, components, clusters,
                                              instances, start_time=start_time, end_time=end_time) is False


def test_marathon_log_line_passes_filter_true_when_service_name_in_string():
    service = 'fake_service'
    levels = []
    components = []
    clusters = []
    instances = []
    line = format_log_line(
        'fake_level',
        clusters,
        service,
        'fake_instance',
        'marathon',
        'fake message with service name %s' % service,
    )
    with mock.patch('paasta_tools.cli.cmds.logs.format_job_id', autospec=True) as format_job_id_patch:
        format_job_id_patch.return_value = service
        assert logs.marathon_log_line_passes_filter(line, levels, service, components, clusters, instances)


def test_marathon_log_line_passes_filter_false_when_service_name_missing():
    service = 'fake_service'
    levels = []
    components = []
    clusters = []
    instances = []
    line = format_log_line(
        'fake_level',
        clusters,
        service,
        'fake_instance',
        'marathon',
        'fake message without service name',
    )

    with mock.patch('paasta_tools.cli.cmds.logs.format_job_id', autospec=True) as format_job_id_patch:
        format_job_id_patch.return_value = service
        assert not logs.marathon_log_line_passes_filter(line, levels, service, components, clusters, instances)


def test_marathon_log_line_passes_filter_fails_invalid_json():
    assert not logs.marathon_log_line_passes_filter("{ abcd }", None, None, None, None, None)


def test_chronos_log_line_passes_filter_fails_invalid_json():
    assert not logs.chronos_log_line_passes_filter("{ abcd }", None, None, None, None, None)


def test_chronos_log_line_passes_filter_true_when_service_name_in_string():
    service = 'fake_service'
    levels = []
    components = []
    clusters = []
    instances = []
    line = format_log_line(
        'fake_level',
        clusters,
        service,
        'fake_instance',
        'chronos',
        'fake message with service name %s' % service,
    )
    with mock.patch('paasta_tools.chronos_tools.compose_job_id', autospec=True) as format_job_id_patch:
        format_job_id_patch.return_value = service
        assert logs.chronos_log_line_passes_filter(line, levels, service, components, clusters, instances)


def test_chronos_log_line_passes_filter_false_when_service_name_missing():
    service = 'fake_service'
    levels = []
    components = []
    clusters = []
    instances = []
    line = format_log_line(
        'fake_level',
        clusters,
        service,
        'fake_instance',
        'chronos',
        'fake message without service name',
    )

    with mock.patch('paasta_tools.chronos_tools.compose_job_id', autospec=True) as format_job_id_patch:
        format_job_id_patch.return_value = service
        assert not logs.chronos_log_line_passes_filter(line, levels, service, components, clusters, instances)


def test_extract_utc_timestamp_from_log_line_ok():
    fake_timestamp = '2015-07-22T10:38:46-07:00'
    fake_utc_timestamp = isodate.parse_datetime('2015-07-22T17:38:46.000000')

    line = '%s this is a fake syslog test message' % fake_timestamp
    assert logs.extract_utc_timestamp_from_log_line(line) == fake_utc_timestamp


def test_extract_utc_timestamp_from_log_line_when_missing_date():
    line = 'this is a fake invalid syslog message'
    assert not logs.extract_utc_timestamp_from_log_line(line)


def test_extract_utc_timestamp_from_log_line_when_invalid_date_format():
    line = 'Jul 22 10:39:08 this is a fake invalid syslog message'
    assert not logs.extract_utc_timestamp_from_log_line(line)


def test_parse_marathon_log_line_fail():
    assert '' == logs.parse_marathon_log_line("fake timestamp", None, None)


def test_parse_marathon_log_line_ok():
    fake_timestamp = '2015-07-22T10:38:46-07:00'
    fake_utc_timestamp = '2015-07-22T17:38:46.000000'
    fake_service = 'fake_service'

    line = '%s this is a fake syslog test message' % fake_timestamp
    clusters = ['fake_cluster']
    expected = json.dumps({
        'timestamp': fake_utc_timestamp,
        'component': 'marathon',
        'cluster': clusters[0],
        'service': fake_service,
        'instance': 'ALL',
        'level': 'event',
        'message': line
    })
    assert sorted(logs.parse_marathon_log_line(line, clusters, fake_service)) == sorted(expected)


def test_parse_chronos_log_line_fail():
    assert '' == logs.parse_chronos_log_line("fake timestamp", None, None)


def test_parse_chronos_log_line_ok():
    fake_timestamp = '2015-07-22T10:38:46-07:00'
    fake_utc_timestamp = '2015-07-22T17:38:46.000000'
    fake_service = 'fake_service'

    line = '%s this is a fake syslog test message' % fake_timestamp
    clusters = ['fake_cluster']
    expected = json.dumps({
        'timestamp': fake_utc_timestamp,
        'component': 'chronos',
        'cluster': clusters[0],
        'service': fake_service,
        'instance': 'ALL',
        'level': 'event',
        'message': line
    })
    assert sorted(logs.parse_chronos_log_line(line, clusters, fake_service)) == sorted(expected)


@pytest.mark.skipif(not scribereader_available, reason='scribereader not available')
def test_scribe_tail_log_everything():
    env = 'fake_env'
    stream_name = 'fake_stream'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    queue = Queue()
    filter_fn = mock.Mock(return_value=True)

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
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        mock_scribereader,
    ):
        mock_scribereader.get_env_scribe_host.return_value = {
            'host': 'fake_host',
            'port': 'fake_port',
        }
        mock_scribereader.get_stream_tailer.return_value = tailer
        logs.scribe_tail(
            env,
            stream_name,
            service,
            levels,
            components,
            clusters,
            queue,
            filter_fn
        )
        assert mock_scribereader.get_env_scribe_host.call_count == 1
        mock_scribereader.get_stream_tailer.assert_called_once_with(
            stream_name,
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


@pytest.mark.skipif(not scribereader_available, reason='scribereader not available')
def test_scribe_tail_log_nothing():
    env = 'fake_env'
    stream_name = 'fake_stream'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instance = 'fake_instance'
    queue = Queue()
    filter_fn = mock.Mock(return_value=False)

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
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        mock_scribereader,
    ):
        mock_scribereader.get_env_scribe_host.return_value = {
            'host': 'fake_host',
            'port': 'fake_port',
        }
        mock_scribereader.get_stream_tailer.return_value = tailer
        logs.scribe_tail(
            env,
            stream_name,
            service,
            levels,
            components,
            clusters,
            queue,
            filter_fn,
        )
        assert queue.qsize() == 0


class FakeKeyboardInterrupt(KeyboardInterrupt):

    """Raising a real KeyboardInterrupt causes pytest to, y'know, stop."""
    pass


@pytest.mark.skipif(not scribereader_available, reason='scribereader not available')
def test_scribe_tail_ctrl_c():
    env = 'fake_env'
    stream_name = 'fake_stream'
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['build', 'deploy']
    clusters = ['fake_cluster1', 'fake_cluster2']
    queue = Queue()
    filter_fn = mock.Mock(return_value=True)

    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        mock_scribereader,
    ):
        # There's no reason this method is the one that raises the
        # KeyboardInterrupt. This just happens to be the first convenient place
        # to simulate the user pressing Ctrl-C.
        mock_scribereader.get_env_scribe_host.side_effect = FakeKeyboardInterrupt
        try:
            logs.scribe_tail(
                env,
                stream_name,
                service,
                levels,
                components,
                clusters,
                queue,
                filter_fn,
            )
        # We have to catch this ourselves otherwise it will fool pytest too!
        except FakeKeyboardInterrupt:
            raise Exception('The code under test failed to catch a (fake) KeyboardInterrupt!')
        # If we made it here, KeyboardInterrupt was not raised and this test
        # was successful.


@pytest.mark.skipif(not scribereader_available, reason='scribereader not available')
def test_scribe_tail_handles_StreamTailerSetupError():
    env = 'fake_env'
    stream_name = 'fake_stream'
    service = 'fake_service'
    levels = ['fake_level1']
    components = ['build']
    clusters = ['fake_cluster1']
    queue = Queue()
    filter_fn = mock.Mock(return_value=True)

    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
    ) as (
        mock_scribereader,
        mock_log,
    ):
        mock_scribereader.get_stream_tailer.side_effect = StreamTailerSetupError('bla', 'unused1', 'unused2')
        with raises(StreamTailerSetupError):
            logs.scribe_tail(
                env,
                stream_name,
                service,
                levels,
                components,
                clusters,
                queue,
                filter_fn,
            )
        mock_log.error.assert_any_call('Failed to setup stream tailing for %s in fake_env' % stream_name)


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


def test_scribereader_run_code_over_scribe_envs():
    clusters = ['fake_cluster1', 'fake_cluster2']
    components = ['build', 'deploy', 'monitoring', 'marathon', 'chronos', 'stdout', 'stderr']

    callback = mock.MagicMock()

    with mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True) \
            as determine_scribereader_envs_patch, \
            mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True):

        envs = ['env1', 'env2']
        determine_scribereader_envs_patch.return_value = envs
        logs.ScribeLogReader(cluster_map={}).run_code_over_scribe_envs(clusters, components, callback)

        # See comment in test_scribereader_print_last_n_logs for where this figure comes from
        assert callback.call_count == 14


def test_scribereader_print_last_n_logs():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instances = ['main']
    components = ['build', 'deploy', 'monitoring', 'marathon', 'chronos', 'stdout', 'stderr']

    with mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True) as mock_scribereader, \
            mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True) \
            as determine_scribereader_envs_patch:

        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        fake_iter = mock.MagicMock()
        fake_iter.__iter__.return_value = ["""{"cluster":"fake_cluster1","component":"stderr","instance":"main",
                                           "level":"debug","message":"testing",
                                           "timestamp":"2016-06-08T06:31:52.706609135Z"}"""] * 100
        mock_scribereader.get_stream_tailer.return_value = fake_iter

        logs.ScribeLogReader(cluster_map={}).print_last_n_logs(service, 100, levels, components, clusters,
                                                               instances, raw_mode=False)

        # one call per component per environment except marathon and chronos which run 1/env/cluster
        # Defaults:
        #    env1, env2                = 2
        # marathon:
        #    env1: cluster1 cluster2   = 2
        #    env2: cluster1 cluster2   = 2
        # chronos:
        #    env1: cluster1 cluster2   = 2
        #    env2: cluster1 cluster2   = 2
        # stdout:
        #    env1, env2                = 2
        # stderr:
        #    env1, env2                = 2
        assert mock_scribereader.get_stream_tailer.call_count == 14


def test_scribereader_print_logs_by_time():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instances = ['main']
    components = ['build', 'deploy', 'monitoring', 'marathon', 'chronos', 'stdout', 'stderr']

    with mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True) as mock_scribereader, \
            mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True) \
            as determine_scribereader_envs_patch:

        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        fake_iter = mock.MagicMock()
        fake_iter.__iter__.return_value = ["""{"cluster":"fake_cluster1","component":"stderr","instance":"main",
                                           "level":"debug","message":"testing",
                                           "timestamp":"2016-06-08T06:31:52.706609135Z"}"""] * 100
        mock_scribereader.get_stream_tailer.return_value = fake_iter
        mock_scribereader.get_stream_reader.return_value = fake_iter

        start_time, end_time = logs.generate_start_end_time()
        logs.ScribeLogReader(cluster_map={}).print_logs_by_time(service, start_time, end_time,
                                                                levels, components, clusters,
                                                                instances, raw_mode=False)

        # Please see comment in test_scribereader_print_last_n_logs for where this number comes from
        assert mock_scribereader.get_stream_tailer.call_count == 14

        start_time, end_time = logs.generate_start_end_time("3d", "2d")
        logs.ScribeLogReader(cluster_map={}).print_logs_by_time(service, start_time, end_time,
                                                                levels, components, clusters,
                                                                instances, raw_mode=False)

        # Please see comment in test_scribereader_print_last_n_logs for where this number comes from
        assert mock_scribereader.get_stream_reader.call_count == 14


def test_tail_paasta_logs_ctrl_c_in_queue_get():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring', 'chronos', 'stdout', 'stderr']
    clusters = ['fake_cluster1', 'fake_cluster2']
    instances = ['fake_instance1', 'fake_instance2']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        mock_scribereader,
    ):
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = FakeKeyboardInterrupt
        queue_patch.return_value = fake_queue
        try:
            logs.ScribeLogReader(cluster_map={}).tail_logs(service, levels, components, clusters, instances)
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
    instances = ['fake_instance1', 'fake_instance2']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        mock_scribereader,
    ):
        determine_scribereader_envs_patch.return_value = ['env1', 'env2']
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        fake_process = mock.MagicMock()
        fake_process.is_alive.side_effect = FakeKeyboardInterrupt
        process_patch.return_value = fake_process
        scribe_log_reader = logs.ScribeLogReader(cluster_map={'env1': 'env1', 'env2': 'env2'})
        try:
            scribe_log_reader.tail_logs(service, levels, components, clusters, instances)
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
    instances = ['fake_instance1', 'fake_instance2']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        mock_scribereader,
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
        scribe_log_reader = logs.ScribeLogReader(cluster_map={'env1': 'env1', 'env2': 'env2'})
        scribe_log_reader.tail_logs(service, levels, components, clusters, instances)
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
    instances = ['fake_instance']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        mock_scribereader,
    ):
        determine_scribereader_envs_patch.return_value = []
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        logs.ScribeLogReader(cluster_map={}).tail_logs(service, levels, components, clusters, instances)
        assert process_patch.call_count == 0
        assert print_log_patch.call_count == 0


def test_tail_paasta_logs_empty_instances():
    service = 'fake_service'
    levels = ['fake_level1', 'fake_level2']
    components = ['deploy', 'monitoring']
    clusters = ['fake_cluster']
    instances = []
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        mock_scribereader,
    ):
        determine_scribereader_envs_patch.return_value = []
        fake_queue = mock.MagicMock(spec_set=Queue())
        fake_queue.get.side_effect = Empty
        queue_patch.return_value = fake_queue
        logs.ScribeLogReader(cluster_map={}).tail_logs(service, levels, components, clusters, instances)
        assert process_patch.call_count == 0
        assert print_log_patch.call_count == 0


def test_tail_paasta_logs_marathon():
    service = 'fake_service'
    clusters = ['fake_cluster']
    instances = ['fake_instance']
    levels = ['fake_level1', 'fake_level2']
    components = ['marathon']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Queue', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.Process', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.parse_marathon_log_line', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.marathon_log_line_passes_filter', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        print_log_patch,
        queue_patch,
        process_patch,
        parse_marathon_log_line_patch,
        marathon_log_line_passes_filter_patch,
        mock_scribereader,
    ):
        determine_scribereader_envs_patch.return_value = ['env1']
        fake_queue = mock.MagicMock(spec_set=Queue())
        # Prevent tail_paasta_logs from reading from queue forever by simulating a Ctrl-C
        fake_queue.get.side_effect = KeyboardInterrupt
        queue_patch.return_value = fake_queue

        logs.ScribeLogReader(cluster_map={'env1': 'env1'}).tail_logs(service, levels, components, clusters, instances)
        assert process_patch.call_count == 1


def test_determine_scribereader_envs():
    cluster = 'fake_cluster'
    components = ['build', 'monitoring']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        mock_scribereader,
    ):
        cluster_map = {
            cluster: 'fake_scribe_env',
        }
        actual = logs.ScribeLogReader(cluster_map=cluster_map).determine_scribereader_envs(components, cluster)
        assert actual == set(['devc', 'fake_scribe_env'])


def test_determine_scribereader_additional_envs():
    cluster = 'fake_cluster'
    components = ['fake_component']
    with mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True), \
            mock.patch('paasta_tools.cli.cmds.logs.LOG_COMPONENTS',
                       spec_set=dict, autospec=None) as mock_LOG_COMPONENTS:
        cluster_map = {
            cluster: 'fake_scribe_env',
        }
        LOG_COMPONENTS = {
            'fake_component': {
                'additional_source_envs': ['fake_scribe_env2']
            }
        }
        mock_LOG_COMPONENTS.__getitem__.side_effect = LOG_COMPONENTS.__getitem__

        actual = logs.ScribeLogReader(cluster_map=cluster_map).determine_scribereader_envs(components, cluster)
        assert 'fake_scribe_env' in actual and 'fake_scribe_env2' in actual


def test_prefix():
    actual = logs.prefix('TEST STRING', 'deploy')
    assert 'TEST STRING' in actual


def test_get_log_reader():
    mock_system_paasta_config = mock.Mock(autospec='paasta_tools.utils.SystemPaastaConfig')
    mock_system_paasta_config.get_log_reader.return_value = {'driver': 'scribereader', 'options': {'cluster_map': {}}}
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader', autospec=True),
    ) as (
        mock_load_system_paasta_config,
        mock_scribereader,
    ):
        mock_load_system_paasta_config.return_value = mock_system_paasta_config

        actual = logs.get_log_reader()
        assert isinstance(actual, logs.ScribeLogReader)


def test_generate_start_end_time():
    start_time, end_time = logs.generate_start_end_time()

    # Default for no args, test that there's a 30 minute difference
    time_delta = end_time - start_time

    # Do a loose comparison to make this test less time sensitive.
    # On slower systems, doing a datetime.datetime.utcnow() might
    # take a few milliseconds itself, doing a straight up == comparison
    # would not work
    actual_time = time_delta.total_seconds()
    ideal_time = 30 * 60

    assert abs(actual_time - ideal_time) < 0.1


def test_generate_start_end_time_human_durations():
    start_time, end_time = logs.generate_start_end_time("35m", "25m")

    time_delta = end_time - start_time

    actual_time = time_delta.total_seconds()
    ideal_time = 10 * 60

    # See note on the test above why this is not a simple == comparison
    assert abs(actual_time - ideal_time) < 0.1


def test_generate_start_end_time_invalid():
    # Try giving a start time that's later than the end time
    try:
        logs.generate_start_end_time("2016-06-06T20:26:49+00:00", "2016-06-06T20:25:49+00:00")
        assert False
    except ValueError:
        assert True


def test_generate_start_end_time_invalid_from():
    try:
        logs.generate_start_end_time("invalid", "2016-06-06T20:25:49+00:00")
        assert False
    except ValueError:
        assert True


def test_generate_start_end_time_invalid_to():
    try:
        logs.generate_start_end_time("2016-06-06T20:25:49+00:00", "invalid")
        assert False
    except ValueError:
        assert True


def test_validate_filtering_args_with_valid_inputs():
    fake_reader = logs.LogReader()
    fake_reader.SUPPORTS_TAILING = True
    fake_reader.SUPPORTS_LINE_COUNT = True
    fake_reader.SUPPORTS_TIME = True
    fake_reader.SUPPORTS_LINE_OFFSET = True

    # No arguments, completely valid
    args, _ = parse_args(["logs"])
    assert logs.validate_filtering_args(args, fake_reader)
    # Tailing
    args, _ = parse_args(["logs", "--tail"])
    assert logs.validate_filtering_args(args, fake_reader)
    # Specify number of lines
    args, _ = parse_args(["logs", "-l", "200"])
    assert logs.validate_filtering_args(args, fake_reader)
    # Specify number of lines and lines to offset by
    args, _ = parse_args(["logs", "-l", "200", "-o", "23"])
    assert logs.validate_filtering_args(args, fake_reader)
    # Specify a time
    args, _ = parse_args(["logs", "--from", "1w"])
    assert logs.validate_filtering_args(args, fake_reader)


def test_validate_filtering_args_with_invalid_inputs():
    fake_reader = logs.LogReader()

    fake_reader.SUPPORTS_TAILING = False
    args, _ = parse_args(["logs", "--tail"])
    assert not logs.validate_filtering_args(args, fake_reader)

    fake_reader.SUPPORTS_TIME = False
    args, _ = parse_args(["logs", "--from", "1w"])
    assert not logs.validate_filtering_args(args, fake_reader)

    fake_reader.SUPPORTS_LINE_COUNT = False
    args, _ = parse_args(["logs", "-l", "200"])
    assert not logs.validate_filtering_args(args, fake_reader)

    fake_reader.SUPPORTS_LINE_OFFSET = False
    args, _ = parse_args(["logs", "-o", "23"])
    assert not logs.validate_filtering_args(args, fake_reader)

    fake_reader.SUPPORTS_TAILING = True
    fake_reader.SUPPORTS_LINE_COUNT = True
    fake_reader.SUPPORTS_LINE_OFFSET = True
    fake_reader.SUPPORTS_TIME = True

    # Can't tail and specify lines at the same time
    args, _ = parse_args(["logs", "-l", "200", "--tail"])
    assert not logs.validate_filtering_args(args, fake_reader)

    # Can't tail and specify time at the same time
    args, _ = parse_args(["logs", "--tail", "--from", "1w"])
    assert not logs.validate_filtering_args(args, fake_reader)

    # Can't use both time and lines at the same time
    args, _ = parse_args(["logs", "--from", "1w", "-l", "100"])
    assert not logs.validate_filtering_args(args, fake_reader)


def test_pick_default_log_mode():
    with mock.patch('paasta_tools.cli.cmds.logs.LogReader.tail_logs', autospec=True) as tail_logs:
        args, _ = parse_args(["logs"])

        fake_reader = logs.LogReader()
        fake_reader.SUPPORTS_TAILING = True

        logs.pick_default_log_mode(
            args, fake_reader, service=None, levels=None, components=None, clusters=None, instances=None)

        # Only supports tailing so that's the one that should be used
        assert tail_logs.call_count == 1

    with mock.patch('paasta_tools.cli.cmds.logs.LogReader.print_logs_by_time', autospec=True) as logs_by_time:
        args, _ = parse_args(["logs"])

        fake_reader = logs.LogReader()
        fake_reader.SUPPORTS_TAILING = True
        fake_reader.SUPPORTS_TIME = True

        logs.pick_default_log_mode(
            args, fake_reader, service=None, levels=None, components=None, clusters=None, instances=None)

        # Supports tailing and time, but time should be prioritized
        assert logs_by_time.call_count == 1

    with mock.patch('paasta_tools.cli.cmds.logs.LogReader.print_last_n_logs', autospec=True) as logs_by_lines:
        args, _ = parse_args(["logs"])

        fake_reader = logs.LogReader()
        fake_reader.SUPPORTS_TAILING = True
        fake_reader.SUPPORTS_TIME = True
        fake_reader.SUPPORTS_LINE_COUNT = True

        logs.pick_default_log_mode(
            args, fake_reader, service=None, levels=None, components=None, clusters=None, instances=None)

        # Supports tailing , time and line counts. Line counts should be prioritized
        assert logs_by_lines.call_count == 1
