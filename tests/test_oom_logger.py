# Copyright 2015-2017 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import sys
from collections import namedtuple

import pytest
import simplejson as json
from mock import Mock
from mock import patch

from paasta_tools.oom_logger import capture_oom_events_from_stdin
from paasta_tools.oom_logger import log_to_scribe
from paasta_tools.oom_logger import main


@pytest.fixture
def sys_stdin():
    return io.StringIO(
        '1500316300 dev37-devc [30533610.306529] Task in '
        '/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79 '
        'killed as a result of limit of '
        '/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79',
    )


@pytest.fixture
def docker_inspect():
    return {'Config': {'Env': ['PAASTA_SERVICE=fake_service', 'PAASTA_INSTANCE=fake_instance']}}


@pytest.fixture
def log_line():
    LogLine = namedtuple(
        'LogLine', [
            'timestamp', 'hostname', 'container_id',
            'cluster', 'service', 'instance',
        ],
    )
    return LogLine(
        timestamp=1500316300,
        hostname='dev37-devc',
        container_id='a687af92e281',
        cluster='fake_cluster',
        service='fake_service',
        instance='fake_instance',
    )


def test_capture_oom_events_from_stdin(sys_stdin):
    sys.stdin = sys_stdin
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)

    assert test_output == [(1500316300, 'dev37-devc', 'a687af92e281')]


def test_log_to_scribe(log_line):
    logger = Mock()
    log_to_scribe(logger, log_line)
    logger.log_line.assert_called_once_with(
        'tmp_paasta_oom_events',
        json.dumps({
            'timestamp': log_line.timestamp,
            'hostname': log_line.hostname,
            'container_id': log_line.container_id,
            'cluster': log_line.cluster,
            'service': log_line.service,
            'instance': log_line.instance,
        }),
    )


@patch('paasta_tools.oom_logger.ScribeLogger', autospec=True)
@patch('paasta_tools.oom_logger.load_system_paasta_config', autospec=True)
@patch('paasta_tools.oom_logger.log_to_scribe', autospec=True)
@patch('paasta_tools.oom_logger.log_to_paasta', autospec=True)
@patch('paasta_tools.oom_logger.get_docker_client', autospec=True)
def test_main(
    mock_get_docker_client,
    mock_log_to_paasta,
    mock_log_to_scribe,
    mock_load_system_paasta_config,
    mock_scribelogger,
    sys_stdin,
    docker_inspect,
    log_line,
):

    sys.stdin = sys_stdin
    docker_client = Mock(inspect_container=Mock(return_value=docker_inspect))
    mock_get_docker_client.return_value = docker_client
    mock_load_system_paasta_config.return_value.get_cluster.return_value = 'fake_cluster'
    scribe_logger = Mock()
    mock_scribelogger.return_value = scribe_logger

    main()
    mock_log_to_paasta.assert_called_once_with(log_line)
    mock_log_to_scribe.assert_called_once_with(scribe_logger, log_line)
