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
import json

import pytest
from mock import Mock
from mock import patch

from paasta_tools.oom_logger import capture_oom_events_from_stdin
from paasta_tools.oom_logger import log_to_scribe
from paasta_tools.oom_logger import LogLine
from paasta_tools.oom_logger import main


@pytest.fixture
def sys_stdin():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n,"
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79 "
        "killed as a result of limit of "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_burstable_qos():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n",
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/kubepods/burstable/podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772 "
        "killed as a result of limit of /kubepods/burstable/"
        "podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_guaranteed_qos():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n",
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/kubepods/podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772 "
        "killed as a result of limit of /kubepods/"
        "podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_besteffort_qos():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n",
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/kubepods/besteffort/podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772 "
        "killed as a result of limit of /kubepods/besteffort/"
        "podf91e9681-4741-4ef4-8f5a-182c5683df8b/"
        "0e4a814eda03622476ff47871e6c397e5b8747af209b44f3b3e1c5289b0f9772\n",
    ]


@pytest.fixture
def sys_stdin_process_name_with_slashes():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] /nail/live/yelp invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n,"
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79 "
        "killed as a result of limit of "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79\n",
    ]


@pytest.fixture
def sys_stdin_process_name_with_spaces():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] python batch/ke invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n,"
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79 "
        "killed as a result of limit of "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79\n",
    ]


@pytest.fixture
def sys_stdin_without_process_name():
    return [
        "some random line1\n",
        "1500216300 dev37-devc [1140036.678311] Task in "
        "/docker/e3a1057fdd485f5dffe48f1584e6f30c2bf6d30107d95518aea32bbb8bb29560 "
        "killed as a result of limit of "
        "/docker/e3a1057fdd485f5dffe48f1584e6f30c2bf6d30107d95518aea32bbb8bb29560\n,"
        "some random line2\n",
        "1500316300 dev37-devc [30533610.306529] Task in "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79 "
        "killed as a result of limit of "
        "/docker/a687af92e281725daf5b4cda0b487f20d2055d2bb6814b76d0e39c18a52a4e79\n",
    ]


@pytest.fixture
def docker_inspect():
    return {
        "Config": {
            "Env": [
                "PAASTA_SERVICE=fake_service",
                "PAASTA_INSTANCE=fake_instance",
                "PAASTA_RESOURCE_MEM=512",
                "MESOS_CONTAINER_NAME=mesos-a04c14a6-83ea-4047-a802-92b850b1624e",
            ]
        }
    }


@pytest.fixture
def log_line():
    return LogLine(
        timestamp=1500316300,
        hostname="dev37-devc",
        container_id="a687af92e281",
        cluster="fake_cluster",
        service="fake_service",
        instance="fake_instance",
        process_name="apache2",
        mesos_container_id="mesos-a04c14a6-83ea-4047-a802-92b850b1624e",
        mem_limit="512",
    )


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin(mock_sys_stdin, sys_stdin):
    mock_sys_stdin.readline.side_effect = sys_stdin
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)

    assert test_output == [(1500316300, "dev37-devc", "a687af92e281", "apache2")]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_kubernetes_qos(
    mock_sys_stdin,
    sys_stdin_kubernetes_besteffort_qos,
    sys_stdin_kubernetes_burstable_qos,
    sys_stdin_kubernetes_guaranteed_qos,
):
    for qos in (
        sys_stdin_kubernetes_besteffort_qos,
        sys_stdin_kubernetes_burstable_qos,
        sys_stdin_kubernetes_guaranteed_qos,
    ):
        mock_sys_stdin.readline.side_effect = qos
        test_output = []
        for a_tuple in capture_oom_events_from_stdin():
            test_output.append(a_tuple)
        assert test_output == [(1500316300, "dev37-devc", "0e4a814eda03", "apache2")]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_with_slashes(
    mock_sys_stdin, sys_stdin_process_name_with_slashes
):
    mock_sys_stdin.readline.side_effect = sys_stdin_process_name_with_slashes
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)

    assert test_output == [
        (1500316300, "dev37-devc", "a687af92e281", "/nail/live/yelp")
    ]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_with_spaces(
    mock_sys_stdin, sys_stdin_process_name_with_spaces
):
    mock_sys_stdin.readline.side_effect = sys_stdin_process_name_with_spaces
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)

    assert test_output == [
        (1500316300, "dev37-devc", "a687af92e281", "python batch/ke")
    ]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_without_process_name(
    mock_sys_stdin, sys_stdin_without_process_name
):
    mock_sys_stdin.readline.side_effect = sys_stdin_without_process_name
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)

    assert test_output == [
        (1500216300, "dev37-devc", "e3a1057fdd48", ""),
        (1500316300, "dev37-devc", "a687af92e281", ""),
    ]


def test_log_to_scribe(log_line):
    logger = Mock()
    log_to_scribe(logger, log_line)
    logger.log_line.assert_called_once_with(
        "tmp_paasta_oom_events",
        json.dumps(
            {
                "timestamp": log_line.timestamp,
                "hostname": log_line.hostname,
                "container_id": log_line.container_id,
                "cluster": log_line.cluster,
                "service": log_line.service,
                "instance": log_line.instance,
                "process_name": log_line.process_name,
                "mesos_container_id": log_line.mesos_container_id,
                "mem_limit": log_line.mem_limit,
            }
        ),
    )


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
@patch("paasta_tools.oom_logger.ScribeLogger", autospec=None)
@patch("paasta_tools.oom_logger.load_system_paasta_config", autospec=True)
@patch("paasta_tools.oom_logger.log_to_scribe", autospec=True)
@patch("paasta_tools.oom_logger.log_to_paasta", autospec=True)
@patch("paasta_tools.oom_logger.get_docker_client", autospec=True)
def test_main(
    mock_get_docker_client,
    mock_log_to_paasta,
    mock_log_to_scribe,
    mock_load_system_paasta_config,
    mock_scribelogger,
    mock_sys_stdin,
    sys_stdin,
    docker_inspect,
    log_line,
):

    mock_sys_stdin.readline.side_effect = sys_stdin
    docker_client = Mock(inspect_container=Mock(return_value=docker_inspect))
    mock_get_docker_client.return_value = docker_client
    mock_load_system_paasta_config.return_value.get_cluster.return_value = (
        "fake_cluster"
    )
    scribe_logger = Mock()
    mock_scribelogger.return_value = scribe_logger

    main()
    mock_log_to_paasta.assert_called_once_with(log_line)
    mock_log_to_scribe.assert_called_once_with(scribe_logger, log_line)
