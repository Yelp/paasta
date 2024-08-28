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
from mock import MagicMock
from mock import Mock
from mock import patch

from paasta_tools.oom_logger import capture_oom_events_from_stdin
from paasta_tools.oom_logger import log_to_clog
from paasta_tools.oom_logger import LogLine
from paasta_tools.oom_logger import main
from paasta_tools.oom_logger import send_sfx_event


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
def sys_stdin_kubernetes_structured_burstable_qos():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n",
        "some random line2\n",
        "1500316300 dev37-devc [541471.893603] oom-kill:constraint=CONSTRAINT_MEMCG,"
        "nodemask=(null),cpuset=1b6f55bfbed10cdc2bb6944078eaefd3278f8f9b3a9725c4ddffb722752a2279,"
        "mems_allowed=0-1,oom_memcg=/kubepods/burstable/podb56c3a7a-a84d-4f84-b97e-446c4e705259/"
        "0e4a814eda030cdc2bb6944078eaefd3278f8f9b3a9725c4ddffb722752a2279,"
        "task_memcg=/kubepods/burstable/podb56c3a7a-a84d-4f84-b97e-446c4e705259/"
        "0e4a814eda030cdc2bb6944078eaefd3278f8f9b3a9725c4ddffb722752a2279,"
        "task=kafka_exporter,pid=43716,uid=65534\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_structured_burstable_systemd_cgroup():
    return [
        "some random line1\n",
        "1500316299 dev37-devc [30533610.306528] apache2 invoked oom-killer: "
        "gfp_mask=0x24000c0, order=0, oom_score_adj=0\n",
        "some random line2\n",
        "1500316300 dev37-devc [541471.893603] oom-kill:constraint=CONSTRAINT_MEMCG,"
        "nodemask=(null),cpuset=docker-e7ba37bd37089f8b1fda33c6f1fe753421ca6216518594bc73bca2ead7c13ba0.scope,"
        "mems_allowed=0,oom_memcg=/kubepods.slice/kubepods-burstable.slice/"
        "kubepods-burstable-pod8a0a7a03_d305_4ebc_83ad_91180c9d5ef9.slice/"
        "docker-e7ba37bd37089f8b1fda33c6f1fe753421ca6216518594bc73bca2ead7c13ba0.scope,"
        "task_memcg=/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod8a0a7a03_d305_4ebc_83ad_91180c9d5ef9.slice/"
        "docker-e7ba37bd37089f8b1fda33c6f1fe753421ca6216518594bc73bca2ead7c13ba0.scope,task=apache2,pid=1757658,uid=0\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_containerd_systemd_cgroup_structured():
    return [
        "some random line1\n",
        "1720128512 dev37-devc [ 7195.442797] python3 invoked oom-killer: "
        "gfp_mask=0xcc0(GFP_KERNEL), order=0, oom_score_adj=999\n",
        "some random line2\n",
        "1720128512 dev37-devc [ 7195.442928] oom-kill:constraint=CONSTRAINT_MEMCG,"
        "cpuset=cri-containerd-e216d2f1e6c625d363c71edb6b3cbab5a9e1b447641b61028d0b94b077adf27c.scope,"
        "mems_allowed=0,oom_memcg=/kubepods.slice/kubepods-burstable.slice/"
        "kubepods-burstable-pod08768c36_163c_40e5_8e49_09cf42ff5046.slice,"
        "task_memcg=/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod08768c36_163c_40e5_8e49_09cf42ff5046.slice/"
        "cri-containerd-e216d2f1e6c625d363c71edb6b3cbab5a9e1b447641b61028d0b94b077adf27c.scope,task=python3,pid=485850,uid=33\n",
    ]


@pytest.fixture
def sys_stdin_kubernetes_containerd_systemd_cgroup():
    return [
        "some random line1\n",
        "1720128512 dev208-uswest1adevc [42201.484624] python3 invoked oom-killer: "
        "gfp_mask=0xcc0(GFP_KERNEL), order=0, oom_score_adj=999\n",
        "some random line2\n",
        "1720128512 dev208-uswest1adevc [42201.484749] oom-kill:constraint=CONSTRAINT_MEMCG,"
        "nodemask=(null),cpuset=kubepods-burstable-pod73331cbb_9b96_4a62_9702_46a56ad49dd0.slice:"
        "cri-containerd:52f9ece9bcf929a08951aa3b4312fbec50890d82b58988f91a0aa9dc96ebc199,"
        "mems_allowed=0,oom_memcg=/system.slice/kubepods-burstable-pod73331cbb_9b96_4a62_9702_46a56ad49dd0.slice:"
        "cri-containerd:52f9ece9bcf929a08951aa3b4312fbec50890d82b58988f91a0aa9dc96ebc199,"
        "task_memcg=/system.slice/kubepods-burstable-pod73331cbb_9b96_4a62_9702_46a56ad49dd0.slice:"
        "cri-containerd:52f9ece9bcf929a08951aa3b4312fbec50890d82b58988f91a0aa9dc96ebc199,task=python3,pid=4190418,uid=33\n",
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
def containerd_inspect():
    return {
        "process": {
            "env": [
                "PAASTA_SERVICE=fake_service",
                "PAASTA_INSTANCE=fake_instance",
                "PAASTA_RESOURCE_MEM=512",
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


@pytest.fixture
def log_line_containerd():
    return LogLine(
        timestamp=1720128512,
        hostname="dev37-devc",
        container_id="e216d2f1e6c625d363c71edb6b3cbab5a9e1b447641b61028d0b94b077adf27c",
        cluster="fake_cluster",
        service="fake_service",
        instance="fake_instance",
        process_name="python3",
        mesos_container_id="mesos-null",
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
def test_capture_oom_events_from_stdin_kubernetes_structured_qos(
    mock_sys_stdin,
    sys_stdin_kubernetes_structured_burstable_qos,
):
    mock_sys_stdin.readline.side_effect = sys_stdin_kubernetes_structured_burstable_qos
    test_output = []
    for a_tuple in capture_oom_events_from_stdin():
        test_output.append(a_tuple)
    assert test_output == [(1500316300, "dev37-devc", "0e4a814eda03", "apache2")]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_kubernetes_structured_burstable_systemd_cgroup(
    mock_sys_stdin,
    sys_stdin_kubernetes_structured_burstable_systemd_cgroup,
):
    mock_sys_stdin.readline.side_effect = (
        sys_stdin_kubernetes_structured_burstable_systemd_cgroup
    )
    test_output = [a_tuple for a_tuple in capture_oom_events_from_stdin()]
    assert test_output == [(1500316300, "dev37-devc", "e7ba37bd3708", "apache2")]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_kubernetes_containerd_systemd_cgroup(
    mock_sys_stdin,
    sys_stdin_kubernetes_containerd_systemd_cgroup,
):
    mock_sys_stdin.readline.side_effect = sys_stdin_kubernetes_containerd_systemd_cgroup
    test_output = [a_tuple for a_tuple in capture_oom_events_from_stdin()]
    assert test_output == [
        (
            1720128512,
            "dev208-uswest1adevc",
            "52f9ece9bcf929a08951aa3b4312fbec50890d82b58988f91a0aa9dc96ebc199",
            "python3",
        )
    ]


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
def test_capture_oom_events_from_stdin_kubernetes_containerd_systemd_cgroup_structured(
    mock_sys_stdin,
    sys_stdin_kubernetes_containerd_systemd_cgroup_structured,
):
    mock_sys_stdin.readline.side_effect = (
        sys_stdin_kubernetes_containerd_systemd_cgroup_structured
    )
    test_output = [a_tuple for a_tuple in capture_oom_events_from_stdin()]
    assert test_output == [
        (
            1720128512,
            "dev37-devc",
            "e216d2f1e6c625d363c71edb6b3cbab5a9e1b447641b61028d0b94b077adf27c",
            "python3",
        )
    ]


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


@patch("paasta_tools.oom_logger.clog", autospec=True)
def test_log_to_clog(mock_clog, log_line):
    log_to_clog(log_line)
    mock_clog.log_line.assert_called_once_with(
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


@patch("paasta_tools.oom_logger.get_instance_config", autospec=True)
def test_send_sfx_event(mock_get_instance_config):
    service = "foo"
    instance = "bar"
    cluster = "baz"

    # Try to use the autospec if it's available
    from paasta_tools.oom_logger import yelp_meteorite

    if yelp_meteorite is None:
        autospec = None
    else:
        autospec = True

    with patch(
        "paasta_tools.oom_logger.yelp_meteorite", autospec=autospec
    ) as mock_meteorite:
        send_sfx_event(service, instance, cluster)

        expected_dimensions = {
            "paasta_service": service,
            "paasta_instance": instance,
            "paasta_cluster": cluster,
            "paasta_pool": mock_get_instance_config.return_value.get_pool.return_value,
        }
        mock_meteorite.events.emit_event.assert_called_once_with(
            "paasta.service.oom_events", dimensions=expected_dimensions
        )
        mock_meteorite.create_counter.assert_called_once_with(
            "paasta.service.oom_count", default_dimensions=expected_dimensions
        )
        assert mock_meteorite.create_counter.return_value.count.call_count == 1


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
@patch("paasta_tools.oom_logger.clog", autospec=True)
@patch("paasta_tools.oom_logger.send_sfx_event", autospec=True)
@patch("paasta_tools.oom_logger.load_system_paasta_config", autospec=True)
@patch("paasta_tools.oom_logger.log_to_clog", autospec=True)
@patch("paasta_tools.oom_logger.log_to_paasta", autospec=True)
@patch("paasta_tools.oom_logger.get_docker_client", autospec=True)
@patch("paasta_tools.oom_logger.parse_args", autospec=True)
def test_main(
    mock_parse_args,
    mock_get_docker_client,
    mock_log_to_paasta,
    mock_log_to_clog,
    mock_load_system_paasta_config,
    mock_send_sfx_event,
    mock_clog,
    mock_sys_stdin,
    sys_stdin,
    docker_inspect,
    log_line,
):

    mock_sys_stdin.readline.side_effect = sys_stdin
    mock_parse_args.return_value.containerd = False
    docker_client = Mock(inspect_container=Mock(return_value=docker_inspect))
    mock_get_docker_client.return_value = docker_client
    mock_load_system_paasta_config.return_value.get_cluster.return_value = (
        "fake_cluster"
    )

    main()
    mock_log_to_paasta.assert_called_once_with(log_line)
    mock_log_to_clog.assert_called_once_with(log_line)
    mock_send_sfx_event.assert_called_once_with(
        "fake_service", "fake_instance", "fake_cluster"
    )


@patch("paasta_tools.oom_logger.sys.stdin", autospec=True)
@patch("paasta_tools.oom_logger.clog", autospec=True)
@patch("paasta_tools.oom_logger.send_sfx_event", autospec=True)
@patch("paasta_tools.oom_logger.load_system_paasta_config", autospec=True)
@patch("paasta_tools.oom_logger.log_to_clog", autospec=True)
@patch("paasta_tools.oom_logger.log_to_paasta", autospec=True)
@patch("paasta_tools.oom_logger.parse_args", autospec=True)
@patch("paasta_tools.oom_logger.get_containerd_container", autospec=True)
@patch("paasta_tools.oom_logger.json.loads", autospec=True)
def test_main_containerd(
    mock_json_loads,
    mock_get_containerd_container,
    mock_parse_args,
    mock_log_to_paasta,
    mock_log_to_clog,
    mock_load_system_paasta_config,
    mock_send_sfx_event,
    mock_clog,
    mock_sys_stdin,
    sys_stdin_kubernetes_containerd_systemd_cgroup_structured,
    log_line_containerd,
    containerd_inspect,
):

    mock_sys_stdin.readline.side_effect = (
        sys_stdin_kubernetes_containerd_systemd_cgroup_structured
    )
    mock_parse_args.return_value.containerd = True

    mock_container_info = MagicMock()
    mock_container_info.spec.value.decode.return_value = str(containerd_inspect)

    mock_get_containerd_container.return_value = mock_container_info
    mock_json_loads.return_value = containerd_inspect

    mock_load_system_paasta_config.return_value.get_cluster.return_value = (
        "fake_cluster"
    )

    main()
    mock_log_to_paasta.assert_called_once_with(log_line_containerd)
    mock_log_to_clog.assert_called_once_with(log_line_containerd)
    mock_send_sfx_event.assert_called_once_with(
        "fake_service", "fake_instance", "fake_cluster"
    )
