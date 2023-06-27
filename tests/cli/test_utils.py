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
import argparse
import getpass
from socket import gaierror

import ephemeral_port_reserve
import mock
from mock import call
from mock import patch
from pytest import mark
from pytest import raises

from paasta_tools.cli import utils
from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import select_k8s_secret_namespace
from paasta_tools.cli.utils import verify_instances
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import SystemPaastaConfig


@patch("socket.gethostbyname_ex", autospec=True)
def test_bad_calculate_remote_master(mock_get_by_hostname, system_paasta_config):
    mock_get_by_hostname.side_effect = gaierror(42, "bar")
    ips, output = utils.calculate_remote_masters("myhost", system_paasta_config)
    assert ips == []
    assert "ERROR while doing DNS lookup of paasta-myhost.yelp:\nbar\n" in output


@patch("socket.gethostbyname_ex", autospec=True)
def test_ok_remote_masters(mock_get_by_hostname, system_paasta_config):
    mock_get_by_hostname.return_value = ("myhost", [], ["1.2.3.4", "1.2.3.5"])
    ips, output = utils.calculate_remote_masters("myhost", system_paasta_config)
    assert output is None
    assert ips == ["1.2.3.4", "1.2.3.5"]


@patch("paasta_tools.cli.utils.check_ssh_on_master", autospec=True)
def test_find_connectable_master_happy_path(mock_check_ssh_on_master):
    masters = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
    timeout = 6.0
    mock_check_ssh_on_master.return_value = (True, None)

    actual = utils.find_connectable_master(masters)
    expected = (masters[0], None)
    assert mock_check_ssh_on_master.call_count == 1
    mock_check_ssh_on_master.assert_called_once_with(masters[0], timeout=timeout)
    assert actual == expected


@patch("random.shuffle", autospec=True)
@patch("paasta_tools.cli.utils.find_connectable_master", autospec=True)
@patch("paasta_tools.cli.utils.calculate_remote_masters", autospec=True)
def test_connectable_master_random(
    mock_calculate_remote_masters,
    mock_find_connectable_master,
    mock_shuffle,
    system_paasta_config,
):
    masters = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
    mock_calculate_remote_masters.return_value = (masters, None)
    mock_find_connectable_master.return_value = (masters[0], None)
    mock_shuffle.return_value = None

    utils.connectable_master("fake_cluster", system_paasta_config)
    mock_shuffle.assert_called_once_with(masters)


@patch("paasta_tools.cli.utils.check_ssh_on_master", autospec=True)
def test_find_connectable_master_one_failure(mock_check_ssh_on_master):
    masters = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
    timeout = 6.0
    # iter() is a workaround
    # (http://lists.idyll.org/pipermail/testing-in-python/2013-April/005527.html)
    # for a bug in mock (http://bugs.python.org/issue17826)
    create_connection_side_effects = iter(
        [(False, "something bad"), (True, "unused"), (True, "unused")]
    )
    mock_check_ssh_on_master.side_effect = create_connection_side_effects
    mock_check_ssh_on_master.return_value = True

    actual = utils.find_connectable_master(masters)
    assert mock_check_ssh_on_master.call_count == 2
    mock_check_ssh_on_master.assert_any_call(masters[0], timeout=timeout)
    mock_check_ssh_on_master.assert_any_call(masters[1], timeout=timeout)
    assert actual == ("192.0.2.2", None)


@patch("paasta_tools.cli.utils.check_ssh_on_master", autospec=True)
def test_find_connectable_master_all_failures(mock_check_ssh_on_master):
    masters = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
    timeout = 6.0
    mock_check_ssh_on_master.return_value = (255, "timeout")

    actual = utils.find_connectable_master(masters)
    assert mock_check_ssh_on_master.call_count == 3
    mock_check_ssh_on_master.assert_any_call((masters[0]), timeout=timeout)
    mock_check_ssh_on_master.assert_any_call((masters[1]), timeout=timeout)
    mock_check_ssh_on_master.assert_any_call((masters[2]), timeout=timeout)
    assert actual[0] is None
    assert "timeout" in actual[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_check_ssh_on_master_check_successful(mock_run):
    master = "fake_master"
    mock_run.return_value = (0, "fake_output")
    expected_command = "ssh -A -n -o StrictHostKeyChecking=no %s /bin/true" % master

    actual = utils.check_ssh_on_master(master)
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert actual == (True, None)


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_check_ssh_on_master_check_ssh_failure(mock_run):
    master = "fake_master"
    mock_run.return_value = (255, "fake_output")

    actual = utils.check_ssh_on_master(master)
    assert actual[0] is False
    assert "fake_output" in actual[1]
    assert "255" in actual[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_check_ssh_on_master_check_sudo_failure(mock_run):
    master = "fake_master"
    mock_run.return_value = (1, "fake_output")

    actual = utils.check_ssh_on_master(master)
    assert actual[0] is False
    assert "1" in actual[1]
    assert "fake_output" in actual[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_metastatus(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -n -o StrictHostKeyChecking=no fake_master sudo paasta_metastatus"
    )
    return_code, actual = utils.run_paasta_metastatus("fake_master", [], 0)
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_metastatus_verbose(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -n -o StrictHostKeyChecking=no fake_master sudo paasta_metastatus -v"
    )
    return_code, actual = utils.run_paasta_metastatus("fake_master", [], 1)
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_metastatus_very_verbose(mock_run):
    mock_run.return_value = (0, "fake_output")
    return_code, actual = utils.run_paasta_metastatus("fake_master", [], 2, False)
    expected_command = (
        "ssh -A -n -o StrictHostKeyChecking=no fake_master sudo paasta_metastatus -vv"
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.utils.list_services", autospec=True)
def test_list_service_instances(mock_list_services, mock_list_instances):
    mock_list_services.return_value = ["fake_service"]
    mock_list_instances.return_value = ["canary", "main"]
    expected = ["fake_service.canary", "fake_service.main"]
    actual = utils.list_service_instances()
    assert actual == expected


@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.utils.list_services", autospec=True)
def test_list_paasta_services(mock_list_services, mock_list_instances):
    mock_list_services.return_value = ["fake_service"]
    mock_list_instances.return_value = ["canary", "main"]
    expected = ["fake_service"]
    actual = utils.list_paasta_services()
    assert actual == expected


@patch("paasta_tools.cli.utils.guess_service_name", autospec=True)
@patch("paasta_tools.cli.utils.validate_service_name", autospec=True)
@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
def test_list_instances_with_autodetect(
    mock_list_instance_for_service, mock_validate_service_name, mock_guess_service_name
):
    expected = ["instance1", "instance2", "instance3"]
    mock_guess_service_name.return_value = "fake_service"
    mock_validate_service_name.return_value = None
    mock_list_instance_for_service.return_value = expected
    actual = utils.list_instances()
    assert actual == expected
    mock_validate_service_name.assert_called_once_with("fake_service")
    mock_list_instance_for_service.assert_called_once_with("fake_service")


@patch("paasta_tools.cli.utils.guess_service_name", autospec=True)
@patch("paasta_tools.cli.utils.validate_service_name", autospec=True)
@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.utils.list_services", autospec=True)
def test_list_instances_no_service(
    mock_list_services,
    mock_list_instance_for_service,
    mock_validate_service_name,
    mock_guess_service_name,
):
    expected = ["instance1", "instance2", "instance3"]
    mock_guess_service_name.return_value = "unused"
    mock_list_services.return_value = ["fake_service1"]
    mock_validate_service_name.side_effect = utils.NoSuchService(None)
    mock_list_instance_for_service.return_value = expected
    actual = utils.list_instances()
    mock_validate_service_name.assert_called_once_with("unused")
    mock_list_instance_for_service.assert_called_once_with("fake_service1")
    assert actual == expected


def test_lazy_choices_completer():
    completer = utils.lazy_choices_completer(lambda: ["1", "2", "3"])
    assert completer(prefix="") == ["1", "2", "3"]


@mock.patch("paasta_tools.cli.utils.INSTANCE_TYPE_HANDLERS", dict(), autospec=None)
@mock.patch("paasta_tools.cli.utils.validate_service_instance", autospec=True)
def test_get_instance_config_by_instance_type(
    mock_validate_service_instance,
):
    instance_type = "fake_type"
    mock_validate_service_instance.return_value = instance_type
    mock_load_config = mock.MagicMock()
    mock_load_config.return_value = "fake_service_config"
    utils.INSTANCE_TYPE_HANDLERS[instance_type] = utils.InstanceTypeHandler(
        None, mock_load_config
    )
    actual = utils.get_instance_config(
        service="fake_service",
        instance="fake_instance",
        cluster="fake_cluster",
        soa_dir="fake_soa_dir",
    )
    assert mock_validate_service_instance.call_count == 1
    assert mock_load_config.call_count == 1
    assert actual == "fake_service_config"


@mock.patch("paasta_tools.cli.utils.validate_service_instance", autospec=True)
def test_get_instance_config_unknown(
    mock_validate_service_instance,
):
    with raises(NotImplementedError):
        mock_validate_service_instance.return_value = "some bogus unsupported framework"
        utils.get_instance_config(
            service="fake_service",
            instance="fake_instance",
            cluster="fake_cluster",
            soa_dir="fake_soa_dir",
        )
    assert mock_validate_service_instance.call_count == 1


def test_get_subparser():
    mock_subparser = mock.Mock()
    mock_function = mock.Mock()
    mock_command = "test"
    mock_help_text = "HALP"
    mock_description = "what_i_do"
    utils.get_subparser(
        subparsers=mock_subparser,
        function=mock_function,
        help_text=mock_help_text,
        description=mock_description,
        command=mock_command,
    )
    mock_subparser.add_parser.assert_called_with(
        "test",
        help="HALP",
        description=("what_i_do"),
        epilog=(
            "Note: This command requires SSH and "
            "sudo privileges on the remote PaaSTA nodes."
        ),
    )
    mock_subparser.add_parser.return_value.set_defaults.assert_called_with(
        command=mock_function
    )


def test_pick_slave_from_status():
    mock_slaves = [1, 2]
    mock_status = mock.Mock(marathon=mock.Mock(slaves=mock_slaves))
    assert utils.pick_slave_from_status(mock_status, host=None) == 1
    assert utils.pick_slave_from_status(mock_status, host="lolhost") == "lolhost"


def test_git_sha_validation():
    assert (
        utils.validate_full_git_sha("060ce8bc10efe0030c048a4711ad5dd85de5adac")
        == "060ce8bc10efe0030c048a4711ad5dd85de5adac"
    )
    with raises(argparse.ArgumentTypeError):
        utils.validate_full_git_sha("BAD")
    assert utils.validate_short_git_sha("060c") == "060c"
    with raises(argparse.ArgumentTypeError):
        utils.validate_short_git_sha("BAD")


@patch("paasta_tools.cli.utils.get_instance_configs_for_service", autospec=True)
def test_list_deploy_groups_parses_configs(
    mock_get_instance_configs_for_service,
):
    mock_get_instance_configs_for_service.return_value = [
        MarathonServiceConfig(
            service="foo",
            cluster="",
            instance="",
            config_dict={"deploy_group": "fake_deploy_group"},
            branch_dict=None,
        ),
        MarathonServiceConfig(
            service="foo",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
        ),
    ]
    actual = utils.list_deploy_groups(service="foo")
    assert actual == {"fake_deploy_group", "fake_cluster.fake_instance"}


def test_get_container_name():
    mock_task = mock.Mock(executor={"container": "container1"})
    ret = utils.get_container_name(mock_task)
    assert ret == "mesos-container1"


def test_pick_random_port():
    def fake_epr(ip, port):
        return port

    with mock.patch.object(
        ephemeral_port_reserve, "reserve", side_effect=fake_epr
    ), mock.patch.object(getpass, "getuser", return_value="nobody", autospec=True):
        # Two calls with the same service should try to reserve the same port.
        port1 = utils.pick_random_port("fake_service")
        port2 = utils.pick_random_port("fake_service")
        assert port1 == port2
        assert 33000 <= port1 < 58000

        # A third call with a different service should try to reserve a different port.
        port3 = utils.pick_random_port("different_fake_service")
        assert port1 != port3
        assert 33000 <= port3 < 58000


@patch("paasta_tools.cli.utils._log_audit", autospec=True)
@patch("paasta_tools.cli.utils.run_paasta_cluster_boost", autospec=True)
@patch("paasta_tools.cli.utils.connectable_master", autospec=True)
@mark.parametrize(
    "master_result,boost_result,expected_result",
    [(utils.NoMasterError("error"), None, 1), (mock.Mock(), 1, 1), (mock.Mock(), 0, 0)],
)
def test_execute_paasta_cluster_boost_on_remote_master(
    mock_connectable_master,
    mock_boost,
    mock_log,
    master_result,
    boost_result,
    expected_result,
):
    mock_c1 = mock.Mock()
    mock_connectable_master.side_effect = [mock_c1, master_result]
    clusters = ["c1", "c2"]
    mock_config = mock.Mock()
    mock_boost.side_effect = [(0, ""), (boost_result, "")]

    code, output = utils.execute_paasta_cluster_boost_on_remote_master(
        clusters,
        mock_config,
        "do_action",
        "a_pool",
        duration=30,
        override=False,
        boost=2,
        verbose=1,
    )

    shared_kwargs = dict(
        action="do_action",
        pool="a_pool",
        duration=30,
        override=False,
        boost=2,
        verbose=1,
    )
    expected_calls = [mock.call(master=mock_c1, **shared_kwargs)]
    if not isinstance(master_result, utils.NoMasterError):
        expected_calls.append(mock.call(master=master_result, **shared_kwargs))
    assert mock_boost.call_args_list == expected_calls
    assert code == expected_result


@mock.patch("paasta_tools.cli.utils._log", mock.Mock(), autospec=None)
@mock.patch("paasta_tools.cli.utils.load_system_paasta_config", autospec=True)
@mock.patch("socket.socket", autospec=True)
def test_trigger_deploys(mock_socket, mock_load_config):
    mock_load_config.return_value = SystemPaastaConfig({}, "/some/fake/dir")
    mock_client = mock_socket.return_value

    utils.trigger_deploys("a_service")

    assert mock_load_config.call_count == 1
    assert mock_client.connect.call_args_list == [
        mock.call(("sysgit.yelpcorp.com", 5049))
    ]
    assert mock_client.send.call_args_list == [mock.call("a_service\n".encode("utf-8"))]
    assert mock_client.close.call_count == 1


@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
@patch("builtins.print", autospec=True)
def test_verify_instances(mock_print, mock_list_all_instances_for_service):
    mock_list_all_instances_for_service.return_value = ["east", "west", "north"]

    assert verify_instances("west,esst", "fake_service", []) == ["esst"]
    assert mock_print.called
    mock_print.assert_has_calls(
        [
            call(
                "\x1b[31mfake_service doesn't have any instances matching esst.\x1b[0m"
            ),
            call("Did you mean any of these?"),
            call("  east"),
            call("  west"),
        ]
    )


@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
@patch("builtins.print", autospec=True)
def test_verify_instances_with_clusters(
    mock_print, mock_list_all_instances_for_service
):
    mock_list_all_instances_for_service.return_value = ["east", "west", "north"]

    assert verify_instances(
        "west,esst,fake", "fake_service", ["fake_cluster1", "fake_cluster2"]
    ) == ["esst", "fake"]
    assert mock_print.called
    mock_print.assert_has_calls(
        [
            call(
                "\x1b[31mfake_service doesn't have any instances matching esst,"
                " fake on fake_cluster1, fake_cluster2.\x1b[0m"
            ),
            call("Did you mean any of these?"),
            call("  east"),
            call("  west"),
        ]
    )


@patch("paasta_tools.cli.utils.list_all_instances_for_service", autospec=True)
def test_verify_instances_with_suffixes(mock_list_all_instances_for_service):
    mock_list_all_instances_for_service.return_value = [
        "fake_instance1",
        "fake_instance2.jobname",
    ]

    assert (
        verify_instances(
            "fake_instance1.containername", "fake_service", ["fake_cluster"]
        )
        == []
    )
    assert (
        verify_instances("fake_instance2.jobname", "fake_service", ["fake_cluster"])
        == []
    )


@mark.parametrize(
    "tag,expected_result",
    [
        (
            "refs/tags/paasta-paasta-deploy.group-00000000T000000-deploy",
            {
                "deploy_group": "deploy.group",
                "image_version": None,
                "tstamp": "00000000T000000",
                "tag": "deploy",
            },
        ),
        (
            "refs/tags/paasta-paasta-test-cluster.main-00000000T000000-start",
            {
                "deploy_group": "test-cluster.main",
                "image_version": None,
                "tstamp": "00000000T000000",
                "tag": "start",
            },
        ),  # technically not a deploy tag, but we use this on all tags
        (
            "refs/tags/paasta-deploy.group-00000000T000000-deploy",
            {
                "deploy_group": "deploy.group",
                "image_version": None,
                "tstamp": "00000000T000000",
                "tag": "deploy",
            },
        ),
        (
            "refs/tags/paasta-no-commit-deploy.group+11111111T111111-00000000T000000-deploy",
            {
                "deploy_group": "no-commit-deploy.group",
                "image_version": "11111111T111111",
                "tstamp": "00000000T000000",
                "tag": "deploy",
            },
        ),
    ],
)
def test_extract_tags(tag, expected_result):
    assert extract_tags(tag) == expected_result


def test_select_k8s_secret_namespace():
    namespaces = {}
    assert not select_k8s_secret_namespace(namespaces)

    namespaces = {"random_experiment"}
    assert select_k8s_secret_namespace(namespaces) == "random_experiment"

    namespaces = {"random_experiment", "paasta"}
    assert select_k8s_secret_namespace(namespaces) == "paasta"

    namespaces = {"paasta-flinks", "paastasvc-something"}
    assert select_k8s_secret_namespace(namespaces).startswith("paasta")

    namespaces = {"paasta-flinks", "tron", "something"}
    namespace = select_k8s_secret_namespace(namespaces)
    assert namespace == "paasta-flinks" or namespace == "tron"

    namespaces = {"a", "b"}
    assert select_k8s_secret_namespace(namespaces) in {"a", "b"}
