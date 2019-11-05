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
from bravado.exception import HTTPError
from bravado.exception import HTTPNotFound
from mock import patch
from pytest import mark
from pytest import raises

from paasta_tools.cli import utils
from paasta_tools.marathon_tools import MarathonServiceConfig


@patch("paasta_tools.cli.utils.gethostbyname_ex", autospec=True)
def test_bad_calculate_remote_master(mock_get_by_hostname, system_paasta_config):
    mock_get_by_hostname.side_effect = gaierror(42, "bar")
    ips, output = utils.calculate_remote_masters("myhost", system_paasta_config)
    assert ips == []
    assert "ERROR while doing DNS lookup of paasta-myhost.yelp:\nbar\n" in output


@patch("paasta_tools.cli.utils.gethostbyname_ex", autospec=True)
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
def test_run_paasta_serviceinit_status(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -o StrictHostKeyChecking=no -t fake_master "
        "sudo paasta_serviceinit -s fake_service -i fake_instance status"
    )

    return_code, actual = utils.run_paasta_serviceinit(
        "status",
        "fake_master",
        "fake_service",
        "fake_instance",
        "fake_cluster",
        stream=True,
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY, stream=True)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_serviceinit_status_verbose(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -o StrictHostKeyChecking=no -t fake_master "
        "sudo paasta_serviceinit -s fake_service -i fake_instance -v status"
    )

    return_code, actual = utils.run_paasta_serviceinit(
        "status",
        "fake_master",
        "fake_service",
        "fake_instance",
        "fake_cluster",
        stream=True,
        verbose=1,
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY, stream=True)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_serviceinit_status_verbose_multi(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -o StrictHostKeyChecking=no -t fake_master sudo paasta_serviceinit "
        "-s fake_service -i fake_instance -v -v -v -v status"
    )

    return_code, actual = utils.run_paasta_serviceinit(
        "status",
        "fake_master",
        "fake_service",
        "fake_instance",
        "fake_cluster",
        stream=True,
        verbose=4,
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY, stream=True)
    assert return_code == 0
    assert actual == mock_run.return_value[1]


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


@patch("paasta_tools.cli.utils.calculate_remote_masters", autospec=True)
@patch("paasta_tools.cli.utils.find_connectable_master", autospec=True)
@patch("paasta_tools.cli.utils.run_paasta_serviceinit", autospec=True)
def test_execute_paasta_serviceinit_status_on_remote_master_happy_path(
    mock_run_paasta_serviceinit,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
    system_paasta_config,
):
    cluster = "fake_cluster_name"
    service = "fake_service"
    instancename = "fake_instance"
    remote_masters = ["fake_master1", "fake_master2", "fake_master3"]
    mock_run_paasta_serviceinit.return_value = (
        mock.sentinel.paasta_serviceinit_return_code,
        mock.sentinel.paasta_serviceinit_output,
    )
    mock_calculate_remote_masters.return_value = (remote_masters, None)
    mock_find_connectable_master.return_value = ("fake_connectable_master", None)

    return_code, actual = utils.execute_paasta_serviceinit_on_remote_master(
        "status", cluster, service, instancename, system_paasta_config
    )
    mock_calculate_remote_masters.assert_called_once_with(cluster, system_paasta_config)
    mock_find_connectable_master.assert_called_once_with(remote_masters)
    mock_run_paasta_serviceinit.assert_called_once_with(
        "status", "fake_connectable_master", service, instancename, cluster, False
    )
    assert return_code == mock.sentinel.paasta_serviceinit_return_code
    assert actual == mock.sentinel.paasta_serviceinit_output


@patch("paasta_tools.cli.utils._run", autospec=True)
def test_run_paasta_serviceinit_scaling(mock_run):
    mock_run.return_value = (0, "fake_output")
    expected_command = (
        "ssh -A -o StrictHostKeyChecking=no -t fake_master sudo paasta_serviceinit "
        "-s fake_service -i fake_instance -v --delta 1 status"
    )

    return_value, actual = utils.run_paasta_serviceinit(
        "status",
        "fake_master",
        "fake_service",
        "fake_instance",
        "fake_cluster",
        stream=True,
        verbose=1,
        delta=1,
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY, stream=True)
    assert return_value == 0
    assert actual == mock_run.return_value[1]


@patch("paasta_tools.cli.utils.calculate_remote_masters", autospec=True)
@patch("paasta_tools.cli.utils.find_connectable_master", autospec=True)
@patch("paasta_tools.cli.utils.check_ssh_on_master", autospec=True)
@patch("paasta_tools.cli.utils.run_paasta_serviceinit", autospec=True)
def test_execute_paasta_serviceinit_on_remote_no_connectable_master(
    mock_run_paasta_serviceinit,
    mock_check_ssh_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
    system_paasta_config,
):
    cluster = "fake_cluster_name"
    service = "fake_service"
    instancename = "fake_instance"
    mock_find_connectable_master.return_value = (None, "fake_err_msg")
    mock_calculate_remote_masters.return_value = (["fake_master"], None)

    return_code, actual = utils.execute_paasta_serviceinit_on_remote_master(
        "status", cluster, service, instancename, system_paasta_config
    )
    assert mock_check_ssh_on_master.call_count == 0
    assert "ERROR: could not find connectable master in cluster %s" % cluster in actual
    assert return_code == 255
    assert "fake_err_msg" in actual


@patch("paasta_tools.cli.utils.calculate_remote_masters", autospec=True)
@patch("paasta_tools.cli.utils.find_connectable_master", autospec=True)
@patch("paasta_tools.cli.utils.run_paasta_metastatus", autospec=True)
def test_execute_paasta_metastatus_on_remote_master(
    mock_run_paasta_metastatus,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
    system_paasta_config,
):
    cluster = "fake_cluster_name"
    remote_masters = ["fake_master1", "fake_master2", "fake_master3"]
    mock_run_paasta_metastatus.return_value = (
        mock.sentinel.paasta_metastatus_return_code,
        mock.sentinel.paasta_metastatus_output,
    )
    mock_calculate_remote_masters.return_value = (remote_masters, None)
    mock_find_connectable_master.return_value = ("fake_connectable_master", None)

    return_code, actual = utils.execute_paasta_metastatus_on_remote_master(
        cluster, system_paasta_config, [], 0, False
    )
    mock_calculate_remote_masters.assert_called_once_with(cluster, system_paasta_config)
    mock_find_connectable_master.assert_called_once_with(remote_masters)
    mock_run_paasta_metastatus.assert_called_once_with(
        "fake_connectable_master", [], 0, False, False
    )
    assert return_code == mock.sentinel.paasta_metastatus_return_code
    assert actual == mock.sentinel.paasta_metastatus_output


@patch("paasta_tools.cli.utils.calculate_remote_masters", autospec=True)
@patch("paasta_tools.cli.utils.find_connectable_master", autospec=True)
@patch("paasta_tools.cli.utils.check_ssh_on_master", autospec=True)
@patch("paasta_tools.cli.utils.run_paasta_metastatus", autospec=True)
def test_execute_paasta_metastatus_on_remote_no_connectable_master(
    mock_run_paasta_metastatus,
    mock_check_ssh_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
    system_paasta_config,
):
    cluster = "fake_cluster_name"
    mock_find_connectable_master.return_value = (None, "fake_err_msg")
    mock_calculate_remote_masters.return_value = (["fake_master"], None)

    return_code, actual = utils.execute_paasta_metastatus_on_remote_master(
        cluster, system_paasta_config, [], 0
    )
    assert mock_check_ssh_on_master.call_count == 0
    assert "ERROR: could not find connectable master in cluster %s" % cluster in actual
    assert return_code == 255
    assert "fake_err_msg" in actual


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


def test_modules_in_pkg():
    from paasta_tools.cli import cmds

    ret = tuple(utils.modules_in_pkg(cmds))
    assert "__init__" not in ret
    assert "cook_image" in ret
    assert "list_clusters" in ret


@mock.patch("paasta_tools.cli.utils.INSTANCE_TYPE_HANDLERS", dict(), autospec=None)
@mock.patch("paasta_tools.cli.utils.validate_service_instance", autospec=True)
def test_get_instance_config_by_instance_type(mock_validate_service_instance,):
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
def test_get_instance_config_unknown(mock_validate_service_instance,):
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


@patch("paasta_tools.cli.utils.client", autospec=True)
def test_get_status_for_instance(mock_client):
    mock_client.get_paasta_api_client.return_value = None
    with raises(SystemExit):
        utils.get_status_for_instance("cluster1", "my-service", "main")
    mock_client.get_paasta_api_client.assert_called_with(cluster="cluster1")
    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_result = mock.Mock(return_value=mock.Mock(marathon=False))
    mock_api.service.status_instance.return_value = mock.Mock(result=mock_result)
    with raises(SystemExit):
        utils.get_status_for_instance("cluster1", "my-service", "main")
    mock_result = mock.Mock(return_value=mock.Mock(marathon=True))
    mock_api.service.status_instance.return_value = mock.Mock(result=mock_result)
    utils.get_status_for_instance("cluster1", "my-service", "main")
    mock_api.service.status_instance.assert_called_with(
        service="my-service", instance="main"
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
def test_list_deploy_groups_parses_configs(mock_get_instance_configs_for_service,):
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


@patch("paasta_tools.cli.utils.client", autospec=True)
def test_get_task_from_instance(mock_client):
    mock_client.get_paasta_api_client.return_value = None
    with raises(utils.PaastaTaskNotFound):
        utils.get_task_from_instance("cluster1", "my-service", "main")
    mock_client.get_paasta_api_client.assert_called_with(cluster="cluster1")
    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_task_1 = mock.Mock()
    mock_task_2 = mock.Mock()
    mock_tasks = [mock_task_1, mock_task_2]
    mock_result = mock.Mock(return_value=mock_tasks)
    mock_task_result = mock.Mock(return_value=mock_task_2)
    mock_api.service.tasks_instance.return_value = mock.Mock(result=mock_result)
    mock_api.service.task_instance.return_value = mock.Mock(result=mock_task_result)
    ret = utils.get_task_from_instance("cluster1", "my-service", "main", task_id="123")
    assert ret == mock_task_2
    mock_api.service.task_instance.assert_called_with(
        service="my-service", instance="main", verbose=True, task_id="123"
    )

    ret = utils.get_task_from_instance("cluster1", "my-service", "main")
    assert ret == mock_task_1
    mock_api.service.tasks_instance.assert_called_with(
        service="my-service", instance="main", verbose=True, slave_hostname=None
    )

    mock_result = mock.Mock(return_value=[])
    mock_api.service.tasks_instance.return_value = mock.Mock(result=mock_result)
    with raises(utils.PaastaTaskNotFound):
        ret = utils.get_task_from_instance(
            "cluster1", "my-service", "main", slave_hostname="test"
        )

    mock_api.service.tasks_instance.side_effect = HTTPError(
        response=mock.Mock(status_code=500)
    )
    with raises(utils.PaastaTaskNotFound):
        ret = utils.get_task_from_instance(
            "cluster1", "my-service", "main", slave_hostname="test"
        )
    mock_api.service.tasks_instance.assert_called_with(
        service="my-service", instance="main", verbose=True, slave_hostname="test"
    )

    mock_api.service.tasks_instance.side_effect = HTTPNotFound(
        response=mock.Mock(status_code=404)
    )
    with raises(utils.PaastaTaskNotFound):
        ret = utils.get_task_from_instance(
            "cluster1", "my-service", "main", slave_hostname="test"
        )


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
