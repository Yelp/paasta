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
import mock
from pytest import raises

from paasta_tools import long_running_service_tools
from paasta_tools.utils import InvalidInstanceConfig


class TestLongRunningServiceConfig:
    def test_get_healthcheck_cmd_happy(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            config_dict={"healthcheck_cmd": "/bin/true"},
            instance="fake_instance",
            branch_dict=None,
        )
        actual = fake_conf.get_healthcheck_cmd()
        assert actual == "/bin/true"

    def test_get_healthcheck_cmd_raises_when_unset(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
        )
        with raises(InvalidInstanceConfig) as exc:
            fake_conf.get_healthcheck_cmd()
        assert "healthcheck mode 'cmd' requires a healthcheck_cmd to run" in str(
            exc.value
        )

    def test_get_healthcheck_for_instance_http(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666

        fake_path = "/fake_path"
        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={},
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig(
                {"mode": "http", "healthcheck_uri": fake_path}
            )
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ), mock.patch("socket.getfqdn", autospec=True, return_value=fake_hostname):
            expected = (
                "http",
                "http://%s:%d%s" % (fake_hostname, fake_random_port, fake_path),
            )
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_service_config, fake_random_port
            )
            assert expected == actual

    def test_get_healthcheck_for_instance_not_matching_mode(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666

        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={},
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig({"mode": "http"})
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ), mock.patch("socket.getfqdn", autospec=True, return_value=fake_hostname):
            expected = ("http", "http://fake_hostname:666/status")
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_service_config, fake_random_port
            )
            assert expected == actual

    def test_get_healthcheck_for_instance_tcp(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666

        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={},
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig({"mode": "tcp"})
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ), mock.patch("socket.getfqdn", autospec=True, return_value=fake_hostname):
            expected = ("tcp", "tcp://%s:%d" % (fake_hostname, fake_random_port))
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_service_config, fake_random_port
            )
            assert expected == actual

    def test_get_healthcheck_for_instance_cmd(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666
        fake_cmd = "/bin/fake_command"
        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={
                "instances": 1,
                "healthcheck_mode": "cmd",
                "healthcheck_cmd": fake_cmd,
            },
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig({})
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ), mock.patch("socket.getfqdn", autospec=True, return_value=fake_hostname):
            expected = ("cmd", fake_cmd)
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_service_config, fake_random_port
            )
            assert expected == actual

    def test_get_healthcheck_for_instance_other(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666
        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={"healthcheck_mode": None},
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig({})
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ), mock.patch("socket.getfqdn", autospec=True, return_value=fake_hostname):
            expected = (None, None)
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_service_config, fake_random_port
            )
            assert expected == actual

    def test_get_healthcheck_for_instance_custom_soadir(self):
        fake_service = "fake_service"
        fake_namespace = "fake_namespace"
        fake_hostname = "fake_hostname"
        fake_random_port = 666
        fake_soadir = "/fake/soadir"
        fake_service_config = long_running_service_tools.LongRunningServiceConfig(
            service=fake_service,
            cluster="fake_cluster",
            instance=fake_namespace,
            config_dict={"healthcheck_mode": None},
            branch_dict=None,
        )
        fake_service_namespace_config = (
            long_running_service_tools.ServiceNamespaceConfig({})
        )
        with mock.patch(
            "paasta_tools.long_running_service_tools.load_service_namespace_config",
            autospec=True,
            return_value=fake_service_namespace_config,
        ) as load_service_namespace_config_patch, mock.patch(
            "socket.getfqdn", autospec=True, return_value=fake_hostname
        ):
            expected = (None, None)
            actual = long_running_service_tools.get_healthcheck_for_instance(
                fake_service,
                fake_namespace,
                fake_service_config,
                fake_random_port,
                soa_dir=fake_soadir,
            )
            assert expected == actual
            load_service_namespace_config_patch.assert_called_once_with(
                fake_service, fake_namespace, fake_soadir
            )

    def test_get_instances_in_config(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={"instances": -10},
            branch_dict={
                "desired_state": "start",
                "git_sha": "c0ded00d",
                "docker_image": "docker_image",
                "image_version": None,
                "force_bounce": None,
            },
        )
        assert fake_conf.get_instances() == -10

    def test_get_instances_default(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
        )
        assert fake_conf.get_instances() == 1

    def test_get_instances_respects_false(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={"instances": False},
            branch_dict={
                "desired_state": "start",
                "git_sha": "c0debabe",
                "docker_image": "docker_image",
                "image_version": None,
                "force_bounce": None,
            },
        )
        assert fake_conf.get_instances() == 0

    def test_validate_with_bad_registration(self):
        fake_conf = long_running_service_tools.LongRunningServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={
                "registrations": ["fake_name.fake_instance", "bad_registration"],
                "deploy_group": None,
            },
            branch_dict=None,
        )
        error_messages = fake_conf.validate()
        assert "bad_registration" in error_messages[0]


class TestServiceNamespaceConfig:
    def test_get_mode_default(self):
        assert long_running_service_tools.ServiceNamespaceConfig().get_mode() is None

    def test_get_mode_default_when_port_specified(self):
        config = {"proxy_port": 1234}
        assert (
            long_running_service_tools.ServiceNamespaceConfig(config).get_mode()
            == "http"
        )

    def test_get_mode_valid(self):
        config = {"mode": "tcp"}
        assert (
            long_running_service_tools.ServiceNamespaceConfig(config).get_mode()
            == "tcp"
        )

    def test_get_mode_invalid(self):
        config = {"mode": "paasta"}
        with raises(long_running_service_tools.InvalidSmartstackMode):
            long_running_service_tools.ServiceNamespaceConfig(config).get_mode()

    def test_get_healthcheck_uri_default(self):
        assert (
            long_running_service_tools.ServiceNamespaceConfig().get_healthcheck_uri()
            == "/status"
        )

    def test_get_discover_default(self):
        assert (
            long_running_service_tools.ServiceNamespaceConfig().get_discover()
            == "region"
        )


def test_get_proxy_port_for_instance():
    mock_config = mock.Mock(
        get_registrations=mock.Mock(return_value=["thing.main.sha.sha"]),
        soa_dir="/nail/blah",
    )
    with mock.patch(
        "paasta_tools.long_running_service_tools.load_service_namespace_config",
        autospec=True,
    ) as mock_load_service_namespace_config:
        mock_load_service_namespace_config.return_value = {"proxy_port": 1234}
        assert (
            long_running_service_tools.get_proxy_port_for_instance(mock_config) == 1234
        )
        mock_load_service_namespace_config.assert_called_once_with(
            service="thing", namespace="main", soa_dir="/nail/blah"
        )


def test_host_passes_blacklist_passes():
    slave_attributes = {"fake_attribute": "fake_value_1"}
    blacklist = [("fake_attribute", "No what we have here"), ("foo", "bar")]
    actual = long_running_service_tools.host_passes_blacklist(
        host_attributes=slave_attributes, blacklist=blacklist
    )
    assert actual is True


def test_host_passes_blacklist_blocks_blacklisted_locations():
    slave_attributes = {"fake_attribute": "fake_value_1"}
    blacklist = [("fake_attribute", "fake_value_1")]
    actual = long_running_service_tools.host_passes_blacklist(
        host_attributes=slave_attributes, blacklist=blacklist
    )
    assert actual is False


def test_host_passes_whitelist():
    fake_slave_attributes = {
        "location_type": "fake_location",
        "fake_location_type": "fake_location",
    }
    fake_whitelist_allow = ("fake_location_type", ["fake_location"])
    fake_whitelist_deny = ("anoterfake_location_type", ["anotherfake_location"])

    slave_passes = long_running_service_tools.host_passes_whitelist(
        fake_slave_attributes, fake_whitelist_deny
    )
    assert not slave_passes
    slave_passes = long_running_service_tools.host_passes_whitelist(
        fake_slave_attributes, fake_whitelist_allow
    )
    assert slave_passes
    slave_passes = long_running_service_tools.host_passes_whitelist(
        fake_slave_attributes, None
    )
    assert slave_passes


def test_get_all_namespaces_for_service():
    name = "vvvvvv"
    soa_dir = "^_^"
    t1_dict = {"hollo": "werld", "smark": "stact"}
    t2_dict = {"vataman": "witir", "sin": "chaps"}
    fake_smartstack = {"smartstack": {"t1": t1_dict, "t2": t2_dict}}
    expected = [("vvvvvv.t2", t2_dict), ("vvvvvv.t1", t1_dict)]
    expected_short = [("t2", t2_dict), ("t1", t1_dict)]
    with mock.patch(
        "service_configuration_lib.read_service_configuration",
        autospec=True,
        return_value=fake_smartstack,
    ) as read_service_configuration_patch:
        actual = long_running_service_tools.get_all_namespaces_for_service(
            name, soa_dir
        )
        read_service_configuration_patch.assert_any_call(name, soa_dir)
        assert sorted(expected) == sorted(actual)

        actual_short = long_running_service_tools.get_all_namespaces_for_service(
            name, soa_dir, False
        )
        read_service_configuration_patch.assert_any_call(name, soa_dir)
        assert sorted(expected_short) == sorted(actual_short)


def test_get_all_namespaces():
    soa_dir = "carbon"
    namespaces = [
        [("aluminum", {"hydrogen": 1}), ("potassium", {"helium": 2})],
        [("uranium", {"lithium": 3}), ("gold", {"boron": 5})],
    ]
    expected = [
        ("uranium", {"lithium": 3}),
        ("gold", {"boron": 5}),
        ("aluminum", {"hydrogen": 1}),
        ("potassium", {"helium": 2}),
    ]
    with mock.patch(
        "os.path.abspath", autospec=True, return_value="oxygen"
    ) as abspath_patch, mock.patch(
        "os.listdir", autospec=True, return_value=["rid1", "rid2"]
    ) as listdir_patch, mock.patch(
        "paasta_tools.long_running_service_tools.get_all_namespaces_for_service",
        autospec=True,
        side_effect=lambda a, b: namespaces.pop(),
    ) as get_namespaces_patch:
        actual = long_running_service_tools.get_all_namespaces(soa_dir)
        assert expected == actual
        abspath_patch.assert_called_once_with(soa_dir)
        listdir_patch.assert_called_once_with("oxygen")
        get_namespaces_patch.assert_any_call("rid1", soa_dir)
        get_namespaces_patch.assert_any_call("rid2", soa_dir)
        assert get_namespaces_patch.call_count == 2


def test_get_expected_instance_count_for_namespace():
    service = "red"
    namespace = "rojo"
    soa_dir = "que_esta"
    fake_job_configs = [
        long_running_service_tools.LongRunningServiceConfig(
            service=service,
            cluster="fake_cluster",
            instance="blue",
            config_dict={"nerve_ns": "rojo", "instances": 11},
            branch_dict=None,
        ),
        long_running_service_tools.LongRunningServiceConfig(
            service=service,
            cluster="fake_cluster",
            instance="green",
            config_dict={"nerve_ns": "amarillo"},
            branch_dict=None,
        ),
    ]

    with mock.patch(
        "paasta_tools.long_running_service_tools.PaastaServiceConfigLoader",
        autospec=True,
        return_value=mock.Mock(
            instance_configs=mock.Mock(return_value=fake_job_configs)
        ),
    ) as fake_pscl:
        actual = long_running_service_tools.get_expected_instance_count_for_namespace(
            service,
            namespace,
            cluster="fake_cluster",
            soa_dir=soa_dir,
            instance_type_class=long_running_service_tools.LongRunningServiceConfig,
        )
        assert actual == 11
        fake_pscl.return_value.instance_configs.assert_called_once_with(
            cluster="fake_cluster",
            instance_type_class=long_running_service_tools.LongRunningServiceConfig,
        )
