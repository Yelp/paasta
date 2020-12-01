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
import json

import docker
import mock
from pytest import raises

from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.cli.cli import main
from paasta_tools.cli.cmds.local_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.local_run import docker_pull_image
from paasta_tools.cli.cmds.local_run import format_command_for_type
from paasta_tools.cli.cmds.local_run import get_container_id
from paasta_tools.cli.cmds.local_run import get_container_name
from paasta_tools.cli.cmds.local_run import get_docker_run_cmd
from paasta_tools.cli.cmds.local_run import get_local_run_environment_vars
from paasta_tools.cli.cmds.local_run import LostContainerException
from paasta_tools.cli.cmds.local_run import paasta_local_run
from paasta_tools.cli.cmds.local_run import perform_cmd_healthcheck
from paasta_tools.cli.cmds.local_run import perform_http_healthcheck
from paasta_tools.cli.cmds.local_run import perform_tcp_healthcheck
from paasta_tools.cli.cmds.local_run import run_docker_container
from paasta_tools.cli.cmds.local_run import run_healthcheck_on_container
from paasta_tools.cli.cmds.local_run import simulate_healthcheck_on_service
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.util.timeout import TimeoutError
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import SystemPaastaConfig


@mock.patch("paasta_tools.cli.cmds.local_run.figure_out_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.load_system_paasta_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.paasta_cook_image", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True)
def test_dry_run(
    mock_get_instance_config,
    mock_validate_service_instance,
    mock_paasta_cook_image,
    mock_load_system_paasta_config,
    mock_figure_out_service_name,
    capfd,
    system_paasta_config,
):
    mock_get_instance_config.return_value.get_cmd.return_value = "fake_command"
    mock_validate_service_instance.return_value = "marathon"
    mock_paasta_cook_image.return_value = 0
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_figure_out_service_name.return_value = "fake_service"

    # Should pass and produce something
    with raises(SystemExit) as excinfo:
        main(
            (
                "local-run",
                "--dry-run",
                "--cluster",
                "fake_cluster",
                "--instance",
                "fake_instance",
            )
        )
    ret = excinfo.value.code
    out, err = capfd.readouterr()
    assert ret == 0

    # We don't care what the contents are, we just care that it is json loadable.
    expected_out = json.loads(out)
    assert isinstance(expected_out, list)


@mock.patch("paasta_tools.cli.cmds.local_run.figure_out_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.load_system_paasta_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.paasta_cook_image", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True)
def test_dry_run_json_dict(
    mock_get_instance_config,
    mock_validate_service_instance,
    mock_paasta_cook_image,
    mock_load_system_paasta_config,
    mock_figure_out_service_name,
    capfd,
    system_paasta_config,
):
    mock_get_instance_config.return_value.get_cmd.return_value = "fake_command"
    mock_get_instance_config.return_value.format_docker_parameters.return_value = {}
    mock_get_instance_config.return_value.get_env_dictionary.return_value = {}
    mock_get_instance_config.return_value.get_mem.return_value = 123
    mock_get_instance_config.return_value.get_disk.return_value = 123
    mock_get_instance_config.return_value.get_cpu.return_value = 123
    mock_get_instance_config.return_value.get_net.return_value = "fake_net"
    mock_get_instance_config.return_value.get_docker_image.return_value = (
        "fake_docker_image"
    )
    mock_get_instance_config.return_value.get_docker_url.return_value = (
        "fake_registry/fake_docker_image"
    )
    mock_get_instance_config.return_value.get_container_port.return_value = 8888
    mock_validate_service_instance.return_value = "marathon"
    mock_paasta_cook_image.return_value = 0
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_figure_out_service_name.return_value = "fake_service"

    # Should pass and produce something
    with raises(SystemExit) as excinfo:
        main(
            (
                "local-run",
                "--dry-run",
                "--cluster",
                "fake_cluster",
                "--instance",
                "fake_instance",
                "--json-dict",
            )
        )
    ret = excinfo.value.code
    out, err = capfd.readouterr()
    assert ret == 0

    # Ensure it's a dict and check some keys
    expected_out = json.loads(out)
    assert isinstance(expected_out, dict)
    assert "docker_hash" in expected_out
    assert "interactive" in expected_out


@mock.patch("paasta_tools.cli.cmds.local_run.execute_in_container", autospec=True)
def test_perform_cmd_healthcheck_success(mock_exec_container):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_cmd = "/bin/false"
    fake_timeout = 10
    mock_exec_container.return_value = ("fake_output", 0)
    assert perform_cmd_healthcheck(
        mock_docker_client, fake_container_id, fake_cmd, fake_timeout
    )
    mock_exec_container.assert_called_with(
        mock_docker_client, fake_container_id, fake_cmd, fake_timeout
    )


@mock.patch("socket.socket.connect_ex", autospec=None)
def test_perform_tcp_healthcheck_success(mock_socket_connect):
    fake_tcp_url = "tcp://fakehost:1234"
    fake_timeout = 10
    mock_socket_connect.return_value = 0
    assert perform_tcp_healthcheck(fake_tcp_url, fake_timeout)
    mock_socket_connect.assert_called_with(("fakehost", 1234))


@mock.patch("socket.socket.connect_ex", autospec=None)
def test_perform_tcp_healthcheck_failure(mock_socket_connect):
    fake_tcp_url = "tcp://fakehost:1234"
    fake_timeout = 10
    mock_socket_connect.return_value = 1
    actual = perform_tcp_healthcheck(fake_tcp_url, fake_timeout)
    assert actual[0] is False
    assert "timeout" in actual[1]
    assert "10 seconds" in actual[1]


@mock.patch("requests.get", autospec=True)
def test_perform_http_healthcheck_success(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(status_code=200, headers={})
    assert perform_http_healthcheck(fake_http_url, fake_timeout)
    mock_http_conn.assert_called_once_with(fake_http_url, verify=False)


@mock.patch("requests.get", autospec=True)
def test_perform_http_healthcheck_failure_known_high(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(status_code=400, headers={})
    result, reason = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert result is False
    assert "400" in reason
    mock_http_conn.assert_called_once_with(fake_http_url, verify=False)


@mock.patch("requests.get", autospec=True)
def test_perform_http_healthcheck_failure_known_low(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(status_code=100, headers={})
    result, reason = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert result is False
    assert "100" in reason
    mock_http_conn.assert_called_once_with(fake_http_url, verify=False)


@mock.patch("requests.get", side_effect=TimeoutError, autospec=True)
def test_perform_http_healthcheck_timeout(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    actual = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert actual[0] is False
    assert "10" in actual[1]
    assert "timed out" in actual[1]
    mock_http_conn.assert_called_once_with(fake_http_url, verify=False)


@mock.patch("requests.get", autospec=True)
def test_perform_http_healthcheck_failure_with_multiple_content_type(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(
        status_code=200,
        headers={"content-type": "fake_content_type_1, fake_content_type_2"},
    )
    actual = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert actual[0] is False
    assert "200" in actual[1]
    mock_http_conn.assert_called_once_with(fake_http_url, verify=False)


@mock.patch("paasta_tools.cli.cmds.local_run.perform_http_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_http_success(mock_sleep, mock_perform_http_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fakehost:666/fake_status_path"
    fake_timeout = 10

    mock_perform_http_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout
    )
    mock_perform_http_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch("paasta_tools.cli.cmds.local_run.perform_http_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_http_fails(mock_sleep, mock_perform_http_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fakehost:666/fake_status_path"
    fake_timeout = 10

    mock_perform_http_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout
    )
    mock_perform_http_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch("paasta_tools.cli.cmds.local_run.perform_tcp_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_tcp_success(mock_sleep, mock_perform_tcp_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "tcp"
    fake_url = "tcp://fakehost:666"
    fake_timeout = 10

    mock_perform_tcp_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout
    )
    assert mock_perform_tcp_healthcheck.call_count == 1
    mock_perform_tcp_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch("paasta_tools.cli.cmds.local_run.perform_tcp_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_tcp_fails(mock_sleep, mock_perform_tcp_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "tcp"
    fake_url = "tcp://fakehost:666"
    fake_timeout = 10

    mock_perform_tcp_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout
    )
    assert mock_perform_tcp_healthcheck.call_count == 1
    mock_perform_tcp_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch("paasta_tools.cli.cmds.local_run.perform_cmd_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_cmd_success(mock_sleep, mock_perform_cmd_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "cmd"
    fake_cmd = "/bin/true"
    fake_timeout = 10

    mock_perform_cmd_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_cmd, fake_timeout
    )
    assert mock_perform_cmd_healthcheck.call_count == 1
    mock_perform_cmd_healthcheck.assert_called_once_with(
        mock_docker_client, fake_container_id, fake_cmd, fake_timeout
    )


@mock.patch("paasta_tools.cli.cmds.local_run.perform_cmd_healthcheck", autospec=True)
@mock.patch("time.sleep", autospec=True)
def test_run_healthcheck_cmd_fails(mock_sleep, mock_perform_cmd_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = "fake_container_id"
    fake_mode = "cmd"
    fake_cmd = "/bin/true"
    fake_timeout = 10

    mock_perform_cmd_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_cmd, fake_timeout
    )
    assert mock_perform_cmd_healthcheck.call_count == 1
    mock_perform_cmd_healthcheck.assert_called_once_with(
        mock_docker_client, fake_container_id, fake_cmd, fake_timeout
    )


@mock.patch(
    "paasta_tools.cli.cmds.local_run.randint", autospec=True, return_value=543534
)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_username",
    autospec=True,
    return_value="fsmonste",
)
def test_get_container_name(mock_get_username, mock_randint):
    expected = "paasta_local_run_{}_{}".format(
        mock_get_username.return_value, mock_randint.return_value
    )
    actual = get_container_name()
    assert actual == expected


@mock.patch("paasta_tools.cli.cmds.local_run.run_docker_container", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True)
def test_configure_and_run_command_uses_cmd_from_config(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_run_docker_container,
    system_paasta_config,
):
    mock_validate_service_instance.return_value = "marathon"
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_get_instance_config.return_value.get_cmd.return_value = "fake_command"
    mock_run_docker_container.return_value = 0

    fake_service = "fake_service"
    docker_url = "8" * 40
    args = mock.MagicMock()
    args.cmd = ""
    args.service = fake_service
    args.instance = "fake_instance"
    args.healthcheck = False
    args.healthcheck_only = False
    args.user_port = None
    args.interactive = False
    args.dry_run_json_dict = False
    args.vault_auth_method = "ldap"
    args.vault_token_file = "/blah/token"
    args.skip_secrets = False

    mock_secret_provider_kwargs = {
        "vault_cluster_config": {},
        "vault_auth_method": "ldap",
        "vault_token_file": "/blah/token",
    }

    return_code = configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_url=docker_url,
        docker_sha=None,
        service=fake_service,
        instance="fake_instance",
        cluster="fake_cluster",
        system_paasta_config=system_paasta_config,
        args=args,
    )
    assert return_code == 0
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        framework="marathon",
        docker_url=docker_url,
        volumes=[],
        interactive=args.interactive,
        command=mock_get_instance_config.return_value.get_cmd.return_value,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        user_port=args.user_port,
        instance_config=mock_get_instance_config.return_value,
        secret_provider_name="paasta_tools.secret_providers",
        soa_dir=args.yelpsoa_config_root,
        dry_run=False,
        json_dict=False,
        secret_provider_kwargs=mock_secret_provider_kwargs,
        skip_secrets=False,
    )


@mock.patch("paasta_tools.cli.cmds.local_run.run_docker_container", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True)
def test_configure_and_run_uses_bash_by_default_when_interactive(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_run_docker_container,
    system_paasta_config,
):
    mock_validate_service_instance.return_value = "marathon"
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_run_docker_container.return_value = 0

    fake_service = "fake_service"
    docker_url = "8" * 40
    args = mock.MagicMock()
    args.cmd = None
    args.service = fake_service
    args.healthcheck = False
    args.healthcheck_only = False
    args.user_port = None
    args.instance = "fake_instance"
    args.interactive = True
    args.dry_run_json_dict = False
    args.vault_auth_method = "ldap"
    args.vault_token_file = "/blah/token"
    args.skip_secrets = False

    return_code = configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_url=docker_url,
        docker_sha=None,
        service=fake_service,
        instance="fake_instance",
        cluster="fake_cluster",
        system_paasta_config=system_paasta_config,
        args=args,
    )
    assert return_code == 0
    mock_secret_provider_kwargs = {
        "vault_cluster_config": {},
        "vault_auth_method": "ldap",
        "vault_token_file": "/blah/token",
    }
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        framework="marathon",
        docker_url=docker_url,
        volumes=[],
        interactive=args.interactive,
        command="bash",
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        user_port=args.user_port,
        instance_config=mock_get_instance_config.return_value,
        secret_provider_name="paasta_tools.secret_providers",
        soa_dir=args.yelpsoa_config_root,
        dry_run=False,
        json_dict=False,
        secret_provider_kwargs=mock_secret_provider_kwargs,
        skip_secrets=False,
    )


@mock.patch("paasta_tools.cli.cmds.local_run.docker_pull_image", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.run_docker_container", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True)
def test_configure_and_run_pulls_image_when_asked(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_run_docker_container,
    mock_docker_pull_image,
    system_paasta_config,
):
    mock_validate_service_instance.return_value = "marathon"
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_run_docker_container.return_value = 0

    fake_instance_config = mock.MagicMock(InstanceConfig)
    fake_instance_config.get_docker_registry.return_value = "fake_registry"
    fake_instance_config.get_docker_image.return_value = "fake_image"
    fake_instance_config.get_docker_url.return_value = "fake_registry/fake_image"
    mock_get_instance_config.return_value = fake_instance_config
    fake_service = "fake_service"
    args = mock.MagicMock()
    args.cmd = None
    args.service = fake_service
    args.instance = "fake_instance"
    args.healthcheck = False
    args.healthcheck_only = False
    args.user_port = None
    args.interactive = True
    args.dry_run_json_dict = False
    args.vault_auth_method = "ldap"
    args.vault_token_file = "/blah/token"
    args.skip_secrets = False

    return_code = configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_url=None,
        docker_sha=None,
        service=fake_service,
        instance="fake_instance",
        cluster="fake_cluster",
        args=args,
        system_paasta_config=system_paasta_config,
        pull_image=True,
    )
    assert return_code == 0
    mock_docker_pull_image.assert_called_once_with("fake_registry/fake_image")
    mock_secret_provider_kwargs = {
        "vault_cluster_config": {},
        "vault_auth_method": "ldap",
        "vault_token_file": "/blah/token",
    }
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        framework="marathon",
        docker_url="fake_registry/fake_image",
        volumes=[],
        interactive=args.interactive,
        command="bash",
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        user_port=args.user_port,
        instance_config=mock_get_instance_config.return_value,
        secret_provider_name="paasta_tools.secret_providers",
        soa_dir=args.yelpsoa_config_root,
        dry_run=False,
        json_dict=False,
        secret_provider_kwargs=mock_secret_provider_kwargs,
        skip_secrets=False,
    )


def test_configure_and_run_docker_container_defaults_to_interactive_instance(
    system_paasta_config,
):
    with mock.patch(
        "paasta_tools.cli.cmds.local_run.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True
    ) as mock_validate_service_instance, mock.patch(
        "paasta_tools.cli.cmds.local_run.run_docker_container",
        autospec=True,
        return_value=0,
    ) as mock_run_docker_container, mock.patch(
        "paasta_tools.cli.cmds.local_run.get_default_interactive_config", autospec=True
    ) as mock_get_default_interactive_config:
        mock_stdin.isatty.return_value = True
        mock_validate_service_instance.side_effect = NoConfigurationForServiceError
        mock_docker_client = mock.MagicMock(spec_set=docker.Client)

        args = mock.MagicMock()
        args.cmd = None
        args.service = "fake_service"
        args.healthcheck = False
        args.healthcheck_only = False
        args.user_port = None
        args.interactive = False
        args.dry_run_json_dict = False
        args.vault_auth_method = "ldap"
        args.vault_token_file = "/blah/token"
        args.skip_secrets = False

        mock_config = mock.create_autospec(AdhocJobConfig)
        mock_get_default_interactive_config.return_value = mock_config
        return_code = configure_and_run_docker_container(
            docker_client=mock_docker_client,
            docker_url="fake_hash",
            docker_sha=None,
            service="fake_service",
            instance=None,
            cluster="fake_cluster",
            args=args,
            system_paasta_config=system_paasta_config,
        )
        assert return_code == 0
        mock_secret_provider_kwargs = {
            "vault_cluster_config": {},
            "vault_auth_method": "ldap",
            "vault_token_file": "/blah/token",
        }
        mock_run_docker_container.assert_called_once_with(
            docker_client=mock_docker_client,
            service="fake_service",
            instance="interactive",
            framework="adhoc",
            docker_url="fake_hash",
            volumes=[],
            interactive=True,
            command="bash",
            healthcheck=args.healthcheck,
            healthcheck_only=args.healthcheck_only,
            user_port=args.user_port,
            instance_config=mock_config,
            secret_provider_name="paasta_tools.secret_providers",
            soa_dir=args.yelpsoa_config_root,
            dry_run=False,
            json_dict=False,
            secret_provider_kwargs=mock_secret_provider_kwargs,
            skip_secrets=False,
        )


def test_configure_and_run_docker_container_respects_docker_sha(system_paasta_config,):
    with mock.patch(
        "paasta_tools.cli.cmds.local_run.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.cli.cmds.local_run.validate_service_instance", autospec=True
    ) as mock_validate_service_instance, mock.patch(
        "paasta_tools.cli.cmds.local_run.run_docker_container",
        autospec=True,
        return_value=0,
    ) as mock_run_docker_container, mock.patch(
        "paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True
    ) as mock_get_default_interactive_config, mock.patch(
        "paasta_tools.utils.get_service_docker_registry",
        autospec=True,
        return_value="fake_registry",
    ):
        mock_stdin.isatty.return_value = True
        mock_validate_service_instance.return_value = "adhoc"
        mock_docker_client = mock.MagicMock(spec_set=docker.Client)

        args = mock.MagicMock()
        args.cmd = None
        args.service = "fake_service"
        args.healthcheck = False
        args.healthcheck_only = False
        args.user_port = None
        args.interactive = False
        args.dry_run_json_dict = False
        args.vault_auth_method = "ldap"
        args.vault_token_file = "/blah/token"
        args.skip_secrets = False

        fake_config = AdhocJobConfig(
            service="fake_service",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
            soa_dir="fake_soa_dir",
        )

        mock_get_default_interactive_config.return_value = fake_config
        return_code = configure_and_run_docker_container(
            docker_client=mock_docker_client,
            docker_url=None,
            docker_sha="abcdefg",
            service="fake_service",
            instance="fake_instance",
            cluster="fake_cluster",
            args=args,
            system_paasta_config=system_paasta_config,
        )
        expected = "fake_registry/services-fake_service:paasta-abcdefg"
        assert mock_run_docker_container.call_args[1]["docker_url"] == expected
        assert return_code == 0


@mock.patch("paasta_tools.cli.cmds.local_run.figure_out_service_name", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.configure_and_run_docker_container", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_docker_client",
    spec_set=docker.Client,
    autospec=None,
)
@mock.patch("paasta_tools.cli.cmds.cook_image.validate_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image.makefile_responds_to", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._run", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.os.geteuid", autospec=True)
def test_run_success(
    mock_os_geteuid,
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
    mock_Client,
    mock_run_docker_container,
    mock_figure_out_service_name,
):
    mock_os_geteuid.return_value = 0
    mock_run.return_value = (0, "Output")
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True
    mock_Client.return_value = None
    mock_run_docker_container.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"

    args = mock.MagicMock()
    args.service = "fake_service"
    args.healthcheck = False
    args.interactive = False
    args.action = "pull"
    assert paasta_local_run(args) is None


@mock.patch("paasta_tools.cli.cmds.local_run.figure_out_service_name", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.configure_and_run_docker_container", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_docker_client",
    spec_set=docker.Client,
    autospec=None,
)
@mock.patch("paasta_tools.cli.cmds.local_run.paasta_cook_image", autospec=True)
def test_run_cook_image_fails(
    mock_paasta_cook_image,
    mock_Client,
    mock_run_docker_container,
    mock_figure_out_service_name,
):
    mock_paasta_cook_image.return_value = 1
    mock_Client.return_value = None
    mock_run_docker_container.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"

    args = mock.MagicMock()
    args.service = "fake_service"
    args.healthcheck = False
    args.interactive = False
    args.action = "build"
    assert paasta_local_run(args) == 1
    assert not mock_run_docker_container.called


def test_get_docker_run_cmd_without_additional_args():
    memory = 555
    chosen_port = 666
    container_port = 8888
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = False
    docker_url = "8" * 40
    command = None
    net = "bridge"
    docker_params = []
    detach = False
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    # Since we can't assert that the command isn't present in the output, we do
    # the next best thing and check that the docker hash is the last thing in
    # the docker run command (the command would have to be after it if it existed)
    assert actual[-1] == docker_url


def test_get_docker_run_cmd_with_env_vars():
    memory = 555
    chosen_port = 666
    container_port = 8888
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {"foo": "bar", "baz": "qux", "x": " with spaces"}
    interactive = False
    docker_url = "8" * 40
    command = None
    net = "bridge"
    docker_params = []
    detach = False
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert actual[actual.index("foo") - 1] == "--env"
    assert actual[actual.index("baz") - 1] == "--env"


def test_get_docker_run_cmd_interactive_false():
    memory = 555
    chosen_port = 666
    container_port = 8888
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = False
    docker_url = "8" * 40
    command = "IE9.exe /VERBOSE /ON_ERROR_RESUME_NEXT"
    net = "bridge"
    docker_params = []
    detach = False
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert "--memory=%dm" % memory in actual
    assert any(["--publish=%s" % chosen_port in arg for arg in actual])
    assert "--name=%s" % container_name in actual
    assert all(["--volume=%s" % volume in actual for volume in volumes])
    assert "--interactive=true" not in actual
    assert "--tty=true" not in actual
    assert docker_url in actual
    assert command in " ".join(actual)


def test_get_docker_run_cmd_interactive_true():
    memory = 555
    chosen_port = 666
    container_port = 8888
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = True
    docker_url = "8" * 40
    command = "IE9.exe /VERBOSE /ON_ERROR_RESUME_NEXT"
    net = "bridge"
    docker_params = []
    detach = False
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert "--interactive=true" in actual
    assert "--detach" not in actual


def test_get_docker_run_docker_params():
    memory = 555
    container_port = 8888
    chosen_port = 666
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = False
    docker_url = "8" * 40
    command = "IE9.exe /VERBOSE /ON_ERROR_RESUME_NEXT"
    net = "bridge"
    docker_params = [
        {"key": "memory-swap", "value": "%sm" % memory},
        {"key": "cpu-period", "value": "200000"},
        {"key": "cpu-quota", "value": "150000"},
    ]
    detach = True
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert "--memory-swap=555m" in actual
    assert "--cpu-period=200000" in actual
    assert "--cpu-quota=150000" in actual


def test_get_docker_run_cmd_host_networking():
    memory = 555
    container_port = 8888
    chosen_port = 666
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = True
    docker_url = "8" * 40
    command = "IE9.exe /VERBOSE /ON_ERROR_RESUME_NEXT"
    net = "host"
    docker_params = []
    detach = True
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert "--net=host" in actual


def test_get_docker_run_cmd_quote_cmd():
    # Regression test to ensure we properly quote multiword custom commands
    memory = 555
    container_port = 8888
    chosen_port = 666
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = True
    docker_url = "8" * 40
    command = "make test"
    net = "host"
    docker_params = []
    detach = True
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert actual[-3:] == ["sh", "-c", "make test"]


def test_get_docker_run_cmd_quote_list():
    # Regression test to ensure we properly quote array custom commands
    memory = 555
    container_port = 8888
    chosen_port = 666
    container_name = "Docker" * 6 + "Doc"
    volumes = ["7_Brides_for_7_Brothers", "7-Up", "7-11"]
    env = {}
    interactive = True
    docker_url = "8" * 40
    command = ["zsh", "-c", "make test"]
    net = "host"
    docker_params = []
    detach = True
    actual = get_docker_run_cmd(
        memory,
        chosen_port,
        container_port,
        container_name,
        volumes,
        env,
        interactive,
        docker_url,
        command,
        net,
        docker_params,
        detach,
    )
    assert actual[-3:] == ["zsh", "-c", "make test"]


def test_get_container_id():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_containers = [
        {"Names": ["/paasta_local_run_1"], "Id": "11111"},
        {"Names": ["/paasta_local_run_2"], "Id": "22222"},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client, return_value=fake_containers
    )
    container_name = "paasta_local_run_2"
    expected = "22222"
    actual = get_container_id(mock_docker_client, container_name)
    assert actual == expected


def test_get_container_id_name_not_found():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_containers = [
        {"Names": ["/paasta_local_run_1"], "Id": "11111"},
        {"Names": ["/paasta_local_run_2"], "Id": "22222"},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client, return_value=fake_containers
    )
    container_name = "paasta_local_run_DOES_NOT_EXIST"
    with raises(LostContainerException):
        get_container_id(mock_docker_client, container_name)


@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
def test_run_docker_container_non_interactive_no_healthcheck(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_docker_client.inspect_container.return_value = {
        "State": {"ExitCode": 666, "Running": True}
    }
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    run_docker_container(
        docker_client=mock_docker_client,
        service="fake_service",
        instance="fake_instance",
        docker_url="fake_hash",
        volumes=[],
        interactive=False,
        command="fake_command",
        healthcheck=False,
        healthcheck_only=False,
        user_port=None,
        instance_config=mock_service_manifest,
        secret_provider_name="vault",
    )
    mock_service_manifest.get_mem.assert_called_once_with()
    mock_pick_random_port.assert_called_once_with("fake_service")
    assert mock_get_docker_run_cmd.call_count == 1
    assert mock_get_healthcheck_for_instance.call_count == 1
    assert mock_execlpe.call_count == 1


@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
def test_run_docker_container_interactive(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    return_code = run_docker_container(
        docker_client=mock_docker_client,
        service="fake_service",
        instance="fake_instance",
        docker_url="fake_hash",
        volumes=[],
        interactive=True,
        command="fake_command",
        healthcheck=False,
        healthcheck_only=False,
        user_port=None,
        instance_config=mock_service_manifest,
        secret_provider_name="vault",
    )
    mock_service_manifest.get_mem.assert_called_once_with()
    mock_pick_random_port.assert_called_once_with("fake_service")
    assert mock_get_docker_run_cmd.call_count == 1
    assert mock_get_healthcheck_for_instance.call_count == 1
    assert mock_execlpe.call_count == 1
    assert mock_run.call_count == 0
    assert mock_get_container_id.call_count == 0
    assert mock_docker_client.attach.call_count == 0
    assert mock_docker_client.stop.call_count == 0
    assert mock_docker_client.remove_container.call_count == 0
    assert return_code == 0


@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container", autospec=True
)
def test_run_docker_container_non_interactive_keyboard_interrupt_with_healthcheck(
    mock_run_healthcheck_on_container,
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(
        spec_set=docker.Client.attach, side_effect=KeyboardInterrupt
    )
    mock_run_healthcheck_on_container.return_value = (True, "Good to go")
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    mock_docker_client.inspect_container.return_value = {
        "State": {"ExitCode": 99, "Running": True}
    }
    return_code = run_docker_container(
        docker_client=mock_docker_client,
        service="fake_service",
        instance="fake_instance",
        docker_url="fake_hash",
        volumes=[],
        interactive=False,
        command="fake_command",
        healthcheck=True,
        healthcheck_only=False,
        user_port=None,
        instance_config=mock_service_manifest,
        secret_provider_name="vault",
    )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert return_code == 99


@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(42, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
def test_run_docker_container_non_interactive_run_returns_nonzero(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_docker_client.inspect_container.return_value = {
        "State": {"ExitCode": 99, "Running": True}
    }
    mock_service_manifest.cluster = "fake_cluster"
    with raises(SystemExit) as excinfo:
        run_docker_container(
            docker_client=mock_docker_client,
            service="fake_service",
            instance="fake_instance",
            docker_url="fake_hash",
            volumes=[],
            interactive=False,
            command="fake_command",
            healthcheck=True,
            healthcheck_only=False,
            user_port=None,
            instance_config=mock_service_manifest,
            secret_provider_name="vault",
        )
    # Cleanup wont' be necessary and the function should bail out early.
    assert mock_docker_client.stop.call_count == 0
    assert mock_docker_client.remove_container.call_count == 0
    assert excinfo.value.code == 1


@mock.patch(
    "paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service",
    autospec=True,
    return_value=True,
)
@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
def test_run_docker_container_with_custom_soadir_uses_healthcheck(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach.return_value = ["line1", "line2"]
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    with raises(SystemExit) as excinfo:
        run_docker_container(
            docker_client=mock_docker_client,
            service="fake_service",
            instance="fake_instance",
            docker_url="fake_hash",
            volumes=[],
            interactive=False,
            command="fake_command",
            healthcheck=True,
            healthcheck_only=True,
            user_port=None,
            instance_config=mock_service_manifest,
            soa_dir="fake_soa_dir",
            secret_provider_name="vault",
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 0
    mock_get_healthcheck_for_instance.assert_called_with(
        "fake_service",
        "fake_instance",
        mock_service_manifest,
        mock_pick_random_port.return_value,
        soa_dir="fake_soa_dir",
    )


@mock.patch(
    "paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service",
    autospec=True,
    return_value=True,
)
@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("cmd", "fake_healthcheck_uri"),
)
def test_run_docker_container_terminates_with_healthcheck_only_success(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach.return_value = "line1"
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.inspect_container.return_value = {"State": {"ExitCode": 0}}
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    with raises(SystemExit) as excinfo:
        run_docker_container(
            docker_client=mock_docker_client,
            service="fake_service",
            instance="fake_instance",
            docker_url="fake_hash",
            volumes=[],
            interactive=False,
            command="fake_command",
            healthcheck=False,
            healthcheck_only=True,
            user_port=None,
            instance_config=mock_service_manifest,
            secret_provider_name="vault",
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 0


@mock.patch(
    "paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service",
    autospec=True,
    return_value=False,
)
@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_docker_run_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("cmd", "fake_healthcheck_uri"),
)
def test_run_docker_container_terminates_with_healthcheck_only_fail(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlpe,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck,
    capfd,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    ATTACH_OUTPUT = "I'm the stdout / stderr!\n"
    mock_docker_client.attach = mock.MagicMock(
        spec_set=docker.Client.attach, return_value=ATTACH_OUTPUT
    )
    mock_docker_client.inspect_container = mock.MagicMock(
        spec_set=docker.Client.inspect_container
    )
    mock_docker_client.inspect_container.return_value = {"State": {"ExitCode": 42}}
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(
        spec_set=docker.Client.remove_container
    )
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.cluster = "fake_cluster"
    with raises(SystemExit) as excinfo:
        run_docker_container(
            docker_client=mock_docker_client,
            service="fake_service",
            instance="fake_instance",
            docker_url="fake_hash",
            volumes=[],
            interactive=False,
            command="fake_command",
            healthcheck=True,
            healthcheck_only=True,
            user_port=None,
            instance_config=mock_service_manifest,
            secret_provider_name="vault",
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 1
    assert (
        ATTACH_OUTPUT not in capfd.readouterr()[0]
    )  # streamed by docker lgos thread instead


@mock.patch("paasta_tools.cli.cmds.local_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.execlpe", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
@mock.patch("paasta_tools.cli.cmds.local_run.check_if_port_free", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.get_container_id", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance",
    autospec=True,
    return_value=("fake_healthcheck_mode", "fake_healthcheck_uri"),
)
def test_run_docker_container_with_user_specified_port(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_check_if_port_free,
    mock_run,
    mock_execlpe,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666  # we dont want it running on this port
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = mock.MagicMock(spec=MarathonServiceConfig)
    mock_service_manifest.get_net.return_value = "bridge"
    mock_service_manifest.get_env_dictionary.return_value = {}
    mock_service_manifest.get_container_port.return_value = 8888
    mock_service_manifest.cluster = "blah"
    run_docker_container(
        docker_client=mock_docker_client,
        service="fake_service",
        instance="fake_instance",
        docker_url="fake_hash",
        volumes=[],
        interactive=False,
        command="fake_command",
        healthcheck=False,
        healthcheck_only=False,
        user_port=1234,
        instance_config=mock_service_manifest,
        secret_provider_name="vault",
    )
    mock_service_manifest.get_mem.assert_called_once_with()
    assert mock_check_if_port_free.call_count == 1
    assert (
        mock_pick_random_port.called is False
    )  # Don't pick a random port, use the user chosen one
    assert mock_execlpe.call_count == 1


@mock.patch("time.sleep", autospec=True)
def test_simulate_healthcheck_on_service_disabled(mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    assert simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        False,
    )


@mock.patch("time.sleep", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container", autospec=True
)
def test_simulate_healthcheck_on_service_enabled_success(
    mock_run_healthcheck_on_container, mock_sleep
):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service="fake_name",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"healthcheck_grace_period_seconds": 0},
        branch_dict=None,
    )
    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    mock_run_healthcheck_on_container.return_value = (True, "it works")
    assert simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        True,
    )


@mock.patch("time.sleep", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container", autospec=True
)
def test_simulate_healthcheck_on_service_enabled_failure(
    mock_run_healthcheck_on_container, mock_sleep
):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service="fake_name",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"healthcheck_grace_period_seconds": 0},
        branch_dict=None,
    )
    mock_service_manifest

    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    mock_run_healthcheck_on_container.return_value = (False, "it failed")
    actual = simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        True,
    )
    assert actual is False


@mock.patch("time.sleep", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container", autospec=True
)
def test_simulate_healthcheck_on_service_enabled_partial_failure(
    mock_run_healthcheck_on_container, mock_sleep
):
    mock_run_healthcheck_on_container.side_effect = [
        (False, ""),
        (False, ""),
        (False, ""),
        (False, ""),
        (True, ""),
    ]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service="fake_name",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"healthcheck_grace_period_seconds": 0},
        branch_dict=None,
    )

    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    assert simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        True,
    )
    # First run_healthcheck_on_container call happens silently
    assert mock_run_healthcheck_on_container.call_count == 5
    assert mock_sleep.call_count == 4


@mock.patch("time.sleep", autospec=True)
@mock.patch("time.time", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container",
    autospec=True,
    return_value=(True, "healthcheck status"),
)
def test_simulate_healthcheck_on_service_enabled_during_grace_period(
    mock_run_healthcheck_on_container, mock_time, mock_sleep
):
    # prevent grace period from ending
    mock_time.side_effect = [0, 0]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service="fake_name",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"healthcheck_grace_period_seconds": 1},
        branch_dict=None,
    )

    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    assert simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        True,
    )
    assert mock_sleep.call_count == 0
    assert mock_run_healthcheck_on_container.call_count == 1


@mock.patch("time.sleep", autospec=True)
@mock.patch("time.time", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run.run_healthcheck_on_container", autospec=True
)
def test_simulate_healthcheck_on_service_enabled_honors_grace_period(
    mock_run_healthcheck_on_container, mock_time, mock_sleep, capfd
):
    # change time to make sure we are sufficiently past grace period
    mock_run_healthcheck_on_container.side_effect = [
        (False, "400 noop"),
        (False, "400 noop"),
        (True, "200 noop"),
    ]

    mock_time.side_effect = [0, 1, 5]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service="fake_name",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={
            # only one healthcheck will be performed silently
            "healthcheck_grace_period_seconds": 2
        },
        branch_dict=None,
    )

    fake_container_id = "fake_container_id"
    fake_mode = "http"
    fake_url = "http://fake_host/fake_status_path"
    assert simulate_healthcheck_on_service(
        mock_service_manifest,
        mock_docker_client,
        fake_container_id,
        fake_mode,
        fake_url,
        True,
    )
    assert mock_sleep.call_count == 2
    assert mock_run_healthcheck_on_container.call_count == 3
    out, _ = capfd.readouterr()
    assert out.count("Healthcheck failed! (disregarded due to grace period)") == 1
    assert out.count("Healthcheck failed! (Attempt") == 1
    assert out.count("200 noop") == 1
    assert out.count("400 noop") == 2
    assert out.count("Healthcheck succeeded!") == 1


def test_simulate_healthcheck_on_service_dead_container_exits_immediately(capfd):
    with mock.patch(
        "time.sleep",
        autospec=True,
        side_effect=AssertionError("sleep should not have been called"),
    ):
        mock_client = mock.MagicMock(spec_set=docker.Client)
        mock_client.inspect_container.return_value = {
            "State": {"Running": False, "ExitCode": 127}
        }
        fake_service_manifest = MarathonServiceConfig(
            service="fake_name",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
        )
        ret = simulate_healthcheck_on_service(
            fake_service_manifest,
            mock_client,
            mock.sentinel.container_id,
            "http",
            "http://fake_host/status",
            True,
        )
        assert ret is False
        out, _ = capfd.readouterr()
        assert out.count("Container exited with code 127") == 1


@mock.patch("paasta_tools.cli.cmds.local_run.timed_flock", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(0, "fake _run output"),
)
def test_pull_image_runs_docker_pull(mock_run, mock_flock):
    open_mock = mock.mock_open()
    mock_flock.return_value.__enter__.return_value.name = "mock"
    with mock.patch("builtins.open", open_mock, autospec=None):
        docker_pull_image("fake_image")
    mock_run.assert_called_once_with(
        "docker pull fake_image", stream=True, stdin=mock.ANY
    )


@mock.patch("paasta_tools.cli.cmds.local_run.timed_flock", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.local_run._run",
    autospec=True,
    return_value=(42, "fake _run output"),
)
def test_pull_docker_image_exists_with_failure(mock_run, mock_flock):
    mock_flock.return_value.__enter__.return_value.name = "mock"
    with raises(SystemExit) as excinfo:
        open_mock = mock.mock_open()
        with mock.patch("builtins.open", open_mock, autospec=None):
            docker_pull_image("fake_image")
    assert excinfo.value.code == 42
    mock_run.assert_called_once_with(
        "docker pull fake_image", stream=True, stdin=mock.ANY
    )


def test_format_command_for_type_for_marathon():
    actual = format_command_for_type("foo", "marathon", "fake-date")
    assert actual == "foo"


@mock.patch("paasta_tools.cli.cmds.local_run.parse_time_variables", autospec=True)
@mock.patch("paasta_tools.cli.cmds.local_run.datetime", autospec=True)
def test_format_command_for_tron(mock_datetime, mock_parse_time_variables):
    fake_date = mock.Mock()
    mock_datetime.datetime.now.return_value = fake_date
    mock_parse_time_variables.return_value = "foo"
    actual = format_command_for_type("{foo}", "tron", fake_date)
    mock_parse_time_variables.assert_called_once_with("{foo}", fake_date)
    assert actual == "foo"


@mock.patch(
    "paasta_tools.cli.cmds.local_run.socket.getfqdn",
    autospec=True,
    return_value="fake_host",
)
def test_get_local_run_environment_vars_marathon(mock_getfqdn,):
    mock_instance_config = mock.MagicMock(spec_set=MarathonServiceConfig)
    mock_instance_config.get_mem.return_value = 123
    mock_instance_config.get_disk.return_value = 123
    mock_instance_config.get_cpus.return_value = 123
    mock_instance_config.get_docker_image.return_value = "fake_docker_image"

    actual = get_local_run_environment_vars(
        instance_config=mock_instance_config, port0=1234, framework="marathon"
    )
    assert actual["MARATHON_PORT"] == "1234"
    assert actual["MARATHON_PORT0"] == "1234"
    assert actual["MARATHON_HOST"] == "fake_host"
    assert actual["MARATHON_APP_RESOURCE_CPUS"] == "123"
    assert actual["MARATHON_APP_RESOURCE_MEM"] == "123"
    assert actual["MARATHON_APP_RESOURCE_DISK"] == "123"


@mock.patch(
    "paasta_tools.cli.cmds.local_run.socket.getfqdn",
    autospec=True,
    return_value="fake_host",
)
def test_get_local_run_environment_vars_other(mock_getfqdn,):
    mock_instance_config = mock.MagicMock(spec_set=AdhocJobConfig)
    mock_instance_config.get_mem.return_value = 123
    mock_instance_config.get_disk.return_value = 123
    mock_instance_config.get_cpus.return_value = 123
    mock_instance_config.get_docker_image.return_value = "fake_docker_image"

    actual = get_local_run_environment_vars(
        instance_config=mock_instance_config, port0=1234, framework="adhoc"
    )
    assert actual["PAASTA_DOCKER_IMAGE"] == "fake_docker_image"
    assert "MARATHON_PORT" not in actual


@mock.patch(
    "paasta_tools.cli.cmds.local_run.os.path.exists", return_value=True, autospec=True
)
def test_volumes_are_deduped(mock_exists):
    with mock.patch(
        "paasta_tools.cli.cmds.local_run.run_docker_container", autospec=True
    ) as mock_run_docker_container, mock.patch(
        "paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True
    ) as mock_get_instance_config, mock.patch(
        "paasta_tools.cli.cmds.local_run.validate_service_instance",
        autospec=True,
        return_value="marathon",
    ):

        mock_get_instance_config.return_value = InstanceConfig(
            cluster="cluster",
            instance="instance",
            service="service",
            config_dict={
                "extra_volumes": [
                    {
                        "hostPath": "/hostPath",
                        "containerPath": "/containerPath",
                        "mode": "RO",
                    }
                ]
            },
            branch_dict=None,
        )

        configure_and_run_docker_container(
            docker_client=mock.Mock(),
            docker_url="12345",
            docker_sha=None,
            service="service",
            instance="instance",
            cluster="cluster",
            system_paasta_config=SystemPaastaConfig(
                {
                    "volumes": [
                        {
                            "hostPath": "/hostPath",
                            "containerPath": "/containerPath",
                            "mode": "RO",
                        }
                    ]
                },
                "/etc/paasta",
            ),
            args=mock.Mock(yelpsoa_config_root="/blurp/durp"),
        )
        args, kwargs = mock_run_docker_container.call_args
        assert kwargs["volumes"] == ["/hostPath:/containerPath:ro"]


@mock.patch(
    "paasta_tools.cli.cmds.local_run.os.path.exists", return_value=False, autospec=True
)
def test_missing_volumes_skipped(mock_exists):
    with mock.patch(
        "paasta_tools.cli.cmds.local_run.run_docker_container", autospec=True
    ) as mock_run_docker_container, mock.patch(
        "paasta_tools.cli.cmds.local_run.get_instance_config", autospec=True
    ) as mock_get_instance_config, mock.patch(
        "paasta_tools.cli.cmds.local_run.validate_service_instance",
        autospec=True,
        return_value="marathon",
    ):

        mock_get_instance_config.return_value = InstanceConfig(
            cluster="cluster",
            instance="instance",
            service="service",
            config_dict={
                "extra_volumes": [
                    {
                        "hostPath": "/hostPath",
                        "containerPath": "/containerPath",
                        "mode": "RO",
                    }
                ]
            },
            branch_dict=None,
        )

        configure_and_run_docker_container(
            docker_client=mock.Mock(),
            docker_url="12345",
            docker_sha=None,
            service="service",
            instance="instance",
            cluster="cluster",
            system_paasta_config=SystemPaastaConfig(
                {
                    "volumes": [
                        {
                            "hostPath": "/hostPath",
                            "containerPath": "/containerPath",
                            "mode": "RO",
                        }
                    ]
                },
                "/etc/paasta",
            ),
            args=mock.Mock(yelpsoa_config_root="/blurp/durp"),
        )
        args, kwargs = mock_run_docker_container.call_args
        assert kwargs["volumes"] == []
