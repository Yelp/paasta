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
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import patch

from pytest import raises
from requests.exceptions import RequestException
from requests.exceptions import SSLError

from paasta_tools.cli.cli import parse_args
from paasta_tools.cli.cmds.push_to_registry import build_command
from paasta_tools.cli.cmds.push_to_registry import is_docker_image_already_in_registry
from paasta_tools.cli.cmds.push_to_registry import paasta_push_to_registry


@patch("paasta_tools.cli.cmds.push_to_registry.build_docker_tag", autospec=True)
def test_build_command(mock_build_docker_tag):
    mock_build_docker_tag.return_value = "my-docker-registry/services-foo:paasta-asdf"
    expected = "docker push my-docker-registry/services-foo:paasta-asdf"
    actual = build_command("foo", "bar")
    assert actual == expected


@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
def test_push_to_registry_run_fail(
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
    mock_is_docker_image_already_in_registry,
):
    mock_build_command.return_value = (
        "docker push my-docker-registry/services-foo:paasta-asdf"
    )
    mock_is_docker_image_already_in_registry.return_value = False
    mock_run.return_value = (1, "Bad")
    args = MagicMock()
    assert paasta_push_to_registry(args) == 1
    assert not mock_log_audit.called


@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
def test_push_to_registry_success(
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
    mock_is_docker_image_already_in_registry,
):
    args, _ = parse_args(["push-to-registry", "-s", "foo", "-c", "abcd" * 10])
    mock_build_command.return_value = (
        "docker push my-docker-registry/services-foo:paasta-asdf"
    )
    mock_run.return_value = (0, "Success")
    mock_is_docker_image_already_in_registry.return_value = False
    assert paasta_push_to_registry(args) == 0
    assert mock_build_command.called
    assert mock_run.called
    mock_log_audit.assert_called_once_with(
        action="push-to-registry", action_details={"commit": "abcd" * 10}, service="foo"
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
def test_push_to_registry_force(
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
    mock_is_docker_image_already_in_registry,
):
    args, _ = parse_args(
        ["push-to-registry", "-s", "foo", "-c", "abcd" * 10, "--force"]
    )
    mock_build_command.return_value = (
        "docker push fake_registry/services-foo:paasta-abcd"
    )
    mock_run.return_value = (0, "Success")
    assert paasta_push_to_registry(args) == 0
    assert not mock_is_docker_image_already_in_registry.called
    mock_run.assert_called_once_with(
        "docker push fake_registry/services-foo:" "paasta-abcd",
        component="build",
        log=True,
        loglevel="debug",
        service="foo",
        timeout=3600,
    )
    mock_log_audit.assert_called_once_with(
        action="push-to-registry", action_details={"commit": "abcd" * 10}, service="foo"
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
def test_push_to_registry_does_not_override_existing_image(
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
    mock_is_docker_image_already_in_registry,
):
    args, _ = parse_args(["push-to-registry", "-s", "foo", "-c", "abcd" * 10])
    mock_run.return_value = (0, "Success")
    mock_is_docker_image_already_in_registry.return_value = True
    assert paasta_push_to_registry(args) == 0
    assert not mock_build_command.called
    assert not mock_run.called
    assert not mock_log_audit.called


@patch("paasta_tools.utils.load_system_paasta_config", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
def test_push_to_registry_does_not_override_when_cant_check_status(
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
    mock_is_docker_image_already_in_registry,
    mock_load_system_paasta_config,
):
    args, _ = parse_args(["push-to-registry", "-s", "foo", "-c", "abcd" * 10])
    mock_run.return_value = (0, "Success")
    mock_is_docker_image_already_in_registry.side_effect = RequestException()
    assert paasta_push_to_registry(args) == 1
    assert not mock_build_command.called
    assert not mock_run.called
    assert not mock_log_audit.called


@patch(
    "paasta_tools.cli.cmds.push_to_registry.is_docker_image_already_in_registry",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.push_to_registry.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._run", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.push_to_registry.build_command", autospec=True)
def test_push_to_registry_works_when_service_name_starts_with_services_dash(
    mock_build_command,
    mock_log_audit,
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_is_docker_image_already_in_registry,
):
    args, _ = parse_args(["push-to-registry", "-s", "foo", "-c", "abcd" * 10])
    mock_run.return_value = (0, "Success")
    mock_is_docker_image_already_in_registry.return_value = False
    assert paasta_push_to_registry(args) == 0
    mock_build_command.assert_called_once_with("foo", "abcd" * 10)
    mock_log_audit.assert_called_once_with(
        action="push-to-registry", action_details={"commit": "abcd" * 10}, service="foo"
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_in_registry_success(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    mock_read_docker_registry_creds.return_value = (None, None)
    mock_get_service_docker_registry.return_value = "registry"
    mock_request_head.return_value = MagicMock(status_code=200)
    assert is_docker_image_already_in_registry(
        "fake_service", "fake_soa_dir", "fake_sha"
    )
    mock_request_head.assert_called_with(
        ANY,
        "https://registry/v2/services-fake_service/manifests/paasta-fake_sha",
        timeout=30,
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_in_registry_success_with_registry_credentials(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    auth = ("username", "password")
    mock_read_docker_registry_creds.return_value = auth
    mock_get_service_docker_registry.return_value = "registry"
    mock_request_head.return_value = MagicMock(status_code=200)
    assert is_docker_image_already_in_registry(
        "fake_service", "fake_soa_dir", "fake_sha"
    )
    mock_request_head.assert_called_with(
        ANY,
        "https://registry/v2/services-fake_service/manifests/paasta-fake_sha",
        auth=auth,
        timeout=30,
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_in_registry_404_no_such_service_yet(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    mock_read_docker_registry_creds.return_value = (None, None)
    mock_get_service_docker_registry.return_value = "registry"
    mock_request_head.return_value = MagicMock(
        status_code=404
    )  # No Such Repository Error
    assert not is_docker_image_already_in_registry(
        "fake_service", "fake_soa_dir", "fake_sha"
    )
    mock_request_head.assert_called_with(
        ANY,
        "https://registry/v2/services-fake_service/manifests/paasta-fake_sha",
        timeout=30,
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_when_image_does_not_exist(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    mock_read_docker_registry_creds.return_value = (None, None)
    mock_get_service_docker_registry.return_value = "registry"
    mock_request_head.return_value = MagicMock(status_code=404)
    assert not is_docker_image_already_in_registry(
        "fake_service", "fake_soa_dir", "fake_sha"
    )
    mock_request_head.assert_called_with(
        ANY,
        "https://registry/v2/services-fake_service/manifests/paasta-fake_sha",
        timeout=30,
    )


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_in_registry_401_unauthorized(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    mock_read_docker_registry_creds.return_value = (None, None)
    mock_request_head.side_effect = RequestException()
    with raises(RequestException):
        is_docker_image_already_in_registry("fake_service", "fake_soa_dir", "fake_sha")


@patch(
    "paasta_tools.cli.cmds.push_to_registry.get_service_docker_registry", autospec=True
)
@patch("paasta_tools.cli.cmds.push_to_registry.requests.Session.head", autospec=True)
@patch(
    "paasta_tools.cli.cmds.push_to_registry.read_docker_registry_creds", autospec=True
)
def test_is_docker_image_already_in_registry_http_when_image_does_not_exist(
    mock_read_docker_registry_creds, mock_request_head, mock_get_service_docker_registry
):
    def mock_head(session, url, timeout):
        if url.startswith("https"):
            raise SSLError("Uh oh")
        return MagicMock(status_code=404)

    mock_get_service_docker_registry.return_value = "registry"
    mock_request_head.side_effect = mock_head

    mock_read_docker_registry_creds.return_value = (None, None)
    assert not is_docker_image_already_in_registry(
        "fake_service", "fake_soa_dir", "fake_sha"
    )
    mock_request_head.assert_called_with(
        ANY,
        "http://registry/v2/services-fake_service/manifests/paasta-fake_sha",
        timeout=30,
    )
