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

from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.utils import get_username


@mock.patch("paasta_tools.cli.cmds.cook_image.validate_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image.makefile_responds_to", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._run", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._log_audit", autospec=True)
def test_run_success(
    mock_log_audit, mock_run, mock_makefile_responds_to, mock_validate_service_name
):
    mock_run.return_value = (0, "Output")
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.commit = None
    args.service = "fake_service"
    assert paasta_cook_image(args) == 0

    mock_log_audit.assert_called_once_with(
        action="cook-image",
        action_details={
            "tag": "paasta-cook-image-fake_service-{}".format(get_username())
        },
        service="fake_service",
    )


@mock.patch("paasta_tools.cli.cmds.cook_image.validate_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image.makefile_responds_to", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._run", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._log_audit", autospec=True)
def test_run_success_with_commit(
    mock_log_audit, mock_run, mock_makefile_responds_to, mock_validate_service_name
):
    mock_run.return_value = (0, "Output")
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.commit = "0" * 40
    args.service = "fake_service"

    with mock.patch(
        "paasta_tools.utils.get_service_docker_registry",
        autospec=True,
        return_value="fake_registry",
    ):
        assert paasta_cook_image(args) == 0

    mock_log_audit.assert_called_once_with(
        action="cook-image",
        action_details={
            "tag": f"fake_registry/services-fake_service:paasta-{args.commit}"
        },
        service="fake_service",
    )


@mock.patch("paasta_tools.cli.cmds.cook_image.validate_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image.makefile_responds_to", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._run", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._log_audit", autospec=True)
def test_run_makefile_fail(
    mock_log_audit, mock_run, mock_makefile_responds_to, mock_validate_service_name
):
    mock_run.return_value = (0, "Output")
    mock_makefile_responds_to.return_value = False
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.service = "fake_service"
    args.commit = None

    assert paasta_cook_image(args) == 1
    assert not mock_log_audit.called


class FakeKeyboardInterrupt(KeyboardInterrupt):
    pass


@mock.patch("paasta_tools.cli.cmds.cook_image.validate_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image.makefile_responds_to", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._run", autospec=True)
@mock.patch("paasta_tools.cli.cmds.cook_image._log_audit", autospec=True)
def test_run_keyboard_interrupt(
    mock_log_audit, mock_run, mock_makefile_responds_to, mock_validate_service_name
):
    mock_run.return_value = (0, "Output")
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True
    mock_run.side_effect = FakeKeyboardInterrupt

    args = mock.MagicMock()
    args.service = "fake_service"
    args.commit = None

    assert paasta_cook_image(args) == 2
    assert not mock_log_audit.called
