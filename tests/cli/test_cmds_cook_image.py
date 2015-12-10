# Copyright 2015 Yelp Inc.
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
from paasta_tools.cli.cmds.cook_image import paasta_cook_image


@mock.patch('paasta_tools.cli.cmds.cook_image.validate_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image.makefile_responds_to', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image._run', autospec=True)
def test_run_success(
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Output')
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.service = 'fake_service'
    assert paasta_cook_image(args) is None


@mock.patch('paasta_tools.cli.cmds.cook_image.validate_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image.makefile_responds_to', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image._run', autospec=True)
def test_run_makefile_fail(
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Output')
    mock_makefile_responds_to.return_value = False
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.service = 'fake_service'

    with raises(SystemExit) as excinfo:
        paasta_cook_image(args)

    assert excinfo.value.code == 1


class FakeKeyboardInterrupt(KeyboardInterrupt):
    pass


@mock.patch('paasta_tools.cli.cmds.cook_image.validate_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image.makefile_responds_to', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image._run', autospec=True)
def test_run_keyboard_interrupt(
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Output')
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True
    mock_run.side_effect = FakeKeyboardInterrupt

    args = mock.MagicMock()
    args.service = 'fake_service'

    with raises(SystemExit) as excinfo:
        paasta_cook_image(args)

    assert excinfo.value.code == 2
