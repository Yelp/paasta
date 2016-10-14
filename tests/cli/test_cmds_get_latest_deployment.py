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
import contextlib
from StringIO import StringIO

from mock import MagicMock
from mock import patch

from paasta_tools.cli.cmds import get_latest_deployment


def test_get_latest_deployment():
    mock_args = MagicMock(
        service='',
        deploy_group='',
        soa_dir='',
    )
    with contextlib.nested(
        patch('sys.stdout', new_callable=StringIO, autospec=None),
        patch('paasta_tools.cli.cmds.get_latest_deployment.get_currently_deployed_sha',
              return_value="FAKE_SHA", autospec=True),
        patch('paasta_tools.cli.cmds.get_latest_deployment.validate_service_name', autospec=True),
    ) as (
        mock_stdout,
        _,
        _,
    ):
        assert get_latest_deployment.paasta_get_latest_deployment(mock_args) == 0
        assert "FAKE_SHA" in mock_stdout.getvalue()


def test_get_latest_deployment_no_deployment_tag():
    mock_args = MagicMock(
        service='fake_service',
        deploy_group='fake_deploy_group',
        soa_dir='',
    )
    with contextlib.nested(
        patch('sys.stdout', new_callable=StringIO, autospec=None),
        patch('paasta_tools.cli.cmds.get_latest_deployment.get_currently_deployed_sha',
              return_value=None, autospec=True),
        patch('paasta_tools.cli.cmds.get_latest_deployment.validate_service_name', autospec=True),
    ) as (
        mock_stdout,
        _,
        _,
    ):
        assert get_latest_deployment.paasta_get_latest_deployment(mock_args) == 1
        assert "A deployment could not be found for fake_deploy_group in fake_service" in \
            mock_stdout.getvalue()
