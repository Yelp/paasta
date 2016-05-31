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
from StringIO import StringIO

import mock

from paasta_tools.cli.cmds.list import paasta_list


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.cli.cmds.list.list_services', autospec=True)
def test_list_paasta_list(mock_list_services, mock_stdout):
    """ paasta_list print each service returned by get_services """

    mock_services = ['service_1', 'service_2']

    mock_list_services.return_value = mock_services
    args = mock.MagicMock()
    args.print_instances = False
    paasta_list(args)

    output = mock_stdout.getvalue()
    assert output == 'service_1\nservice_2\n'


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.cli.cmds.list.list_service_instances', autospec=True)
def test_list_paasta_list_instances(mock_list_service_instances, mock_stdout):
    """ paasta_list print each service.instance """

    mock_services = ['service_1.main', 'service_2.canary']

    mock_list_service_instances.return_value = mock_services
    args = mock.MagicMock()
    args.print_instances = True
    paasta_list(args)

    output = mock_stdout.getvalue()
    assert output == 'service_1.main\nservice_2.canary\n'
