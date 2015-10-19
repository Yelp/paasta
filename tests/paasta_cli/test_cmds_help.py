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

from mock import patch
from StringIO import StringIO

from paasta_tools.paasta_cli.cmds.help import paasta_help


@patch('sys.stdout', new_callable=StringIO)
def test_list_paasta_list(mock_stdout):
    args = ['./paasta_cli', 'help']
    paasta_help(args)
    output = mock_stdout.getvalue()
    assert 'http://y/paasta' in output
