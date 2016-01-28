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

from paasta_tools import cleanup_chronos_jobs


def test_cleanup_jobs():
    chronos_client = mock.Mock()
    returns = [None, None, Exception('boom')]

    def side_effect(*args):
        result = returns.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    chronos_client.delete = mock.Mock(side_effect=side_effect)
    result = cleanup_chronos_jobs.cleanup_jobs(chronos_client, ['foo', 'bar', 'baz'])

    # I'd like to just compare the lists, but you can't compare exception objects.
    print result
    assert result[0] == ('foo', None)
    assert result[1] == ('bar', None)
    assert result[2][0] == 'baz'
    assert isinstance(result[2][1], Exception)


def test_format_list_output():
    assert cleanup_chronos_jobs.format_list_output("Successfully Removed:", ['foo', 'bar', 'baz']) \
        == "Successfully Removed:\n  foo\n  bar\n  baz"


def test_deployed_job_names():
    mock_client = mock.Mock()
    mock_client.list.return_value = [{'name': 'foo', 'blah': 'blah'}, {'name': 'bar', 'blah': 'blah'}]
    assert cleanup_chronos_jobs.deployed_job_names(mock_client) == ['foo', 'bar']
