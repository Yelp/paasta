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
import datetime

import dateutil
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
    print(result)
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


@mock.patch('paasta_tools.cleanup_chronos_jobs.chronos_tools.get_temporary_jobs_for_service_instance', autospec=True)
def test_filter_expired_tmp_jobs(mock_get_temporary_jobs):
    two_days_ago = datetime.datetime.now(dateutil.tz.tzutc()) - datetime.timedelta(days=2)
    one_hour_ago = datetime.datetime.now(dateutil.tz.tzutc()) - datetime.timedelta(hours=1)
    mock_get_temporary_jobs.side_effect = [
        [{'name': 'tmp foo bar', 'lastSuccess': two_days_ago.isoformat()}],
        [{'name': 'tmp anotherservice anotherinstance', 'lastSuccess': one_hour_ago.isoformat()}],
    ]
    actual = cleanup_chronos_jobs.filter_expired_tmp_jobs(mock.Mock(), ['foo bar', 'anotherservice anotherinstance'])
    assert actual == ['foo bar']


def test_filter_paasta_jobs():
    expected = ['foo bar']
    assert cleanup_chronos_jobs.filter_paasta_jobs(iter(['foo bar', 'madeupchronosjob'])) == expected
