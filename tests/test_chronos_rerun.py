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
import datetime

import mock

from paasta_tools import chronos_rerun


@mock.patch('paasta_tools.chronos_rerun.chronos_tools.parse_time_variables')
def test_modify_command_for_date(mock_parse_time_variables):
    mock_parse_time_variables.return_value = '2016-03-17'
    fake_chronos_job_config = {
        'command': 'foo'
    }
    actual = chronos_rerun.modify_command_for_date(fake_chronos_job_config,
                                                   datetime.datetime.now())

    assert actual == {
        'command': '2016-03-17'
    }


def test_remove_parents():
    fake_chronos_job_config = {
        'parents': ['foo', 'bar', 'baz']
    }
    assert chronos_rerun.remove_parents(fake_chronos_job_config) == {}


def test_set_default_schedule():
    fake_chronos_job_config = {
        'schedule': 'foo'
    }
    assert chronos_rerun.set_default_schedule(fake_chronos_job_config) == \
        {'schedule': 'R1//PT1M'}


def test_set_tmp_naming_scheme():
    fake_chronos_job_config = {
        'name': 'foo bar'
    }
    assert chronos_rerun.set_tmp_naming_scheme(fake_chronos_job_config) == \
        {'name': 'tmp foo bar'}


@mock.patch('paasta_tools.chronos_rerun.remove_parents')
@mock.patch('paasta_tools.chronos_rerun.set_default_schedule')
@mock.patch('paasta_tools.chronos_rerun.modify_command_for_date')
def test_clone_job_dependent(mock_remove_parents, mock_set_default_schedule, mock_modify_command_for_date):
    fake_chronos_job_config = {
        'parents': ['foo', 'bar']
    }
    chronos_rerun.clone_job(fake_chronos_job_config, '2016-03-2016T04:40:31')
    assert mock_remove_parents.called_once()
    assert mock_set_default_schedule.called_once()
    assert mock_modify_command_for_date.called_once()
