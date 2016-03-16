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


def test_job_for_date_no_date():
    fake_chronos_job_config = {
        'command': 'foo'
    }
    assert chronos_rerun.job_for_date(fake_chronos_job_config,
                                      datetime.datetime.now()) == \
        fake_chronos_job_config


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


@mock.patch('paasta_tools.chronos_rerun.remove_parents')
@mock.patch('paasta_tools.chronos_rerun.set_default_schedule')
def test_clone_job_dependent(mock_remove_parents, mock_set_default_schedule):
    fake_chronos_job_config = {
        'parents': ['foo', 'bar']
    }
    chronos_rerun.clone_job(fake_chronos_job_config)
    assert mock_remove_parents.called_once()
    assert mock_set_default_schedule.called_once()
