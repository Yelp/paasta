#!/usr/bin/env python
# Copyright 2017 Yelp Inc.
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

from paasta_tools import marathon_dashboard


def test_main():
    soa_dir = '/fake/soa/dir'
    cluster = 'fake_cluster'
    with mock.patch(
        'paasta_tools.marathon_dashboard.create_marathon_dashboard',
        autospec=True,
        return_value={},
    ) as create_marathon_dashboard:
        marathon_dashboard.main(('--soa-dir', soa_dir, '--cluster', cluster))
        create_marathon_dashboard.assert_called_once_with(
            cluster=cluster,
            soa_dir=soa_dir,
        )


def test_create_marathon_dashboard():
    soa_dir = '/fake/soa/dir'
    cluster = 'fake_cluster'
    expected_output = {'fake_cluster': []}
    assert(marathon_dashboard.create_marathon_dashboard(cluster=cluster, soa_dir=soa_dir) == expected_output)
