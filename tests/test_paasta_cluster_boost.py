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

from paasta_tools import paasta_cluster_boost

FAKE_SLAVE_DATA = [
    {
        'datacenter': 'westeros-1',
        'ecosystem': 'stagef',
        'habitat': 'uswest1cstagef',
        'instance_type': 'c3.4xlarge',
        'kwatest': 'foo',
        'pool': 'piscine',
        'region': 'uswest1-stagef',
        'role': 'taskproc',
        'runtimeenv': 'stage',
        'superregion': 'norcal-stagef',
        'topology_env': 'stagef',
    },
    {
        'datacenter': 'westeros-1',
        'ecosystem': 'stagef',
        'habitat': 'uswest1cstagef',
        'instance_type': 'c4.4xlarge',
        'kwatest': 'foo',
        'pool': 'default',
        'region': 'uswest1-stagef',
        'role': 'taskproc',
        'runtimeenv': 'stage',
        'superregion': 'norcal-stagef',
        'topology_env': 'stagef',
    },
]


def test_check_pool_exist():
    with mock.patch(
        'paasta_tools.paasta_cluster_boost.load_system_paasta_config',
        autospec=True,
    ) as load_system_paasta_config_patch:
        load_system_paasta_config_patch.return_value.get_expected_slave_attributes = mock.Mock(
            return_value=FAKE_SLAVE_DATA,
        )
        assert paasta_cluster_boost.check_pool_exist(pool='piscine', region='westeros-1')


def test_check_pool_exist_bad_pool():
    with mock.patch(
        'paasta_tools.paasta_cluster_boost.load_system_paasta_config',
        autospec=True,
    ) as load_system_paasta_config_patch:
        load_system_paasta_config_patch.return_value.get_expected_slave_attributes = mock.Mock(
            return_value=FAKE_SLAVE_DATA,
        )
        assert not paasta_cluster_boost.check_pool_exist(pool='big_pond', region='westeros-1')


def test_check_pool_exist_bad_region():
    with mock.patch(
        'paasta_tools.paasta_cluster_boost.load_system_paasta_config',
        autospec=True,
    ) as load_system_paasta_config_patch:
        load_system_paasta_config_patch.return_value.get_expected_slave_attributes = mock.Mock(
            return_value=FAKE_SLAVE_DATA,
        )
        assert not paasta_cluster_boost.check_pool_exist(pool='piscine', region='the-north')


def test_check_pool_exist_no_data():
    with mock.patch(
        'paasta_tools.paasta_cluster_boost.load_system_paasta_config',
        autospec=True,
    ) as load_system_paasta_config_patch:
        load_system_paasta_config_patch.return_value.get_expected_slave_attributes = mock.Mock(return_value=None)
        assert not paasta_cluster_boost.check_pool_exist(pool='piscine', region='westeros-1')
