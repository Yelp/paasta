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
from pytest import raises

from paasta_tools import paasta_cluster_boost

FAKE_SLAVE_DATA = [
    {
        "datacenter": "westeros-1",
        "ecosystem": "stagef",
        "habitat": "uswest1cstagef",
        "instance_type": "c3.4xlarge",
        "kwatest": "foo",
        "pool": "default",
        "region": "uswest1-stagef",
        "role": "taskproc",
        "runtimeenv": "stage",
        "superregion": "norcal-stagef",
        "topology_env": "stagef",
    },
    {
        "datacenter": "westeros-1",
        "ecosystem": "stagef",
        "habitat": "uswest1cstagef",
        "instance_type": "c4.4xlarge",
        "kwatest": "foo",
        "pool": "default",
        "region": "uswest1-stagef",
        "role": "taskproc",
        "runtimeenv": "stage",
        "superregion": "norcal-stagef",
        "topology_env": "stagef",
    },
]


def test_main():
    with mock.patch(
        "paasta_tools.paasta_cluster_boost.parse_args",
        autospec=True,
        return_value=mock.Mock(verbose=1),
    ), mock.patch(
        "paasta_tools.paasta_cluster_boost.paasta_cluster_boost", autospec=True
    ) as mock_paasta_cluster_boost:
        mock_paasta_cluster_boost.return_value = True
        with raises(SystemExit) as e:
            paasta_cluster_boost.main()
        assert e.value.code == 0

        mock_paasta_cluster_boost.return_value = False
        with raises(SystemExit) as e:
            paasta_cluster_boost.main()
        assert e.value.code == 1


def test_paasta_cluster_boost():
    with mock.patch(
        "paasta_tools.paasta_cluster_boost.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.paasta_cluster_boost.load_boost.get_zk_cluster_boost_path",
        autospec=True,
    ) as mock_get_zk_cluster_boost_path, mock.patch(
        "paasta_tools.paasta_cluster_boost.load_boost.set_boost_factor", autospec=True
    ) as mock_set_boost_factor, mock.patch(
        "paasta_tools.paasta_cluster_boost.load_boost.clear_boost", autospec=True
    ) as mock_clear_boost, mock.patch(
        "paasta_tools.paasta_cluster_boost.load_boost.get_boost_factor", autospec=True
    ):
        mock_get_regions = mock.Mock(return_value=[])
        mock_load_system_paasta_config.return_value = mock.Mock(
            get_cluster_boost_enabled=mock.Mock(return_value=False),
            get_boost_regions=mock_get_regions,
        )
        mock_get_regions.return_value = ["useast1-dev"]
        assert not paasta_cluster_boost.paasta_cluster_boost(
            action="status", pool="default", boost=1.0, duration=20, override=False
        )

        mock_load_system_paasta_config.return_value = mock.Mock(
            get_cluster_boost_enabled=mock.Mock(return_value=True),
            get_boost_regions=mock_get_regions,
        )
        mock_get_regions.return_value = []
        assert not paasta_cluster_boost.paasta_cluster_boost(
            action="status", pool="default", boost=1.0, duration=20, override=False
        )

        mock_load_system_paasta_config.return_value = mock.Mock(
            get_cluster_boost_enabled=mock.Mock(return_value=True),
            get_boost_regions=mock_get_regions,
        )
        mock_get_regions.return_value = ["useast1-dev"]
        assert paasta_cluster_boost.paasta_cluster_boost(
            action="status", pool="default", boost=1.0, duration=20, override=False
        )
        assert not mock_set_boost_factor.called
        assert not mock_clear_boost.called

        assert paasta_cluster_boost.paasta_cluster_boost(
            action="set", pool="default", boost=1.0, duration=20, override=False
        )
        mock_set_boost_factor.assert_called_with(
            zk_boost_path=mock_get_zk_cluster_boost_path.return_value,
            region="useast1-dev",
            pool="default",
            factor=1.0,
            duration_minutes=20,
            override=False,
        )
        assert not mock_clear_boost.called

        assert paasta_cluster_boost.paasta_cluster_boost(
            action="clear", pool="default", boost=1.0, duration=20, override=False
        )
        mock_clear_boost.assert_called_with(
            zk_boost_path=mock_get_zk_cluster_boost_path.return_value,
            region="useast1-dev",
            pool="default",
        )
