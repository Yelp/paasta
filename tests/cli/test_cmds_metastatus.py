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
from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from paasta_tools.cli.cmds import metastatus
from paasta_tools.utils import SystemPaastaConfig


@mock.patch('paasta_tools.cli.cmds.metastatus.load_system_paasta_config', autospec=True)
def test_report_cluster_status(mock_load_system_paasta_config, capfd):
    cluster = 'fake_cluster'

    fake_system_paasta_config = SystemPaastaConfig({
        'dashboard_links': {
            'fake_cluster': {
                'URL': 'http://paasta-fake_cluster.yelp:5050',
            },
        },
    }, 'fake_directory')

    mock_load_system_paasta_config.return_value = fake_system_paasta_config

    thing_to_patch = 'paasta_tools.cli.cmds.metastatus.execute_paasta_metastatus_on_remote_master'
    with mock.patch(thing_to_patch, autospec=True) as mock_execute_paasta_metastatus_on_remote_master:
        mock_execute_paasta_metastatus_on_remote_master.return_value = mock.sentinel.return_value, 'mock_status'
        return_code = metastatus.print_cluster_status(
            cluster,
            fake_system_paasta_config,
            False,
            [],
            verbose=0
        )
        mock_execute_paasta_metastatus_on_remote_master.assert_called_once_with(
            cluster=cluster,
            system_paasta_config=fake_system_paasta_config,
            humanize=False,
            groupings=[],
            verbose=0,
            autoscaling_info=False
        )
        actual, _ = capfd.readouterr()
        assert 'Cluster: %s' % cluster in actual
        assert 'mock_status' in actual
        assert return_code == mock.sentinel.return_value


def test_figure_out_clusters_to_inspect_respects_the_user():
    fake_args = mock.Mock()
    fake_args.clusters = 'a,b,c'
    fake_all_clusters = ['a', 'b', 'c', 'd']
    assert ['a', 'b', 'c'] == metastatus.figure_out_clusters_to_inspect(fake_args, fake_all_clusters)


def test_get_cluster_dashboards():
    with mock.patch('paasta_tools.cli.cmds.metastatus.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig({
            'dashboard_links': {
                'fake_cluster': {
                    'URL': 'http://paasta-fake_cluster.yelp:5050',
                },
            },
        }, 'fake_directory')
        output_text = metastatus.get_cluster_dashboards('fake_cluster')
        assert 'http://paasta-fake_cluster.yelp:5050' in output_text
        assert 'URL: ' in output_text


def test_get_cluster_no_dashboards():
    with mock.patch('paasta_tools.cli.cmds.metastatus.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig(
            {}, 'fake_directory')
        output_text = metastatus.get_cluster_dashboards('fake_cluster')
        assert 'No dashboards configured' in output_text


def test_get_cluster_dashboards_unknown_cluster():
    with mock.patch('paasta_tools.cli.cmds.metastatus.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig({
            'dashboard_links': {
                'another_fake_cluster': {
                    'URL': 'http://paasta-fake_cluster.yelp:5050',
                },
            },
        }, 'fake_directory')
        output_text = metastatus.get_cluster_dashboards('fake_cluster')
        assert 'No dashboards configured for fake_cluster' in output_text


def test_paasta_metastatus_returns_zero_all_clusters_ok():
    args = mock.Mock(
        soa_dir=mock.sentinel.soa_dir,
        clusters='cluster1,cluster2,cluster3',
    )

    with mock.patch(
        'paasta_tools.cli.cmds.metastatus.list_clusters', autospec=True,
    ) as mock_list_clusters, mock.patch(
        'paasta_tools.cli.cmds.metastatus.print_cluster_status', autospec=True,
    ) as mock_print_cluster_status, mock.patch(
        'paasta_tools.cli.cmds.metastatus.load_system_paasta_config', autospec=True,
    ):
        mock_list_clusters.return_value = ['cluster1', 'cluster2', 'cluster3']
        mock_print_cluster_status.side_effect = [0, 0, 0]

        return_code = metastatus.paasta_metastatus(args)
        assert return_code == 0
        assert mock_print_cluster_status.call_count == 3


def test_paasta_metastatus_returns_one_on_error():
    args = mock.Mock(
        soa_dir=mock.sentinel.soa_dir,
        clusters='cluster1,cluster2,cluster3',
    )

    with mock.patch(
        'paasta_tools.cli.cmds.metastatus.list_clusters', autospec=True,
    ) as mock_list_clusters, mock.patch(
        'paasta_tools.cli.cmds.metastatus.print_cluster_status', autospec=True,
    ) as mock_print_cluster_status, mock.patch(
        'paasta_tools.cli.cmds.metastatus.load_system_paasta_config', autospec=True,
    ):
        mock_list_clusters.return_value = ['cluster1', 'cluster2', 'cluster3']
        mock_print_cluster_status.side_effect = [0, 0, 255]

        return_code = metastatus.paasta_metastatus(args)
        assert return_code == 1
        assert mock_print_cluster_status.call_count == 3
