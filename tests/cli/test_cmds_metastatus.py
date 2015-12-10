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
from StringIO import StringIO

from paasta_tools.cli.cmds import metastatus
from paasta_tools.smartstack_tools import DEFAULT_SYNAPSE_PORT


@mock.patch('sys.stdout', new_callable=StringIO)
def test_report_cluster_status(mock_stdout):
    cluster = 'fake_cluster'
    thing_to_patch = 'paasta_tools.cli.cmds.metastatus.execute_paasta_metastatus_on_remote_master'
    with mock.patch(thing_to_patch) as mock_execute_paasta_metastatus_on_remote_master:
        mock_execute_paasta_metastatus_on_remote_master.return_value = 'mock_status'
        metastatus.print_cluster_status(cluster)
        mock_execute_paasta_metastatus_on_remote_master.assert_called_once_with(
            cluster, False
        )
        actual = mock_stdout.getvalue()
        assert 'Cluster: %s' % cluster in actual
        assert 'mock_status' in actual


def test_figure_out_clusters_to_inspect_respects_the_user():
    fake_args = mock.Mock()
    fake_args.clusters = 'a,b,c'
    fake_all_clusters = ['a', 'b', 'c', 'd']
    assert ['a', 'b', 'c'] == metastatus.figure_out_clusters_to_inspect(fake_args, fake_all_clusters)


def test_get_cluster_dashboards():
    output_text = metastatus.get_cluster_dashboards('fake-cluster')
    assert 'http://paasta-fake-cluster.yelp:5050' in output_text
    assert 'http://paasta-fake-cluster.yelp:5052' in output_text
    assert 'http://paasta-fake-cluster.yelp:5053' in output_text
    assert 'http://paasta-fake-cluster.yelp:%s' % DEFAULT_SYNAPSE_PORT in output_text
    assert 'http://chronos.paasta-fake-cluster.yelp/' in output_text
    assert 'http://mesos.paasta-fake-cluster.yelp/' in output_text
    assert 'http://marathon.paasta-fake-cluster.yelp/' in output_text
