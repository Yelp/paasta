import mock
from StringIO import StringIO

from paasta_tools.paasta_cli.cmds import metastatus


@mock.patch('sys.stdout', new_callable=StringIO)
def test_report_cluster_status(mock_stdout):
    cluster = 'fake_cluster'
    thing_to_patch = 'paasta_tools.paasta_cli.cmds.metastatus.execute_paasta_metastatus_on_remote_master'
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
    assert 'http://paasta-fake-cluster.yelp:3212' in output_text
    assert 'http://chronos.paasta-fake-cluster.yelp/' in output_text
    assert 'http://mesos.paasta-fake-cluster.yelp/' in output_text
    assert 'http://marathon.paasta-fake-cluster.yelp/' in output_text
