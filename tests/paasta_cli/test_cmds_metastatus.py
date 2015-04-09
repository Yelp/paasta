import mock

from paasta_tools.paasta_cli.cmds import metastatus


def test_report_cluster_status():
    cluster = 'fake_cluster'
    thing_to_patch = 'paasta_tools.paasta_cli.cmds.metastatus.execute_paasta_metastatus_on_remote_master'
    with mock.patch(thing_to_patch) as mock_execute_paasta_metastatus_on_remote_master:
        mock_execute_paasta_metastatus_on_remote_master.return_value = 'mock_status'
        actual = metastatus.report_cluster_status(cluster)
        mock_execute_paasta_metastatus_on_remote_master.assert_called_once_with(
            cluster, False
        )
        assert 'cluster: %s' % cluster in actual
        assert 'mock_status' in actual


def test_figure_out_clusters_to_inspect_respects_the_user():
    fake_args = mock.Mock()
    fake_args.clusters = 'a,b,c'
    fake_all_clusters = ['a', 'b', 'c', 'd']
    assert ['a', 'b', 'c'] == metastatus.figure_out_clusters_to_inspect(fake_args, fake_all_clusters)
