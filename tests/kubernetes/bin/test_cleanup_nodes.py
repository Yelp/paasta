import mock
import pytest
from kubernetes.client.rest import ApiException
from kubernetes.client import V1DeleteOptions

from paasta_tools.kubernetes.bin.cleanup_nodes import main
from paasta_tools.kubernetes.bin.cleanup_nodes import nodes_for_cleanup
from paasta_tools.kubernetes.bin.cleanup_nodes import terminate_nodes

def test_nodes_for_cleanup():
    with mock.patch('paasta_tools.kubernetes.bin.cleanup_nodes.is_node_ready', side_effect=[True, False, False]):
        m1, m2, m3 = mock.Mock(), mock.Mock(), mock.Mock()
        for_cleanup = nodes_for_cleanup([m1,m2,m3])
        assert for_cleanup == [m2,m3]

def test_terminate_nodes():

    mock_client = mock.MagicMock()
    mock_client.core.delete_node.side_effect = [None, ApiException(404), None]

    m1, m2, m3 = mock.Mock(), mock.Mock(), mock.Mock()
    success, errors = terminate_nodes(
        client=mock_client, 
        nodes=[m1, m2, m3], 
    )
    expected_calls = [
        mock.call.core.delete_node(node, body=V1DeleteOptions(), propagation_policy='foreground') 
        for node in [m1, m2, m3]
    ]

    assert mock_client.mock_calls == expected_calls
    assert success == [m1, m3]
    assert errors[0][0] == m2
    assert isinstance(errors[0][1], ApiException)


    mock_client.reset_mock()

    mock_client.core.delete_node.side_effect = [None, ApiException(404), None]
    success, errors = terminate_nodes(
        client=mock_client, 
        nodes=[m1, m2, m3], 
    )
    expected_calls = [
        mock.call.core.delete_node(node, body=V1DeleteOptions(), propagation_policy='foreground') 
        for node in [m1,m2,m3]
    ]
    assert mock_client.mock_calls == expected_calls
    assert success == [m1, m3]
    assert errors[0][0] == m2
    assert isinstance(errors[0][1], ApiException)


def test_main():
    with mock.patch(
        "paasta_tools.kubernetes.bin.cleanup_nodes.get_all_nodes", autospec=True
    ) as mock_get_all_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.cleanup_nodes.KubeClient", autospec=True
    ) as mock_kube_client, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.is_node_ready', autospec=True
    ) as mock_is_node_ready, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.nodes_for_cleanup', autospec=True
    ) as mock_nodes_for_cleanup, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.terminate_nodes', autospec=True
    ) as mock_terminate_nodes, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.parse_args', autospec=True
    ) as mock_parse_args:
        mock_args = mock.MagicMock()
        mock_args.dry_run = False

        mock_parse_args.return_value = mock_args

        m1, m2, m3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        mock_get_all_nodes.return_value = [m1, m2, m3]

        mock_is_node_ready.side_effect = [True, False, False]

        mock_terminate_nodes.return_value = ([], [('foo', mock.MagicMock())])
        with pytest.raises(SystemExit) as e:
            main()
            mock_terminate_nodes.assert_called_once_with([m2, m3])
            assert e.value.code == 1


def test_main_dry_run():
    with mock.patch(
        "paasta_tools.kubernetes.bin.cleanup_nodes.get_all_nodes", autospec=True
    ) as mock_get_all_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.cleanup_nodes.KubeClient", autospec=True
    ) as mock_kube_client, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.is_node_ready', autospec=True
    ) as mock_is_node_ready, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.nodes_for_cleanup', autospec=True
    ) as mock_nodes_for_cleanup, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.terminate_nodes', 
    ) as mock_terminate_nodes, mock.patch(
        'paasta_tools.kubernetes.bin.cleanup_nodes.parse_args', autospec=True
    ) as mock_parse_args:
        mock_args = mock.MagicMock()
        mock_args.dry_run = True

        mock_parse_args.return_value = mock_args
        m1, m2, m3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        mock_get_all_nodes.return_value = [m1, m2, m3]
        mock_is_node_ready.side_effect = [True, False, False]

        main()
        mock_terminate_nodes.assert_not_called()