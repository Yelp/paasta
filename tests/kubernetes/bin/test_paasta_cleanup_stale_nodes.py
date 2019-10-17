import mock
import pytest
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes import does_instance_exist
from paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes import main
from paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes import nodes_for_cleanup
from paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes import terminate_nodes


def test_nodes_for_cleanup():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.terminated_nodes",
        autospec=True,
    ) as mock_terminated_nodes:
        m1, m2, m3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        mock_client = mock.Mock()
        mock_terminated_nodes.return_value = [m2, m3]
        for_cleanup = nodes_for_cleanup(mock_client, [m1, m2, m3])
        assert for_cleanup == [m2, m3]


def test_terminate_nodes():

    mock_client = mock.MagicMock()
    mock_client.core.delete_node.side_effect = [None, ApiException(404), None]

    m1, m2, m3 = mock.Mock(), mock.Mock(), mock.Mock()
    success, errors = terminate_nodes(client=mock_client, nodes=[m1, m2, m3])
    expected_calls = [
        mock.call.core.delete_node(
            node, body=V1DeleteOptions(), propagation_policy="foreground"
        )
        for node in [m1, m2, m3]
    ]

    assert mock_client.mock_calls == expected_calls
    assert success == [m1, m3]
    assert errors[0][0] == m2
    assert isinstance(errors[0][1], ApiException)

    mock_client.reset_mock()

    mock_client.core.delete_node.side_effect = [None, ApiException(404), None]
    success, errors = terminate_nodes(client=mock_client, nodes=[m1, m2, m3])
    expected_calls = [
        mock.call.core.delete_node(
            node, body=V1DeleteOptions(), propagation_policy="foreground"
        )
        for node in [m1, m2, m3]
    ]
    assert mock_client.mock_calls == expected_calls
    assert success == [m1, m3]
    assert errors[0][0] == m2
    assert isinstance(errors[0][1], ApiException)


def test_does_instance_exist():

    # if the node doesn't exist at all, then the client will raise an exception
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.ClientError",
        autospec=True,
    ) as mock_error:
        mock_error.response = {"Error": {"Code": 404}}
        mock_client = mock.MagicMock()
        mock_client.side_effect = mock_error

        assert does_instance_exist(mock_client, "i-12345") is False

    statuses = [
        "pending",
        "running",
        "shutting-down",
        "terminated",
        "stopping",
        "stopped",
    ]
    running = [True, True, False, False, False, False]

    for status, running in zip(statuses, running):
        mock_client.reset_mock()
        mock_client.describe_instance_status.return_value = {
            "InstanceStatuses": [{"InstanceState": {"Name": status}}]
        }
        assert does_instance_exist(mock_client, "i-12345") is running

    # finally, there have been instances where the client doesn't 404, but there
    # isn't a status attached to the instance
    mock_client.reset_mock()
    mock_client.describe_instance_status.return_value = {"InstanceStatuses": []}
    assert does_instance_exist(mock_client, "i-12345") is False


def test_main():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.get_all_nodes",
        autospec=True,
    ) as mock_get_all_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.KubeClient",
        autospec=True,
    ) as mock_kube_client, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.nodes_for_cleanup",
        autospec=True,
    ) as mock_nodes_for_cleanup, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.terminate_nodes",
        autospec=True,
    ) as mock_terminate_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.parse_args",
        autospec=True,
    ) as mock_parse_args, mock.patch(
        "boto3.client", autospec=True
    ):
        mock_args = mock.MagicMock()
        mock_args.dry_run = False

        mock_parse_args.return_value = mock_args

        m1 = mock.MagicMock(metadata=mock.Mock())
        m2 = mock.Mock(metadata=mock.Mock())
        m3 = mock.Mock(metadata=mock.Mock())

        for i, m in enumerate([m1, m2, m3]):
            m.metadata.name = f"m{i+1}"
            m.metadata.labels = {
                "failure-domain.beta.kubernetes.io/region": "us-west-1"
            }

        mock_get_all_nodes.return_value = [m1, m2, m3]
        mock_nodes_for_cleanup.return_value = [m2, m3]
        mock_terminate_nodes.return_value = (["m2"], [("m3", mock.MagicMock())])

        with pytest.raises(SystemExit) as e:
            main()

        mock_terminate_nodes.assert_called_once_with(mock_kube_client(), ["m2", "m3"])
        assert e.value.code == 1

        mock_terminate_nodes.reset_mock()
        mock_terminate_nodes.return_value = (["m2", "m3"], [])
        main()
        mock_terminate_nodes.assert_called_once_with(mock_kube_client(), ["m2", "m3"])


def test_main_dry_run():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.get_all_nodes",
        autospec=True,
    ) as mock_get_all_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.KubeClient",
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.is_node_ready",
        autospec=True,
    ) as mock_is_node_ready, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.terminate_nodes",
        autospec=True,
    ) as mock_terminate_nodes, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes.parse_args",
        autospec=True,
    ) as mock_parse_args, mock.patch(
        "boto3.client", autospec=True
    ):
        mock_args = mock.MagicMock()
        mock_args.dry_run = True

        mock_parse_args.return_value = mock_args
        m1, m2, m3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        mock_get_all_nodes.return_value = [m1, m2, m3]
        mock_is_node_ready.side_effect = [True, False, False]

        print(mock_terminate_nodes)
        main()

        # https://bugs.python.org/issue28380
        # we can't just use assert_not_called() here,
        # so inspect the list of calls instead
        assert len(mock_terminate_nodes.mock_calls) == 0
