from copy import deepcopy

import mock

from paasta_tools.hpa_metrics_collector.hpa_metrics_collector import collect

fake_data = {
    "items": [
        {
            "metadata": {
                "name": "transactions-prepaid--revert--commission--billing--update-99lcs",
                "namespace": "paasta",
                "annotations": {"autoscaling": "http"},
                "labels": {
                    "paasta.yelp.com/config_sha": "config93f369e8",
                    "paasta.yelp.com/git_sha": "git0d598687",
                    "paasta.yelp.com/instance": "prepaid_revert_commission_billing_update",
                    "paasta.yelp.com/service": "transactions",
                },
            },
            "status": {"phase": "Running", "podIP": "10.145.27.71"},
            "spec": {
                "containers": [
                    {
                        "name": "prepaid--revert--commission--billing--update",
                        "ports": [{"containerPort": 8888, "protocol": "TCP"}],
                    }
                ]
            },
        }
    ]
}


def mock_responses(responses, default_response=None):
    return lambda input: responses[input] if input in responses else default_response


def test_collect():
    with mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.update_metrics",
        autospec=True,
    ) as mock_update_metrics, mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.requests",
        autospec=True,
    ) as mock_requests:
        mock_json = mock.MagicMock()
        mock_json.json.return_value = {"utilization": "0.17"}
        mock_node_info = mock.MagicMock()
        mock_node_info.json.return_value = fake_data
        mock_requests.get.side_effect = mock_responses(
            {"http://169.254.255.254:10255/pods/": mock_node_info},
            default_response=mock_json,
        )
        collect("fake_token", "fake_cluster")
        mock_update_metrics.assert_called_with(
            "transactions-prepaid--revert--commission--billing--update-99lcs",
            "paasta",
            "http",
            "17.0",
            fake_data["items"][0]["metadata"]["labels"],
            "fake_token",
            "fake_cluster",
        )


def test_collect_no_autoscaling():
    with mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.update_metrics",
        autospec=True,
    ) as mock_update_metrics, mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.requests",
        autospec=True,
    ) as mock_requests:
        mock_json = mock.MagicMock()
        mock_json.json.return_value = {"utilization": "0.17"}
        mock_node_info = mock.MagicMock()
        node_info = deepcopy(fake_data)
        node_info["items"][0]["metadata"]["annotations"] = {}
        mock_node_info.json.return_value = node_info
        mock_requests.get.side_effect = mock_responses(
            {"http://169.254.255.254:10255/pods/": mock_node_info},
            default_response=mock_json,
        )
        collect("fake_token", "fake_cluster")
        assert mock_update_metrics.call_count == 0


def test_collect_no_status_no_exception(capfd):
    with mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.update_metrics",
        autospec=True,
    ) as mock_update_metrics, mock.patch(
        "paasta_tools.hpa_metrics_collector.hpa_metrics_collector.requests",
        autospec=True,
    ) as mock_requests:
        mock_json = mock.MagicMock()
        mock_json.json.return_value = {"something is wrong"}
        mock_node_info = mock.MagicMock()
        mock_node_info.json.return_value = fake_data
        mock_requests.get.side_effect = mock_responses(
            {"http://169.254.255.254:10255/pods/": mock_node_info},
            default_response=mock_json,
        )
        assert mock_update_metrics.call_count == 0
