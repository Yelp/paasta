from collections import defaultdict
from collections import namedtuple

import mock
import pytest

from paasta_tools.kubernetes.bin.kube_state_metrics_collector import (
    collect_and_emit_metrics,
)


Family = namedtuple("Family", ["name", "samples"])

Sample = namedtuple("Sample", ["name", "labels", "value"])


@pytest.fixture
def mock_metrics_interface():
    with mock.patch(
        "paasta_tools.kubernetes.bin.kube_state_metrics_collector.get_metrics_interface",
        autospec=True,
    ) as _mock_get_metrics_interface:
        mock_metrics_interface = _mock_get_metrics_interface.return_value
        yield mock_metrics_interface


@pytest.fixture
def mock_system_paasta_config():
    with mock.patch(
        "paasta_tools.kubernetes.bin.kube_state_metrics_collector.load_system_paasta_config",
        autospec=True,
    ) as _mock_load_system_paasta_config:
        mock_system_paasta_config = mock.Mock()
        mock_system_paasta_config.get_cluster.return_value = "westeros-prod"
        _mock_load_system_paasta_config.return_value = mock_system_paasta_config
        yield mock_system_paasta_config


@pytest.fixture(autouse=True)
def mock_requests_get():
    with mock.patch("requests.get", autospec=True) as mock_requests_get:
        mock_requests_get.return_value.text = ""
        yield


@pytest.fixture
def mock_get_kube_state_metrics_url():
    with mock.patch(
        "paasta_tools.kubernetes.bin.kube_state_metrics_collector._get_kube_state_metrics_url",
        autospec=True,
    ) as _mock_get_kube_state_metrics_url:
        _mock_get_kube_state_metrics_url.return_value = "http://abc.def"
        yield _mock_get_kube_state_metrics_url


@pytest.fixture
def mock_text_string_to_metric_families():
    with mock.patch(
        "paasta_tools.kubernetes.bin.kube_state_metrics_collector.text_string_to_metric_families",
        autospec=True,
    ) as _mock_text_string_to_metric_families:
        yield _mock_text_string_to_metric_families


def test_exit_if_no_kube_state_metrics(
    mock_get_kube_state_metrics_url, mock_metrics_interface,
):
    mock_get_kube_state_metrics_url.return_value = None
    collect_and_emit_metrics()
    assert not mock_metrics_interface.create_gauge.called


def test_empty_config(
    mock_get_kube_state_metrics_url, mock_system_paasta_config, mock_metrics_interface
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {}
    collect_and_emit_metrics()
    assert not mock_metrics_interface.create_gauge.called


def test_unaggregated_metric(
    mock_get_kube_state_metrics_url,
    mock_text_string_to_metric_families,
    mock_system_paasta_config,
    mock_metrics_interface,
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {
        "unaggregated_metrics": ["test_metric"]
    }
    mock_text_string_to_metric_families.return_value = [
        Family(
            name="test_metric",
            samples=[Sample(name="test_metric", labels={"label": "value"}, value=1),],
        ),
        Family(
            name="other_metric",
            samples=[Sample(name="other_metric", labels={}, value=5),],
        ),
    ]
    collect_and_emit_metrics()
    mock_metrics_interface.create_gauge.assert_called_once_with(
        "test_metric",
        label="value",
        paasta_cluster="westeros-prod",
        kubernetes_cluster="westeros-prod",
    )
    mock_metrics_interface.create_gauge.return_value.set.assert_called_once_with(1)


def test_aggregated_metric(
    mock_get_kube_state_metrics_url,
    mock_text_string_to_metric_families,
    mock_system_paasta_config,
    mock_metrics_interface,
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {
        "summed_metric_to_group_keys": {"test_metric": ["sum_key"],}
    }
    mock_text_string_to_metric_families.return_value = [
        Family(
            name="test_metric",
            samples=[
                Sample(name="test_metric", labels={"sum_key": "a"}, value=1),
                Sample(name="test_metric", labels={"sum_key": "a"}, value=2),
                Sample(name="test_metric", labels={"sum_key": "a"}, value=3),
                Sample(name="test_metric", labels={"sum_key": "b"}, value=9),
                Sample(name="test_metric", labels={"sum_key": "b"}, value=11),
            ],
        )
    ]
    gauges = defaultdict(mock.Mock)
    mock_metrics_interface.create_gauge.side_effect = lambda name, **dimensions: gauges[
        (name, dimensions["sum_key"])
    ]

    collect_and_emit_metrics()
    mock_metrics_interface.create_gauge.assert_has_calls(
        [
            mock.call(
                "test_metric",
                paasta_cluster="westeros-prod",
                kubernetes_cluster="westeros-prod",
                sum_key="a",
            ),
            mock.call(
                "test_metric",
                paasta_cluster="westeros-prod",
                kubernetes_cluster="westeros-prod",
                sum_key="b",
            ),
        ]
    )
    assert mock_metrics_interface.create_gauge.call_count == 2
    gauges["test_metric", "a"].set.assert_called_once_with(1 + 2 + 3)
    gauges["test_metric", "b"].set.assert_called_once_with(9 + 11)


def test_label_joining_old(
    mock_get_kube_state_metrics_url,
    mock_text_string_to_metric_families,
    mock_system_paasta_config,
    mock_metrics_interface,
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {
        "unaggregated_metrics": ["test_metric"],
        "label_metric_to_label_key": {"test_labels": ["source", "dest"],},
    }
    mock_text_string_to_metric_families.return_value = [
        Family(
            name="test_metric",
            samples=[Sample(name="test_metric", labels={"dest": "abc"}, value=4),],
        ),
        Family(
            name="test_labels",
            samples=[
                Sample(
                    name="test_labels",
                    labels={"source": "abc", "is_good": "true",},
                    value=1,
                ),
            ],
        ),
    ]

    collect_and_emit_metrics()
    mock_metrics_interface.create_gauge.assert_called_once_with(
        "test_metric",
        paasta_cluster="westeros-prod",
        kubernetes_cluster="westeros-prod",
        dest="abc",
        is_good="true",
    )
    mock_metrics_interface.create_gauge.return_value.set.assert_called_once_with(4)


def test_label_joining(
    mock_get_kube_state_metrics_url,
    mock_text_string_to_metric_families,
    mock_system_paasta_config,
    mock_metrics_interface,
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {
        "unaggregated_metrics": ["test_metric"],
        "label_metric_to_label_key": {
            "test_labels": {
                "source_key": "source_key",
                "destination_keys": ["dest_key"],
            },
        },
    }
    mock_text_string_to_metric_families.return_value = [
        Family(
            name="test_metric",
            samples=[Sample(name="test_metric", labels={"dest_key": "abc"}, value=4),],
        ),
        Family(
            name="test_labels",
            samples=[
                Sample(
                    name="test_labels",
                    labels={"source_key": "abc", "is_good": "true",},
                    value=1,
                ),
            ],
        ),
    ]

    collect_and_emit_metrics()
    mock_metrics_interface.create_gauge.assert_called_once_with(
        "test_metric",
        paasta_cluster="westeros-prod",
        kubernetes_cluster="westeros-prod",
        dest_key="abc",
        is_good="true",
    )
    mock_metrics_interface.create_gauge.return_value.set.assert_called_once_with(4)


def test_label_renaming(
    mock_get_kube_state_metrics_url,
    mock_text_string_to_metric_families,
    mock_system_paasta_config,
    mock_metrics_interface,
):
    mock_system_paasta_config.get_kube_state_metrics_collector_config.return_value = {
        "unaggregated_metrics": ["test_metric"],
        "label_renames": {"constantinople": "istanbul",},
    }
    mock_text_string_to_metric_families.return_value = [
        Family(
            name="test_metric",
            samples=[
                Sample(name="test_metric", labels={"constantinople": "xyz"}, value=6)
            ],
        ),
    ]

    collect_and_emit_metrics()
    mock_metrics_interface.create_gauge.assert_called_once_with(
        "test_metric",
        paasta_cluster="westeros-prod",
        kubernetes_cluster="westeros-prod",
        istanbul="xyz",
    )
    mock_metrics_interface.create_gauge.return_value.set.assert_called_once_with(6)
