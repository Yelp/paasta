import pytest
from kubernetes.client import V1ResourceRequirements

from paasta_tools.contrib.get_running_task_allocation import (
    get_kubernetes_resource_request_limit,
)
from paasta_tools.contrib.get_running_task_allocation import get_matching_namespaces


def test_get_kubernetes_resource_request_limit():
    test_resource_req = V1ResourceRequirements(
        limits={"cpu": "1.3", "memory": "2048Mi", "ephemeral-storage": "4096Mi"},
        requests={
            "cpu": "0.3",
            "memory": "2048Mi",
            "ephemeral-storage": "4096Mi",
        },
    )

    assert get_kubernetes_resource_request_limit(test_resource_req) == {
        "cpus": 0.3,
        "cpus_limit": 1.3,
        "disk": 4096.0,
        "mem": 2048.0,
    }


@pytest.mark.parametrize(
    "namespaces, namespace_prefix, additional_namespaces, expected",
    (
        (
            ["paasta", "paasta-flink", "paasta-spark", "luisp-was-here", "tron"],
            "paasta",
            ["tron"],
            ["paasta", "paasta-flink", "paasta-spark", "tron"],
        ),
        (
            ["paasta", "paasta-flink", "paasta-spark", "luisp-was-here", "tron"],
            "paasta",
            [""],
            ["paasta", "paasta-flink", "paasta-spark"],
        ),
    ),
)
def test_get_matching_namespaces(
    namespaces, namespace_prefix, additional_namespaces, expected
):
    assert sorted(
        get_matching_namespaces(namespaces, namespace_prefix, additional_namespaces)
    ) == sorted(expected)
