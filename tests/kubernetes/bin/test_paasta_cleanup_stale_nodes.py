from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from unittest import mock

from boto3_type_annotations.ec2 import Client
from kubernetes.client import CoreV1Api
from kubernetes.client import V1Node
from kubernetes.client import V1NodeCondition
from kubernetes.client import V1NodeSpec
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta

from paasta_tools.kubernetes.bin.paasta_cleanup_stale_nodes import cleanup_stale_nodes
from paasta_tools.kubernetes_tools import KubeClient

NOW = datetime.now(timezone.utc)

# a node plus the EC2 state of its instance (None meaning EC2 doesn't know about it)
NodeAndInstanceState = Tuple[V1Node, Optional[str]]


def make_node(
    name: str,
    ec2_state: Optional[str] = "running",
    ready: bool = True,
    not_ready_for: timedelta = timedelta(minutes=1),
    deleting_for: Optional[timedelta] = None,
    pool: str = "default",
) -> NodeAndInstanceState:
    node = V1Node(
        metadata=V1ObjectMeta(
            name=name,
            uid=f"uid-{name}",
            resource_version="1",
            labels={
                "topology.kubernetes.io/region": "us-west-2",
                "paasta.yelp.com/pool": pool,
            },
            deletion_timestamp=NOW - deleting_for if deleting_for else None,
            finalizers=["karpenter.sh/termination", "other"],
        ),
        spec=V1NodeSpec(provider_id=f"aws:///us-west-2a/i-{name}"),
        status=V1NodeStatus(
            conditions=[
                V1NodeCondition(
                    type="Ready",
                    status="True" if ready else "Unknown",
                    last_transition_time=NOW - not_ready_for,
                )
            ]
        ),
    )
    return node, ec2_state


class FakeEC2:
    """Just enough of the EC2 client for describe_instances pagination."""

    def __init__(self, states: Dict[str, str]) -> None:
        self.states = states

    def get_paginator(self, name: str) -> "FakeEC2":
        assert name == "describe_instances"
        return self

    def paginate(self, Filters: List[Dict[str, List[str]]]) -> Iterator[dict]:
        ids = Filters[0]["Values"]
        yield {
            "Reservations": [
                {
                    "Instances": [
                        {"InstanceId": iid, "State": {"Name": self.states[iid]}}
                        for iid in ids
                        if iid in self.states
                    ]
                }
            ]
        }


def make_kube_client() -> mock.Mock:
    # KubeClient sets .core in __init__, so autospec can't see it - spec it explicitly
    return mock.create_autospec(
        KubeClient, instance=True, core=mock.create_autospec(CoreV1Api, instance=True)
    )


def cleanup(nodes_and_states: List[NodeAndInstanceState], **kwargs: Any) -> tuple:
    """Runs cleanup_stale_nodes against fakes, returning (exit code, fake CoreV1Api)."""
    kube_client = make_kube_client()
    ec2_states = {
        f"i-{node.metadata.name}": state
        for node, state in nodes_and_states
        if state is not None
    }
    nodes = [node for node, _ in nodes_and_states]
    exit_code = cleanup_stale_nodes(kube_client, FakeEC2(ec2_states), nodes, **kwargs)
    return exit_code, kube_client.core


def make_healthy_nodes(count: int) -> List[NodeAndInstanceState]:
    return [make_node(f"ok{i}") for i in range(count)]


def test_cleans_up_oldest_single_stale_node():
    nodes = make_healthy_nodes(20) + [
        # instance gone entirely; NotReady the longest -> should be the one deleted
        make_node(
            "gone-old", ec2_state=None, ready=False, not_ready_for=timedelta(hours=1)
        ),
        make_node("gone-new", ec2_state=None, ready=False),
        # instance stopped - counts as gone, but newer
        make_node(
            "stopped",
            ec2_state="stopped",
            ready=False,
            not_ready_for=timedelta(minutes=5),
        ),
        # Karpenter should handle shutting-down/stopping on its own - keep
        make_node(
            "shutting-down",
            ec2_state="shutting-down",
            ready=False,
            not_ready_for=timedelta(days=5),
        ),
        make_node(
            "stopping",
            ec2_state="stopping",
            ready=False,
            not_ready_for=timedelta(days=5),
        ),
        # NotReady but instance still running - keep
        make_node(
            "flaky", ec2_state="running", ready=False, not_ready_for=timedelta(days=1)
        ),
        # system pool nodes are never touched, even if their instance is gone
        make_node(
            "system",
            ec2_state=None,
            ready=False,
            not_ready_for=timedelta(days=4),
            pool="system",
        ),
        # Karpenter only just started tearing this down - give it time to finish
        make_node(
            "karpenter-new",
            ec2_state=None,
            ready=False,
            not_ready_for=timedelta(days=3),
            deleting_for=timedelta(seconds=10),
        ),
    ]

    exit_code, core = cleanup(nodes)

    assert exit_code == 0
    core.delete_node.assert_called_once()
    (name,), kwargs = core.delete_node.call_args
    assert name == "gone-old"
    assert kwargs["body"].preconditions.uid == "uid-gone-old"

    # dry run never mutates anything
    exit_code, core = cleanup(nodes, dry_run=True)
    assert exit_code == 0
    core.delete_node.assert_not_called()


def test_refuses_when_something_looks_wrong():
    # 3 of 12 (25%) stale > 20% default threshold
    too_many_stale = make_healthy_nodes(9) + [
        make_node(f"gone{i}", ec2_state=None, ready=False) for i in range(3)
    ]
    # e.g. wrong region/account: EC2 returns nothing at all, so every NotReady node
    # looks stale. The Ready-node canary should catch this before any deletion.
    ec2_sees_nothing = [make_node(f"ok{i}", ec2_state=None) for i in range(20)] + [
        make_node("gone", ec2_state=None, ready=False)
    ]

    for nodes in (too_many_stale, ec2_sees_nothing):
        exit_code, core = cleanup(nodes)
        assert exit_code == 1
        core.delete_node.assert_not_called()


def test_noop_without_candidates():
    kube_client = make_kube_client()
    ec2_client = mock.create_autospec(Client, instance=True)
    nodes = [node for node, _ in make_healthy_nodes(5)]

    assert cleanup_stale_nodes(kube_client, ec2_client, nodes) == 0
    # if everything was healthy, we shouldn't have used the EC2 API
    ec2_client.get_paginator.assert_not_called()
    kube_client.core.delete_node.assert_not_called()
