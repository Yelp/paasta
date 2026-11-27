#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
"""
Usage: ./paasta_cleanup_stale_nodes.py [options]

Removes non-existent Kubernetes nodes from the Kubernetes API.
"""
import argparse
import logging
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

import boto3
from boto3_type_annotations.ec2 import Client
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Node
from kubernetes.client import V1Preconditions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import get_all_nodes

log = logging.getLogger(__name__)

PROTECTED_POOLS = {"system"}
EC2_FILTER_BATCH_SIZE = 200
# the transition states (stopping, terminating) are normal and Karpenter/K8s are
# still doing their thing - we only want to clean things up that are fully gone
# and somehow not yet GC'd from k8s
DEAD_INSTANCE_STATES = {"terminated", "stopped"}


class StaleNodeSafetyError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove Kubernetes nodes whose EC2 instance no longer exists"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true", dest="dry_run", default=False
    )
    parser.add_argument(
        "--max-deletions",
        type=int,
        default=1,
        help="Maximum number of nodes to delete per run (default: %(default)s)",
    )
    parser.add_argument(
        "--max-stale-pct",
        type=float,
        default=20.0,
        help=(
            "Refuse to delete anything if more than this percent of all nodes appear "
            "stale, as that likely means something else is broken (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--min-deleting-seconds",
        type=int,
        default=60,
        help=(
            "Only clean up nodes that are already being deleted (e.g. by Karpenter) if "
            "they've been stuck deleting for at least this long (default: %(default)s)"
        ),
    )
    return parser.parse_args()


def get_instance_id(node: V1Node) -> Optional[str]:
    # providerID looks like aws:///us-west-2a/i-00b22800f55c64d68
    provider_id = (
        node.spec.provider_id or ""
    )  # XXX: how do we sometimes have None providerIDs?!
    if not provider_id.startswith("aws://"):
        # XXX: should we throw an exception here? we only have aws nodes :p
        return None
    _, _, instance_id = provider_id.rpartition("/")
    return instance_id


def get_ready_condition_transition_time(node: V1Node) -> Optional[datetime]:
    for condition in node.status.conditions or []:
        if condition.type == "Ready":
            return condition.last_transition_time
    return None


def is_node_ready(node: V1Node) -> bool:
    for condition in node.status.conditions or []:
        if condition.type == "Ready":
            return condition.status == "True"
    # err on the side of not touching nodes we can't make sense of
    log.error(
        f"no Ready condition found for node {node.metadata.name}. Conditions {node.status.conditions}"
    )
    return True


def is_cleanup_candidate(
    node: V1Node, now: datetime, min_deleting_age: timedelta
) -> bool:
    if (
        is_node_ready(node)
        or (node.metadata.labels or {}).get("paasta.yelp.com/pool") in PROTECTED_POOLS
    ):
        return False
    # NOTE: nodes already being deleted (i.e. with a deletionTimestamp) are intentionally
    # included: if Karpenter loses track of a node mid-termination, its finalizer keeps the
    # Node object around indefinitely even though the instance is long gone. We do give
    # Karpenter some time to finish normal teardowns though, so we don't race it.
    deletion_timestamp = node.metadata.deletion_timestamp
    return deletion_timestamp is None or now - deletion_timestamp >= min_deleting_age


def get_instance_states(
    ec2_client: Client, instance_ids: Sequence[str]
) -> Dict[str, str]:
    """Returns a mapping of instance id -> EC2 state name for the instances EC2 knows about.
    Instances that EC2 doesn't know about at all are not included.

    Unlike describe_instance_status(InstanceIds=...), filtering on instance-id doesn't
    error out if any of the ids don't exist, so we can check many instances per call.
    """
    states: Dict[str, str] = {}
    paginator = ec2_client.get_paginator("describe_instances")
    for i in range(0, len(instance_ids), EC2_FILTER_BATCH_SIZE):
        batch = list(instance_ids[i : i + EC2_FILTER_BATCH_SIZE])
        for page in paginator.paginate(
            Filters=[{"Name": "instance-id", "Values": batch}]
        ):
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    states[instance["InstanceId"]] = instance["State"]["Name"]
    return states


def find_stale_nodes(
    ec2_client: Client,
    nodes: Sequence[V1Node],
    now: datetime,
    min_deleting_age: timedelta,
) -> List[V1Node]:
    """Returns NotReady nodes whose backing EC2 instance is dead (missing from EC2,
    terminated, or stopped), ordered by how long they've been NotReady (longest first)."""
    candidates: Dict[str, V1Node] = {}
    canary: Optional[V1Node] = None
    canary_instance_id: Optional[str] = None
    for node in nodes:
        instance_id = get_instance_id(node)
        if instance_id is None:
            continue
        if is_cleanup_candidate(node, now, min_deleting_age):
            candidates[instance_id] = node
        elif (
            canary is None
            and is_node_ready(node)
            and node.metadata.deletion_timestamp is None
        ):
            canary, canary_instance_id = node, instance_id

    if not candidates:
        return []
    if canary is None or canary_instance_id is None:
        raise StaleNodeSafetyError(
            "no Ready node available to use as an EC2 canary; refusing to continue"
        )

    states = get_instance_states(ec2_client, [canary_instance_id, *candidates])
    if states.get(canary_instance_id) != "running":
        raise StaleNodeSafetyError(
            f"canary instance {canary_instance_id} (node {canary.metadata.name}) was not "
            "reported as running by EC2; our view of EC2 is likely wrong, refusing to continue"
        )

    stale = []
    for instance_id, node in candidates.items():
        state = states.get(instance_id)
        log.debug(f"{node.metadata.name} ({instance_id}) state: {state}")
        if state is None or state in DEAD_INSTANCE_STATES:
            stale.append(node)

    epoch = datetime.min.replace(tzinfo=timezone.utc)
    return sorted(stale, key=lambda n: get_ready_condition_transition_time(n) or epoch)


def delete_node(client: KubeClient, node: V1Node) -> None:
    finalizers = node.metadata.finalizers or []
    has_karpenter_finalizer = "karpenter.sh/termination" in finalizers
    already_deleting = node.metadata.deletion_timestamp is not None

    if already_deleting and not has_karpenter_finalizer:
        # something other than Karpenter is holding this node - don't guess at what
        raise StaleNodeSafetyError(
            f"node {node.metadata.name} is already being deleted and has no "
            f"karpenter.sh/termination finalizer to remove (finalizers: {finalizers})"
        )

    if not already_deleting:
        client.core.delete_node(
            node.metadata.name,
            body=V1DeleteOptions(
                # fail if the node has been replaced or modified since we inspected it
                preconditions=V1Preconditions(
                    uid=node.metadata.uid,
                    resource_version=node.metadata.resource_version,
                )
            ),
            # we get rid of the node ASAP so that if anything else happens to be held up with their own finalizers
            # we don't get blocked by them (and any other controllers should also see that the Node is gone and
            # finish cleaning things up even if they were uselessly blocked before :p)
            propagation_policy="Background",
        )
        if not has_karpenter_finalizer:
            # the delete is all we need, nothing should block it
            return

    # the instance is gone, so there's nothing for Karpenter's termination flow to do
    # (drain, wait for volume detachment, etc.) - remove its finalizer so the Node can
    # actually go away now rather than waiting for Karpenter (or our next run!)
    #
    # NOTE: this MUST be a JSON patch (i.e. a list of ops) so that a k8s strategic merge
    # doesn't result in silently dropping the patch when we try to clear things like the
    # finalizer list with an empty list :p
    patched_node = client.core.patch_node(
        node.metadata.name,
        [
            # ensure that the node hasn't changed since we first looked at it
            {"op": "test", "path": "/metadata/uid", "value": node.metadata.uid},
            {"op": "test", "path": "/metadata/finalizers", "value": finalizers},
            # and only after verifying, remove the karpenter finalizer
            {
                "op": "replace",
                "path": "/metadata/finalizers",
                "value": [f for f in finalizers if f != "karpenter.sh/termination"],
            },
        ],
    )
    if "karpenter.sh/termination" in (patched_node.metadata.finalizers or []):
        raise StaleNodeSafetyError(
            f"patched node {node.metadata.name} but karpenter.sh/termination "
            f"finalizer is still present (finalizers: {patched_node.metadata.finalizers})"
        )


def cleanup_stale_nodes(
    kube_client: KubeClient,
    ec2_client: Client,
    nodes: Sequence[V1Node],
    *,
    dry_run: bool = False,
    max_deletions: int = 1,
    max_stale_pct: float = 20.0,
    min_deleting_age: timedelta = timedelta(seconds=60),
) -> int:
    """Finds and cleans up stale nodes, returning an exit code."""
    try:
        stale_nodes = find_stale_nodes(
            ec2_client,
            nodes,
            now=datetime.now(timezone.utc),
            min_deleting_age=min_deleting_age,
        )
    except StaleNodeSafetyError as e:
        log.error(str(e))
        return 1

    if not stale_nodes:
        log.debug("no stale nodes found")
        return 0

    stale_names = [node.metadata.name for node in stale_nodes]
    stale_pct = 100 * len(stale_nodes) / len(nodes)
    log.info(
        f"found {len(stale_nodes)}/{len(nodes)} ({stale_pct:.1f}%) stale nodes: {stale_names}"
    )
    if stale_pct > max_stale_pct:
        log.error(
            f"Skipping deletion: {stale_pct:.1f}% of nodes are stale, which exceeds the {max_stale_pct}% "
            "safety threshold - something has likely gone very wrong!"
        )
        return 1

    to_delete = stale_nodes[:max_deletions]
    if dry_run:
        log.info(f"dry run: would have deleted {[n.metadata.name for n in to_delete]}")
        return 0

    exit_code = 0
    for node in to_delete:
        try:
            delete_node(kube_client, node)
        except (ApiException, StaleNodeSafetyError) as e:
            log.error(f"error deleting node {node.metadata.name}: {e}")
            exit_code = 1
            continue
        log.info(f"successfully deleted node {node.metadata.name}")
    return exit_code


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    # only make our own logs verbose - the k8s client and botocore are very noisy
    if args.verbose:
        log.setLevel(logging.DEBUG)

    kube_client = KubeClient()
    all_nodes = get_all_nodes(kube_client)
    if not all_nodes:
        log.error("no nodes found in cluster, this should never happen")
        return 1

    labels = all_nodes[0].metadata.labels or {}
    # all nodes will be in the same region with how we lay out our clusters
    region = labels.get("topology.kubernetes.io/region")
    if not region:
        log.error("unable to determine region from nodes, this should never happen")
        return 1

    return cleanup_stale_nodes(
        kube_client,
        boto3.client("ec2", region),
        all_nodes,
        dry_run=args.dry_run,
        max_deletions=args.max_deletions,
        max_stale_pct=args.max_stale_pct,
        min_deleting_age=timedelta(seconds=args.min_deleting_seconds),
    )


if __name__ == "__main__":
    sys.exit(main())
