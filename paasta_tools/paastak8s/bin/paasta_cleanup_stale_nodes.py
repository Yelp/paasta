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

Removes terminated Kubernetes nodes from the Kubernetes API.

Command line options:

- -v, --verbose: Verbose output
- -n, --dry-run: Only report what would have been deleted
"""
import argparse
import logging
import sys
from typing import List
from typing import Sequence
from typing import Tuple

import boto3
from boto3_type_annotations.ec2 import Client
from botocore.exceptions import ClientError
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Node
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import KubeClient

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove terminated Kubernetes nodes")
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true", dest="dry_run", default=False
    )
    args = parser.parse_args()
    return args


def nodes_for_cleanup(ec2_client: Client, nodes: Sequence[V1Node]) -> List[V1Node]:
    not_ready = [
        node
        for node in nodes
        if not is_node_ready(node)
        and "node-role.kubernetes.io/master" not in node.metadata.labels
    ]
    terminated = terminated_nodes(ec2_client, not_ready)
    return terminated


def terminated_nodes(ec2_client: Client, nodes: Sequence[V1Node]) -> List[V1Node]:
    instance_ids = [node.spec.provider_id.split("/")[-1] for node in nodes]
    # if there are any instances that don't exist in the query to describe_instance_status
    # then amazon won't return the results for any, so we have to query the instances
    # one by one
    statuses = [
        does_instance_exist(ec2_client, instance_id) for instance_id in instance_ids
    ]
    for node, status in zip(nodes, statuses):
        log.debug(f"{node.metadata.name} exists: {status}")

    return [node for node, status in zip(nodes, statuses) if not status]


def does_instance_exist(ec2_client: Client, instance_id: str):
    try:
        instance = ec2_client.describe_instance_status(InstanceIds=[instance_id])
        if instance["InstanceStatuses"]:
            status = instance["InstanceStatuses"][0]["InstanceState"]["Name"]
            # see possible values at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_instance_status
            # 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
            # it's unlikely that we'll ever be in this situation with a pending node - the common case is that
            # the node is either running and been marked as not ready, or it's being shutdown
            if status not in ("running", "pending"):
                return False
            return True

        log.debug(
            f"no instance status in response for {instance_id}; assuming to have been terminated"
        )
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
            log.debug(
                f"instance {instance_id} not found; assuming to have been terminated"
            )
            return False
        else:
            log.error(f"error fetching instance status for {instance_id}")
            raise e
    return True


def terminate_nodes(
    client: KubeClient, nodes: List[str]
) -> Tuple[List[str], List[Tuple[str, Exception]]]:
    success = []
    errors = []
    for node in nodes:
        try:
            body = V1DeleteOptions()
            client.core.delete_node(node, body=body, propagation_policy="foreground")
        except ApiException as e:
            errors.append((node, e))
            continue
        success.append(node)
    return (success, errors)


def is_node_ready(node: V1Node) -> bool:
    for condition in node.status.conditions:
        if condition.type == "Ready":
            return condition.status == "True"
    log.error(
        f"no KubeletReady condition found for node {node.metadata.name}. Conditions {node.status.conditions}"
    )
    return True


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    dry_run = args.dry_run

    kube_client = KubeClient()
    all_nodes = get_all_nodes(kube_client)
    log.debug(f"found nodes in cluster {[node.metadata.name for node in all_nodes]}")

    # we depend on iam credentials existing on the host for this to run.
    # anywhere else, and you'll need to set credentials using environment variables
    # we also make the assumption that all nodes are in the same region here
    region = all_nodes[0].metadata.labels["failure-domain.beta.kubernetes.io/region"]
    ec2_client = boto3.client("ec2", region)

    filtered_nodes = nodes_for_cleanup(ec2_client, all_nodes)
    if logging.DEBUG >= logging.root.level:
        log.debug(
            f"nodes to be deleted: {[node.metadata.name for node in filtered_nodes]}"
        )

    if not dry_run:
        success, errors = terminate_nodes(
            kube_client, [node.metadata.name for node in filtered_nodes]
        )
    else:
        success, errors = [], []
        log.info("dry run mode detected: not deleting nodes")

    for node_name in success:
        log.info(f"successfully deleted node {node_name}")

    for node_name, exception in errors:
        log.error(f"error deleting node: {node_name}: {exception}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
