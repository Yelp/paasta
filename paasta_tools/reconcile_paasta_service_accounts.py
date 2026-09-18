#!/usr/bin/env python
# Copyright 2015-2024 Yelp Inc.
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
Usage: ./reconcile_paasta_service_accounts.py [options]

Reconcile PaaSTA ServiceAccounts for IAM roles on the local cluster.

For each IAM role whose trust policy grants AssumeRoleWithWebIdentity via this
cluster's OIDC provider, ensures a ServiceAccount exists in each shared namespace
referenced by the trust policy's Deny/StringNotLike conditions.

Only manages SAs in shared namespaces (configured via SystemPaastaConfig).
paastasvc-* namespace SAs are handled by ensure_service_account() at deploy time.

SA naming mirrors paasta_tools.kubernetes_tools.get_service_account_name.

Credentials come from the environment (instance profile on system nodes,
or AWS_PROFILE when run by a human).

Command line options:

- -c <CLUSTER>, --cluster <CLUSTER>: Kubernetes cluster name
- -n <NAMESPACE>, --namespace <NAMESPACE>: Limit to specific namespaces (repeatable)
- --allow-unlisted: Allow --namespace values not in paasta_sa_namespaces config
- --dry-run: Log intended changes without applying them
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from dataclasses import dataclass
from enum import Enum
from enum import auto
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from urllib.parse import urlparse

import boto3
from kubernetes.client import V1ServiceAccount
from kubernetes.client.exceptions import ApiException

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import ensure_service_account
from paasta_tools.kubernetes_tools import get_all_service_accounts
from paasta_tools.kubernetes_tools import get_service_account_name
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)

MANAGED_LABEL = "paasta.yelp.com/managed"
ROLE_ARN_ANNOTATION = "eks.amazonaws.com/role-arn"


class DriftKind(Enum):
    MISSING = auto()
    WRONG_ARN = auto()
    UNMANAGED = auto()
    EXTRA = auto()
    OK = auto()


@dataclass
class DesiredSA:
    namespace: str
    sa_name: str
    role_arn: str


@dataclass
class DriftItem:
    namespace: str
    sa_name: str
    kind: DriftKind
    desired_arn: Optional[str] = None
    actual_arn: Optional[str] = None


@dataclass
class ReconcileStats:
    created: int = 0
    updated: int = 0
    deleted: int = 0
    errors: int = 0


def get_oidc_provider_arn(
    account_id: str,
    cluster_server_url: str,
    aws_region: str,
    cluster_name: str,
    ecosystem: str,
) -> str:
    host = urlparse(cluster_server_url).hostname or ""
    if ".eks.amazonaws.com" in host:
        oidc_id = host.split(".")[0].upper()
        return f"arn:aws:iam::{account_id}:oidc-provider/oidc.eks.{aws_region}.amazonaws.com/id/{oidc_id}"
    return (
        f"arn:aws:iam::{account_id}:oidc-provider/"
        f"s3.{aws_region}.amazonaws.com/aws-k8s-pod-identity-oidc-{ecosystem}-{aws_region}/{cluster_name}"
    )


def parse_namespaces_from_trust_policy(
    trust_policy: Dict,
    allowed_namespaces: Set[str],
) -> Set[str]:
    namespaces: Set[str] = set()
    for statement in trust_policy.get("Statement", []):
        if statement.get("Effect") != "Deny":
            continue
        condition = statement.get("Condition", {})
        string_not_like = condition.get("StringNotLike", {})
        for key, patterns in string_not_like.items():
            if not key.endswith(":sub"):
                continue
            if isinstance(patterns, str):
                patterns = [patterns]
            for pattern in patterns:
                # Expected: "system:serviceaccount:<namespace>:<sa-name>"
                # Wildcard: "*:*:*:*"
                parts = pattern.split(":")
                if len(parts) < 4:
                    continue
                _, _, namespace, *_ = parts
                namespace = namespace.strip("*")
                if namespace == "":
                    namespaces.update(allowed_namespaces)
                elif namespace in allowed_namespaces:
                    namespaces.add(namespace)
    return namespaces


def collect_desired_sas(
    oidc_provider_arn: str,
    allowed_namespaces: Set[str],
) -> List[DesiredSA]:
    session = boto3.Session()
    iam = session.client("iam")
    desired: List[DesiredSA] = []

    paginator = iam.get_paginator("list_roles")
    for page in paginator.paginate():
        for role in page["Roles"]:
            role_arn = role["Arn"]
            trust_policy = role["AssumeRolePolicyDocument"]

            references_our_cluster = False
            for statement in trust_policy.get("Statement", []):
                if statement.get("Effect") != "Allow":
                    continue
                actions = statement.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                if "sts:AssumeRoleWithWebIdentity" not in actions:
                    continue
                federated = statement.get("Principal", {}).get("Federated", [])
                if isinstance(federated, str):
                    federated = [federated]
                if oidc_provider_arn in federated:
                    references_our_cluster = True
                    break

            if not references_our_cluster:
                continue

            namespaces = parse_namespaces_from_trust_policy(
                trust_policy,
                allowed_namespaces,
            )
            if not namespaces:
                continue

            sa_name = get_service_account_name(role_arn)
            for namespace in namespaces:
                desired.append(
                    DesiredSA(
                        namespace=namespace,
                        sa_name=sa_name,
                        role_arn=role_arn,
                    )
                )

    return desired


def compute_drift(
    kube_client: KubeClient,
    desired: List[DesiredSA],
    allowed_namespaces: Set[str],
) -> Tuple[List[DriftItem], List[str]]:
    errors: List[str] = []
    drift: List[DriftItem] = []

    try:
        existing_namespaces = {
            ns.metadata.name for ns in kube_client.core.list_namespace().items
        }
    except Exception:
        log.exception("Failed to list namespaces")
        errors.append("Failed to list namespaces")
        return drift, errors

    desired_by_ns: Dict[str, Dict[str, str]] = {}
    for d in desired:
        desired_by_ns.setdefault(d.namespace, {})[d.sa_name] = d.role_arn

    inspected_sas_by_ns: Dict[str, Dict[str, V1ServiceAccount]] = {}

    for namespace in allowed_namespaces:
        sa_map = desired_by_ns.get(namespace, {})

        if namespace not in existing_namespaces:
            for sa_name, role_arn in sa_map.items():
                drift.append(
                    DriftItem(
                        namespace=namespace,
                        sa_name=sa_name,
                        kind=DriftKind.MISSING,
                        desired_arn=role_arn,
                    )
                )
            continue

        try:
            all_sas: Dict[str, V1ServiceAccount] = {
                sa.metadata.name: sa
                for sa in get_all_service_accounts(kube_client, namespace)
            }
            inspected_sas_by_ns[namespace] = all_sas
        except ApiException:
            log.exception(f"Failed to list SAs in {namespace}")
            errors.append(f"Failed to list SAs in {namespace}")
            continue

        for sa_name, role_arn in sa_map.items():
            if sa_name not in all_sas:
                drift.append(
                    DriftItem(
                        namespace=namespace,
                        sa_name=sa_name,
                        kind=DriftKind.MISSING,
                        desired_arn=role_arn,
                    )
                )
            else:
                existing = all_sas[sa_name]
                existing_arn = (existing.metadata.annotations or {}).get(
                    ROLE_ARN_ANNOTATION
                )
                is_managed = (existing.metadata.labels or {}).get(
                    MANAGED_LABEL
                ) == "true"

                if existing_arn != role_arn:
                    drift.append(
                        DriftItem(
                            namespace=namespace,
                            sa_name=sa_name,
                            kind=DriftKind.WRONG_ARN,
                            desired_arn=role_arn,
                            actual_arn=existing_arn,
                        )
                    )
                elif not is_managed:
                    drift.append(
                        DriftItem(
                            namespace=namespace,
                            sa_name=sa_name,
                            kind=DriftKind.UNMANAGED,
                            desired_arn=role_arn,
                            actual_arn=existing_arn,
                        )
                    )
                else:
                    drift.append(
                        DriftItem(
                            namespace=namespace,
                            sa_name=sa_name,
                            kind=DriftKind.OK,
                            desired_arn=role_arn,
                            actual_arn=existing_arn,
                        )
                    )

    all_desired_names = {(d.namespace, d.sa_name) for d in desired}
    for namespace, all_sas in inspected_sas_by_ns.items():
        for sa_name, sa in all_sas.items():
            if (sa.metadata.labels or {}).get(MANAGED_LABEL) == "true":
                if (namespace, sa_name) not in all_desired_names:
                    drift.append(
                        DriftItem(
                            namespace=namespace,
                            sa_name=sa_name,
                            kind=DriftKind.EXTRA,
                            actual_arn=(sa.metadata.annotations or {}).get(
                                ROLE_ARN_ANNOTATION
                            ),
                        )
                    )

    return drift, errors


def apply_drift(
    kube_client: KubeClient,
    drift: List[DriftItem],
    dry_run: bool,
) -> ReconcileStats:
    stats = ReconcileStats()

    for item in drift:
        ns, sa_name = item.namespace, item.sa_name

        if item.kind in (DriftKind.MISSING, DriftKind.WRONG_ARN, DriftKind.UNMANAGED):
            if item.kind == DriftKind.MISSING:
                log.info(f"{ns}: create SA {sa_name} -> {item.desired_arn}")
            elif item.kind == DriftKind.UNMANAGED:
                log.info(f"{ns}: adopt SA {sa_name} (adding managed label)")
            else:
                log.info(
                    f"{ns}: update SA {sa_name} {item.actual_arn!r} -> {item.desired_arn!r}"
                )
            if not dry_run:
                try:
                    ensure_service_account(
                        iam_role=item.desired_arn,
                        namespace=ns,
                        kube_client=kube_client,
                        managed=True,
                    )
                    if item.kind == DriftKind.MISSING:
                        stats.created += 1
                    else:
                        stats.updated += 1
                except Exception:
                    log.exception(f"Failed to ensure SA {sa_name} in {ns}")
                    stats.errors += 1
            else:
                if item.kind == DriftKind.MISSING:
                    stats.created += 1
                else:
                    stats.updated += 1

        elif item.kind == DriftKind.EXTRA:
            log.info(f"{ns}: delete SA {sa_name}")
            if not dry_run:
                try:
                    kube_client.core.delete_namespaced_service_account(
                        name=sa_name,
                        namespace=ns,
                    )
                    stats.deleted += 1
                except ApiException:
                    log.exception(f"Failed to delete SA {sa_name} in {ns}")
                    stats.errors += 1
            else:
                stats.deleted += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile PaaSTA ServiceAccounts for IAM roles.",
    )
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        metavar="CLUSTER",
        default=None,
        help="Kubernetes cluster name",
    )
    parser.add_argument(
        "-n",
        "--namespace",
        action="append",
        dest="namespaces",
        metavar="NAMESPACE",
        help="Limit to these namespaces (may be repeated; default: all shared namespaces)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        default=False,
        help="Log intended changes without applying them",
    )

    parser.add_argument(
        "--allow-unlisted",
        action="store_true",
        dest="allow_unlisted",
        default=False,
        help="Allow --namespace values not in paasta_sa_namespaces config",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    system_paasta_config = load_system_paasta_config()
    cluster = args.cluster or system_paasta_config.get_cluster()

    configured_namespaces = (
        system_paasta_config.get_reconciled_iam_service_account_namespaces()
    )
    if args.namespaces:
        unlisted = set(args.namespaces) - set(configured_namespaces)
        if unlisted and not args.allow_unlisted:
            log.warning(
                f"Namespaces not in reconciled_iam_service_account_namespaces config "
                f"(pass --allow-unlisted to override): {sorted(unlisted)}"
            )
            allowed_namespaces = set(args.namespaces) - unlisted
        else:
            allowed_namespaces = set(args.namespaces)
    else:
        allowed_namespaces = set(configured_namespaces)

    if not allowed_namespaces:
        log.info("No namespaces to reconcile")
        sys.exit(0)

    cluster_info = system_paasta_config.get_kube_clusters().get(cluster, {})
    aws_region = cluster_info.get("aws_region")
    if not aws_region:
        raise RuntimeError(f"Unable to determine AWS region for cluster: {cluster}")
    server = cluster_info.get("server")
    if not server:
        raise RuntimeError(
            f"Missing server for cluster {cluster} in kube_clusters config"
        )
    ecosystem = system_paasta_config.get_ecosystem_for_cluster(cluster) or ""

    session = boto3.Session()
    account_id = session.client("sts", region_name=aws_region).get_caller_identity()[
        "Account"
    ]

    oidc_provider_arn = get_oidc_provider_arn(
        account_id=account_id,
        cluster_server_url=server,
        aws_region=aws_region,
        cluster_name=cluster,
        ecosystem=ecosystem,
    )
    log.debug(f"Cluster: {cluster}, OIDC provider: {oidc_provider_arn}")
    log.info(f"Namespaces: {sorted(allowed_namespaces)}")

    if args.dry_run:
        log.info("DRY RUN - no changes will be applied")

    log.info("Collecting desired SAs from IAM trust policies...")
    desired = collect_desired_sas(oidc_provider_arn, allowed_namespaces)
    log.info(f"Found {len(desired)} desired SA entries")

    kube_client = KubeClient()

    drift, errors = compute_drift(kube_client, desired, allowed_namespaces)

    actionable = [i for i in drift if i.kind != DriftKind.OK]
    if actionable:
        stats = apply_drift(kube_client, actionable, args.dry_run)
        log.info(
            f"Results: created={stats.created} updated={stats.updated} "
            f"deleted={stats.deleted} errors={stats.errors}"
        )
        success = stats.errors == 0 and not errors
    else:
        log.info("No drift detected, nothing to do")
        success = not errors

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
