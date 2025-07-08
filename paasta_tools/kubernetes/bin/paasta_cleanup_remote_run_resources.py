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
import argparse
import logging
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Sequence
from typing import Tuple

from paasta_tools.kubernetes.remote_run import get_max_job_duration_limit
from paasta_tools.kubernetes.remote_run import get_remote_run_jobs
from paasta_tools.kubernetes.remote_run import get_remote_run_role_bindings
from paasta_tools.kubernetes.remote_run import get_remote_run_roles
from paasta_tools.kubernetes.remote_run import get_remote_run_service_accounts
from paasta_tools.kubernetes_tools import get_all_managed_namespaces
from paasta_tools.kubernetes_tools import KubeClient


ListingFuncType = Callable[[KubeClient, str], Sequence[Any]]
DeletionFuncType = Callable[[str, str], Any]
log = logging.getLogger(__name__)


def clean_namespace(
    kube_client: KubeClient,
    namespace: str,
    auth_age_limit: datetime,
    job_age_limit: datetime,
    dry_run: bool = False,
):
    """Clean ephemeral remote-run resource in a namespace

    :param KubeClient kube_client: kubernetes client
    :param str namepsace: kubernetes namespace
    :param datetime auth_age_limit: expiration time for authentication resources
    :param datetime job_age_limit: expiration time for job resources
    :param bool dry_run: delete resources for real or not
    """
    dry_run_msg = " (dry_run)" if dry_run else ""
    cleanup_actions: Sequence[Tuple[DeletionFuncType, ListingFuncType, datetime]] = (
        (
            kube_client.core.delete_namespaced_service_account,
            get_remote_run_service_accounts,
            auth_age_limit,
        ),
        (
            kube_client.rbac.delete_namespaced_role,
            get_remote_run_roles,
            auth_age_limit,
        ),
        (
            kube_client.rbac.delete_namespaced_role_binding,
            get_remote_run_role_bindings,
            auth_age_limit,
        ),
        (
            kube_client.batches.delete_namespaced_job,
            get_remote_run_jobs,
            job_age_limit,
        ),
    )
    for delete_func, list_func, age_limit in cleanup_actions:
        for entity in list_func(kube_client, namespace):
            if (
                not entity.metadata.name.startswith("remote-run-")
                or entity.metadata.creation_timestamp > age_limit
            ):
                continue
            log.info(f"Deleting {entity.metadata.name} in {namespace}{dry_run_msg}")
            if not dry_run:
                delete_func(entity.metadata.name, namespace)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean ephemeral Kubernetes resources created by remote-run invocations",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=600,
        help="Maximum age, in seconds, resources are allowed to have",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        default=False,
        help="Do not actually delete resources",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="More verbose logging",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    kube_client = KubeClient()
    now = datetime.now(tz=timezone.utc)
    age_limit = now - timedelta(seconds=args.max_age)
    job_age_limit = now - timedelta(seconds=get_max_job_duration_limit())
    for namespace in get_all_managed_namespaces(kube_client):
        clean_namespace(
            kube_client,
            namespace,
            auth_age_limit=age_limit,
            job_age_limit=job_age_limit,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
