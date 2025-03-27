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
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Sequence
from typing import Tuple

from paasta_tools.kubernetes.remote_run import get_remote_run_role_bindings
from paasta_tools.kubernetes.remote_run import get_remote_run_roles
from paasta_tools.kubernetes.remote_run import get_remote_run_service_accounts
from paasta_tools.kubernetes_tools import get_all_managed_namespaces
from paasta_tools.kubernetes_tools import KubeClient


ListingFuncType = Callable[[KubeClient, str], Sequence[Any]]
DeletionFuncType = Callable[[str, str], Any]


def clean_namespace(kube_client: KubeClient, namespace: str, age_limit: datetime):
    """Clean ephemeral remote-run resource in a namespace

    :param KubeClient kube_client: kubernetes client
    :param str namepsace: kubernetes namespace
    :param datetime age_limit: expiration time for resources
    """
    cleanup_actions: Sequence[Tuple[DeletionFuncType, ListingFuncType]] = (
        (
            kube_client.core.delete_namespaced_service_account,
            get_remote_run_service_accounts,
        ),
        (kube_client.rbac.delete_namespaced_role, get_remote_run_roles),
        (kube_client.rbac.delete_namespaced_role_binding, get_remote_run_role_bindings),
    )
    for delete_func, list_func in cleanup_actions:
        for entity in list_func(kube_client, namespace):
            if (
                not entity.metadata.name.startswith("remote-run-")
                or entity.metadata.creation_timestamp > age_limit
            ):
                continue
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
    return parser.parse_args()


def main():
    args = parse_args()
    kube_client = KubeClient()
    age_limit = datetime.now(tz=timezone.utc) - timedelta(seconds=args.max_age)
    for namespace in get_all_managed_namespaces(kube_client):
        clean_namespace(kube_client, namespace, age_limit)


if __name__ == "__main__":
    main()
