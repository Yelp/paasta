#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
Usage: ./setup_kubernetes_job.py <service.instance> [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from typing import Optional
from typing import Sequence
from typing import Tuple

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import SPACER

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates Kubernetes jobs.")
    parser.add_argument(
        "service_instance_list",
        nargs="+",
        help="The list of Kubernetes service instances to create or update",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-c", "--cluster", dest="cluster", help="paasta cluster",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False,
    )
    parser.add_argument(
        "-l",
        "--rate-limit",
        dest="rate_limit",
        default=0,
        metavar="LIMIT",
        type=int,
        help="Update or create up to this number of service instances. Default is 0 (no limit).",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        # filter out unwanted zookeeper messages in the log
        logging.getLogger("kazoo").setLevel(logging.WARN)
        logging.basicConfig(level=logging.INFO)

    # system_paasta_config = load_system_paasta_config()
    kube_client = KubeClient()

    ensure_namespace(kube_client, namespace="paasta")
    setup_kube_succeeded = setup_kube_deployments(
        kube_client=kube_client,
        service_instances=args.service_instance_list,
        soa_dir=soa_dir,
        cluster=args.cluster or load_system_paasta_config().get_cluster(),
        rate_limit=args.rate_limit,
    )
    sys.exit(0 if setup_kube_succeeded else 1)


def validate_job_name(service_instance: str) -> bool:
    try:
        service, instance, _, __ = decompose_job_id(service_instance)
    except InvalidJobNameError:
        log.error(
            "Invalid service instance specified. Format is service%sinstance." % SPACER
        )
        return False
    return True


def setup_kube_deployments(
    kube_client: KubeClient,
    service_instances: Sequence[str],
    cluster: str,
    rate_limit: int = 0,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> bool:
    if service_instances:
        existing_kube_deployments = set(list_all_deployments(kube_client))
        existing_apps = {
            (deployment.service, deployment.instance)
            for deployment in existing_kube_deployments
        }
    service_instances_with_valid_names = [
        decompose_job_id(service_instance)
        for service_instance in service_instances
        if validate_job_name(service_instance)
    ]
    applications = [
        create_application_object(
            kube_client=kube_client,
            service=service_instance[0],
            instance=service_instance[1],
            cluster=cluster,
            soa_dir=soa_dir,
        )
        for service_instance in service_instances_with_valid_names
    ]

    api_updates = 0
    for _, app in applications:
        if app:
            try:
                if (
                    app.kube_deployment.service,
                    app.kube_deployment.instance,
                ) not in existing_apps:
                    log.info(f"Creating {app} because it does not exist yet.")
                    app.create(kube_client)
                    api_updates += 1
                elif app.kube_deployment not in existing_kube_deployments:
                    log.info(f"Updating {app} because configs have changed.")
                    app.update(kube_client)
                    api_updates += 1
                else:
                    log.info(f"{app} is up-to-date!")

                log.info(f"Ensuring related API objects for {app} are in sync")
                app.update_related_api_objects(kube_client)
            except Exception:
                log.exception(f"Error while processing: {app}")
        if rate_limit > 0 and api_updates >= rate_limit:
            log.info(
                f"Not doing any further updates as we reached the limit ({api_updates})"
            )
            break

    return (False, None) not in applications and len(
        service_instances_with_valid_names
    ) == len(service_instances)


def create_application_object(
    kube_client: KubeClient, service: str, instance: str, cluster: str, soa_dir: str,
) -> Tuple[bool, Optional[Application]]:
    try:
        service_instance_config = load_kubernetes_service_config_no_cache(
            service, instance, cluster, soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        log.debug(
            "No deployments found for %s.%s in cluster %s. Skipping."
            % (service, instance, cluster)
        )
        return True, None
    except NoConfigurationForServiceError:
        error_msg = (
            f"Could not read kubernetes configuration file for %s.%s in cluster %s"
            % (service, instance, cluster)
        )
        log.error(error_msg)
        return False, None

    try:
        formatted_application = service_instance_config.format_kubernetes_app()
    except InvalidKubernetesConfig as e:
        log.error(str(e))
        return False, None

    app = get_application_wrapper(formatted_application)
    app.load_local_config(soa_dir, cluster)
    return True, app


if __name__ == "__main__":
    main()
