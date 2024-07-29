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
import traceback
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.eks_tools import load_eks_service_config_no_cache
from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import list_all_paasta_deployments
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.metrics import metrics_lib
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
        "-c",
        "--cluster",
        dest="cluster",
        help="paasta cluster",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
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
    parser.add_argument(
        "--eks",
        help="This flag deploys only k8 services that should run on EKS",
        dest="eks",
        action="store_true",
        default=False,
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

    # emit deploy events for updated jobs
    deploy_metrics = metrics_lib.get_metrics_interface("paasta")

    # emit timing metrics for s_k_j
    cluster = args.cluster or load_system_paasta_config().get_cluster()
    timer = metrics_lib.system_timer(
        dimensions=dict(
            cluster=cluster,
            eks=args.eks,
        ),
    )
    timer.start()

    kube_client = KubeClient()
    service_instances_valid = True

    # validate the service_instance names
    service_instances_with_valid_names = get_service_instances_with_valid_names(
        service_instances=args.service_instance_list
    )

    # returns a list of pairs of (No error?, KubernetesDeploymentConfig | EksDeploymentConfig) for every service_instance
    service_instance_configs_list = get_kubernetes_deployment_config(
        service_instances_with_valid_names=service_instances_with_valid_names,
        cluster=cluster,
        soa_dir=soa_dir,
        eks=args.eks,
    )

    if ((False, None) in service_instance_configs_list) or (
        len(service_instances_with_valid_names) != len(args.service_instance_list)
    ):
        service_instances_valid = False

    if service_instance_configs_list:
        for _, service_instance_config in service_instance_configs_list:
            if service_instance_config:
                ensure_namespace(
                    kube_client, namespace=service_instance_config.get_namespace()
                )

        setup_kube_succeeded = setup_kube_deployments(
            kube_client=kube_client,
            cluster=args.cluster or load_system_paasta_config().get_cluster(),
            service_instance_configs_list=service_instance_configs_list,
            rate_limit=args.rate_limit,
            soa_dir=soa_dir,
            metrics_interface=deploy_metrics,
            eks=args.eks,
        )
    else:
        setup_kube_succeeded = False
    exit_code = 0 if setup_kube_succeeded and service_instances_valid else 1

    timer.stop(tmp_dimensions={"result": exit_code})
    logging.info(
        f"Stopping timer for {cluster} (eks={args.eks}) with result {exit_code}: {timer()}ms elapsed"
    )
    sys.exit(exit_code)


def get_service_instances_with_valid_names(
    service_instances: Sequence[str],
) -> List[Tuple[str, str, str, str]]:
    service_instances_with_valid_names = [
        decompose_job_id(service_instance)
        for service_instance in service_instances
        if validate_job_name(service_instance)
    ]
    return service_instances_with_valid_names


def validate_job_name(service_instance: str) -> bool:
    try:
        service, instance, _, __ = decompose_job_id(service_instance)
    except InvalidJobNameError:
        log.error(
            "Invalid service instance specified. Format is service%sinstance." % SPACER
        )
        return False
    return True


def get_kubernetes_deployment_config(
    service_instances_with_valid_names: list,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
    eks: bool = False,
) -> List[Tuple[bool, Union[KubernetesDeploymentConfig, EksDeploymentConfig]]]:
    service_instance_configs_list = []
    for service_instance in service_instances_with_valid_names:
        try:
            service_instance_config: Union[
                KubernetesDeploymentConfig, EksDeploymentConfig
            ]
            if eks:
                service_instance_config = load_eks_service_config_no_cache(
                    service=service_instance[0],
                    instance=service_instance[1],
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
            else:
                service_instance_config = load_kubernetes_service_config_no_cache(
                    service=service_instance[0],
                    instance=service_instance[1],
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
            service_instance_configs_list.append((True, service_instance_config))
        except NoDeploymentsAvailable:
            log.debug(
                "No deployments found for %s.%s in cluster %s. Skipping."
                % (service_instance[0], service_instance[1], cluster)
            )
            service_instance_configs_list.append((True, None))
        except NoConfigurationForServiceError:
            error_msg = (
                f"Could not read kubernetes configuration file for %s.%s in cluster %s"
                % (service_instance[0], service_instance[1], cluster)
            )
            log.error(error_msg)
            service_instance_configs_list.append((False, None))
    return service_instance_configs_list


def setup_kube_deployments(
    kube_client: KubeClient,
    cluster: str,
    service_instance_configs_list: List[
        Tuple[bool, Union[KubernetesDeploymentConfig, EksDeploymentConfig]]
    ],
    rate_limit: int = 0,
    soa_dir: str = DEFAULT_SOA_DIR,
    metrics_interface: metrics_lib.BaseMetrics = metrics_lib.NoMetrics("paasta"),
    eks: bool = False,
) -> bool:

    if not service_instance_configs_list:
        return True

    existing_kube_deployments = set(list_all_paasta_deployments(kube_client))
    existing_apps = {
        (deployment.service, deployment.instance, deployment.namespace)
        for deployment in existing_kube_deployments
    }

    applications = [
        create_application_object(
            cluster=cluster,
            soa_dir=soa_dir,
            service_instance_config=service_instance,
            eks=eks,
        )
        if service_instance
        else (_, None)
        for _, service_instance in service_instance_configs_list
    ]
    api_updates = 0
    for _, app in applications:
        if app:
            app_dimensions = {
                "paasta_service": app.kube_deployment.service,
                "paasta_instance": app.kube_deployment.instance,
                "paasta_cluster": cluster,
                "paasta_namespace": app.kube_deployment.namespace,
            }
            try:
                app.update_dependency_api_objects(kube_client)
                if (
                    app.kube_deployment.service,
                    app.kube_deployment.instance,
                    app.kube_deployment.namespace,
                ) not in existing_apps:
                    if app.soa_config.get_bounce_method() == "downthenup":
                        if any(
                            (
                                existing_app[:2]
                                == (
                                    app.kube_deployment.service,
                                    app.kube_deployment.instance,
                                )
                            )
                            for existing_app in existing_apps
                        ):
                            # For downthenup, we don't want to create until cleanup_kubernetes_job has cleaned up the instance in the other namespace.
                            continue
                    log.info(f"Creating {app} because it does not exist yet.")
                    app.create(kube_client)
                    app_dimensions["deploy_event"] = "create"
                    metrics_interface.emit_event(
                        name="deploy",
                        dimensions=app_dimensions,
                    )
                    api_updates += 1
                elif app.kube_deployment not in existing_kube_deployments:
                    log.info(f"Updating {app} because configs have changed.")
                    app.update(kube_client)
                    app_dimensions["deploy_event"] = "update"
                    metrics_interface.emit_event(
                        name="deploy",
                        dimensions=app_dimensions,
                    )
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
    return (False, None) not in applications


def create_application_object(
    cluster: str,
    soa_dir: str,
    service_instance_config: Union[KubernetesDeploymentConfig, EksDeploymentConfig],
    eks: bool = False,
) -> Tuple[bool, Optional[Application]]:
    try:
        formatted_application = service_instance_config.format_kubernetes_app()
    except InvalidKubernetesConfig:
        log.error(traceback.format_exc())
        return False, None

    app = get_application_wrapper(formatted_application)
    app.load_local_config(soa_dir, cluster, eks)
    return True, app


if __name__ == "__main__":
    main()
