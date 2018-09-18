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
from typing import Union

from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import ensure_paasta_namespace
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import max_unavailable
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import SPACER

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument(
        'service_instance_list', nargs='+',
        help="The list of marathon service instances to create or update",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    # system_paasta_config = load_system_paasta_config()
    kube_client = KubeClient()

    ensure_paasta_namespace(kube_client)
    setup_kube_succeeded = setup_kube_deployments(
        kube_client=kube_client,
        service_instances=args.service_instance_list,
        soa_dir=soa_dir,
    )
    sys.exit(0 if setup_kube_succeeded else 1)


def setup_kube_deployments(
    kube_client: KubeClient,
    service_instances: Sequence[str],
    soa_dir: str=DEFAULT_SOA_DIR,
) -> bool:
    succeeded = True
    if service_instances:
        deployments = list_all_deployments(kube_client)
    for service_instance in service_instances:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error("Invalid service instance specified. Format is service%sinstance." % SPACER)
            succeeded = False
        else:
            if reconcile_kubernetes_deployment(
                kube_client=kube_client,
                service=service,
                instance=instance,
                kube_deployments=deployments,
                soa_dir=soa_dir,
            )[0]:
                succeeded = False
    return succeeded


def reconcile_kubernetes_deployment(
    kube_client: KubeClient,
    service: str,
    instance: str,
    kube_deployments: Sequence[KubeDeployment],
    soa_dir: str,
) -> Tuple[int, Optional[int]]:
    try:
        service_instance_config = load_kubernetes_service_config_no_cache(
            service,
            instance,
            load_system_paasta_config().get_cluster(),
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        log.debug("No deployments found for %s.%s in cluster %s. Skipping." %
                  (service, instance, load_system_paasta_config().get_cluster()))
        return 0, None
    except NoConfigurationForServiceError:
        error_msg = "Could not read kubernetes configuration file for %s.%s in cluster %s" % \
                    (service, instance, load_system_paasta_config().get_cluster())
        log.error(error_msg)
        return 1, None

    try:
        formatted_application = service_instance_config.format_kubernetes_app()
    except InvalidKubernetesConfig as e:
        log.error(str(e))
        return (1, None)

    desired_deployment = KubeDeployment(
        service=service,
        instance=instance,
        git_sha=formatted_application.metadata.labels["git_sha"],
        config_sha=formatted_application.metadata.labels["config_sha"],
        replicas=formatted_application.spec.replicas,
    )

    if not (service, instance) in [(kd.service, kd.instance) for kd in kube_deployments]:
        log.debug(f"{desired_deployment} does not exist so creating")
        create_kubernetes_application(
            kube_client=kube_client,
            application=formatted_application,
        )
    elif desired_deployment not in kube_deployments:
        log.debug(f"{desired_deployment} exists but config_sha or git_sha doesn't match or number of instances changed")
        update_kubernetes_application(
            kube_client=kube_client,
            application=formatted_application,
        )
    else:
        log.debug(f"{desired_deployment} is up to date, no action taken")

    ensure_pod_disruption_budget(
        kube_client=kube_client,
        service=service,
        instance=instance,
        min_instances=service_instance_config.get_desired_instances() - max_unavailable(
            instance_count=service_instance_config.get_desired_instances(),
            bounce_margin_factor=service_instance_config.get_bounce_margin_factor(),
        ),
    )
    return 0, None


def ensure_pod_disruption_budget(
        kube_client: KubeClient,
        service: str,
        instance: str,
        min_instances: int,
) -> V1beta1PodDisruptionBudget:
    pdr = pod_disruption_budget_for_service_instance(
        service=service,
        instance=instance,
        min_instances=min_instances,
    )
    try:
        existing_pdr = kube_client.policy.read_namespaced_pod_disruption_budget(
            name=pdr.metadata.name,
            namespace=pdr.metadata.namespace,
        )
    except ApiException as e:
        if e.status == 404:
            existing_pdr = None
        else:
            raise

    if existing_pdr:
        if existing_pdr.spec.min_available != pdr.spec.min_available:
            # poddisruptionbudget objects are not mutable like most things in the kubernetes api,
            # so we have to do a delete/replace.
            # unfortunately we can't really do this transactionally, but I guess we'll just hope for the best?
            logging.debug(f'existing poddisruptionbudget {pdr.metadata.name} is out of date; deleting')
            kube_client.policy.delete_namespaced_pod_disruption_budget(
                name=pdr.metadata.name,
                namespace=pdr.metadata.namespace,
                body=V1DeleteOptions(),
            )
            logging.debug(f'creating poddisruptionbudget {pdr.metadata.name}')
            return create_pod_disruption_budget(
                kube_client=kube_client,
                pod_disruption_budget=pdr,
            )
        else:
            logging.debug(f'poddisruptionbudget {pdr.metadata.name} up to date')
    else:
        logging.debug(f'creating poddisruptionbudget {pdr.metadata.name}')
        return create_pod_disruption_budget(
            kube_client=kube_client,
            pod_disruption_budget=pdr,
        )


def create_kubernetes_application(kube_client: KubeClient, application: Union[V1Deployment, V1StatefulSet]) -> None:
    if isinstance(application, V1Deployment):
        create_deployment(
            kube_client=kube_client,
            formatted_deployment=application,
        )
    elif isinstance(application, V1StatefulSet):
        create_stateful_set(
            kube_client=kube_client,
            formatted_stateful_set=application,
        )
    else:
        raise Exception("Unknown kubernetes object to create")


def update_kubernetes_application(kube_client: KubeClient, application: Union[V1Deployment, V1StatefulSet]) -> None:
    if isinstance(application, V1Deployment):
        update_deployment(
            kube_client=kube_client,
            formatted_deployment=application,
        )
    elif isinstance(application, V1StatefulSet):
        update_stateful_set(
            kube_client=kube_client,
            formatted_stateful_set=application,
        )
    else:
        raise Exception("Unknown kubernetes object to update")


if __name__ == "__main__":
    main()
