#!/usr/bin/env python
# Copyright 2019-2020 Yelp Inc.
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
Usage: ./cleanup_kubernetes_jobs.py [options]

Clean up kubernetes apps that aren't supposed to run on this cluster by deleting them.

Gets the current app list from kubernetes, and then a 'valid_app_list'
via utils.get_services_for_cluster

If an app in the kubernetes app list isn't in the valid_app_list, it's
deleted.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
- -t <KILL_THRESHOLD>, --kill-threshold: The decimal fraction of apps we think
    is sane to kill when this job runs
- -f, --force: Force the killing of apps if we breach the threshold
- -c, --cluster: Specifies the paasta cluster to check
- --eks: This flag cleans up only k8 services that shouldn't be running on EKS leaving instances from eks-*.yaml files
"""
import argparse
import logging
import sys
import traceback
from contextlib import contextmanager
from typing import Dict
from typing import Generator
from typing import List
from typing import Set
from typing import Tuple
from typing import Union

from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from pysensu_yelp import Status

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.eks_tools import load_eks_service_config
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper
from paasta_tools.kubernetes.application.controller_wrappers import StatefulSetWrapper
from paasta_tools.kubernetes.application.tools import Application
from paasta_tools.kubernetes.application.tools import list_all_applications
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import load_kubernetes_service_config
from paasta_tools.monitoring_tools import send_event
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)
APPLICATION_TYPES = [V1StatefulSet, V1Deployment]


class DontKillEverythingError(Exception):
    pass


class StatefulSetsAreNotSupportedError(Exception):
    pass


@contextmanager
def alert_state_change(application: Application, cluster: str) -> Generator:
    service = application.kube_deployment.service
    instance = application.kube_deployment.instance
    try:
        yield
        log_line = (
            "Deleted stale Kubernetes apps that looks lost: %s"
            % application.item.metadata.name
        )
        _log(
            service=service,
            component="deploy",
            level="event",
            cluster=cluster,
            instance=instance,
            line=log_line,
        )

    except Exception:
        loglines = ["Exception raised during cleanup of service %s:" % application]
        loglines.extend(traceback.format_exc().rstrip().split("\n"))
        for logline in loglines:
            _log(
                service=service,
                component="deploy",
                level="debug",
                cluster=cluster,
                instance=instance,
                line=logline,
            )
        raise


def instance_is_not_bouncing(
    instance_config: Union[KubernetesDeploymentConfig, EksDeploymentConfig],
    applications: List[Application],
) -> bool:
    """

    :param instance_config: a KubernetesDeploymentConfig or an EksDeploymentConfig with the configuration of the instance
    :param applications: a list of all deployments or stateful sets on the cluster that match the service
     and instance of provided instance_config
    """
    for application in applications:
        if isinstance(application, DeploymentWrapper):
            existing_app = application.item
            if (
                (
                    existing_app.metadata.namespace != instance_config.get_namespace()
                    and (instance_config.get_bounce_method() == "downthenup")
                )
                or (
                    existing_app.metadata.namespace == instance_config.get_namespace()
                    and (
                        instance_config.get_instances()
                        <= (existing_app.status.ready_replicas or 0)
                    )
                )
            ) or instance_config.get_desired_state() == "stop":
                return True

        elif (
            isinstance(application, StatefulSetWrapper)
            and application.item.metadata.namespace != instance_config.get_namespace()
        ):
            log.critical(
                "Paasta detected a StatefulSet that was migrated to a new namespace"
                "StatefulSet bouncing across namespaces is not supported"
            )
            raise StatefulSetsAreNotSupportedError
    return False


def get_applications_to_kill(
    applications_dict: Dict[Tuple[str, str], List[Application]],
    cluster: str,
    valid_services: Set[Tuple[str, str]],
    soa_dir: str,
    eks: bool = False,
) -> List[Application]:
    """

    :param applications_dict: A dictionary with (service, instance) as keys and a list of applications for each tuple
    :param cluster: paasta cluster
    :param valid_services: a set with the valid (service, instance) tuples for this cluster
    :param soa_dir: The SOA config directory to read from
    :return: list of applications to kill
    """
    log.info("Determining apps to be killed")

    applications_to_kill: List[Application] = []
    for (service, instance), applications in applications_dict.items():
        if len(applications) >= 1:
            if (service, instance) not in valid_services:
                applications_to_kill.extend(applications)
            else:
                instance_config: Union[KubernetesDeploymentConfig, EksDeploymentConfig]
                if eks:
                    instance_config = load_eks_service_config(
                        cluster=cluster,
                        service=service,
                        instance=instance,
                        soa_dir=soa_dir,
                    )
                else:
                    instance_config = load_kubernetes_service_config(
                        cluster=cluster,
                        service=service,
                        instance=instance,
                        soa_dir=soa_dir,
                    )
                try:
                    not_bouncing = instance_is_not_bouncing(
                        instance_config, applications
                    )
                except StatefulSetsAreNotSupportedError:
                    overrides = {
                        "page": True,
                        "alert_after": 0,
                        "tip": f"Revert {service}.{instance} in soa-configs to not include the namespace key.",
                        "runbook": "y/rb-paasta-namespace",
                        "ticket": True,
                    }
                    send_event(
                        service=service,
                        check_name=f"statefulset_bounce_{service}.{instance}",
                        overrides=overrides,
                        status=Status.CRITICAL,  # type: ignore
                        output=f"Unsupported bounce: {service}.{instance}. PaaSTA managed StatefulSets do not support custom namespace",
                        soa_dir=soa_dir,
                    )
                else:
                    for application in applications:
                        if (
                            application.kube_deployment.namespace
                            != instance_config.get_namespace()
                            and not_bouncing
                        ):
                            applications_to_kill.append(application)
    return applications_to_kill


def cleanup_unused_apps(
    soa_dir: str,
    cluster: str,
    kill_threshold: float = 0.5,
    force: bool = False,
    eks: bool = False,
) -> None:
    """Clean up old or invalid jobs/apps from kubernetes. Retrieves
    both a list of apps currently in kubernetes and a list of valid
    app ids in order to determine what to kill.

    :param soa_dir: The SOA config directory to read from
    :param cluster: paasta cluster to clean
    :param kill_threshold: The decimal fraction of apps we think is
        sane to kill when this job runs.
    :param force: Force the cleanup if we are above the kill_threshold"""
    log.info("Creating KubeClient")
    kube_client = KubeClient()

    log.info("Loading running Kubernetes apps")
    applications_dict = list_all_applications(kube_client, APPLICATION_TYPES)
    log.info("Retrieving valid apps from yelpsoa_configs")
    valid_services = set(
        get_services_for_cluster(
            instance_type="eks" if eks else "kubernetes", soa_dir=soa_dir
        )
    )

    applications_to_kill: List[Application] = get_applications_to_kill(
        applications_dict, cluster, valid_services, soa_dir, eks
    )

    log.debug("Running apps: %s" % list(applications_dict))
    log.debug("Valid apps: %s" % valid_services)
    log.debug("Terminating: %s" % applications_to_kill)
    if applications_to_kill:
        above_kill_threshold = float(len(applications_to_kill)) / float(
            len(applications_dict)
        ) > float(kill_threshold)
        if above_kill_threshold and not force:
            log.critical(
                "Paasta was about to kill more than %s of the running services, this "
                "is probably a BAD mistake!, run again with --force if you "
                "really need to destroy everything" % kill_threshold
            )
            raise DontKillEverythingError

    for applicaton in applications_to_kill:
        with alert_state_change(applicaton, cluster):
            applicaton.deep_delete(kube_client)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Cleans up stale kubernetes jobs.")
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
        default=load_system_paasta_config().get_cluster(),
        help="paasta cluster",
    )
    parser.add_argument(
        "-t",
        "--kill-threshold",
        dest="kill_threshold",
        default=0.5,
        help="The decimal fraction of apps we think is "
        "sane to kill when this job runs",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        dest="force",
        default=False,
        help="Force the cleanup if we are above the " "kill_threshold",
    )
    parser.add_argument(
        "--eks",
        help="This flag cleans up only k8 services that shouldn't be running on EKS leaving instances from eks-*.yaml files",
        dest="eks",
        action="store_true",
        default=False,
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    soa_dir = args.soa_dir
    kill_threshold = args.kill_threshold
    force = args.force
    cluster = args.cluster
    eks = args.eks
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    try:
        cleanup_unused_apps(
            soa_dir,
            cluster=cluster,
            kill_threshold=kill_threshold,
            force=force,
            eks=eks,
        )
    except DontKillEverythingError:
        sys.exit(1)


if __name__ == "__main__":
    main()
