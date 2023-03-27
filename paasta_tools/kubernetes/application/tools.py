import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Sequence
from typing import Tuple

from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper
from paasta_tools.kubernetes.application.controller_wrappers import StatefulSetWrapper
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import paasta_prefixed

log = logging.getLogger(__name__)


def is_valid_application(deployment: V1Deployment):
    is_valid = True
    missing = []
    for attr in ["service", "instance", "git_sha", "config_sha"]:
        prefixed_attr = paasta_prefixed(attr)
        if prefixed_attr not in deployment.metadata.labels:
            is_valid = False
            missing.append(prefixed_attr)
    if missing:
        log.warning(
            f"deployment/{deployment.metadata.name} in "
            f"namespace/{deployment.metadata.namespace} "
            f"is missing following labels: {missing}"
        )
    return is_valid


def list_paasta_managed_deployments(
    kube_client: KubeClient, **kwargs
) -> Dict[Tuple[str, str], List[Application]]:
    deployments: Dict[Tuple[str, str], List[Application]] = {}
    for deployment in kube_client.deployments.list_deployment_for_all_namespaces(
        label_selector=paasta_prefixed("managed"), **kwargs
    ).items:
        if is_valid_application(deployment):
            application = DeploymentWrapper(deployment)
            service = application.kube_deployment.service
            instance = application.kube_deployment.instance
            if deployments.get((service, instance), None):
                deployments[(service, instance)].append(application)
            else:
                deployments[(service, instance)] = [application]
    return deployments


def list_paasta_managed_stateful_sets(
    kube_client: KubeClient, **kwargs
) -> Dict[Tuple[str, str], List[Application]]:
    deployments: Dict[Tuple[str, str], List[Application]] = {}
    for deployment in kube_client.deployments.list_stateful_set_for_all_namespaces(
        label_selector=paasta_prefixed("managed"), **kwargs
    ).items:
        if is_valid_application(deployment):
            application = StatefulSetWrapper(deployment)
            service = application.kube_deployment.service
            instance = application.kube_deployment.instance
            if deployments.get((service, instance), None):
                deployments[(service, instance)].append(application)
            else:
                deployments[(service, instance)] = [application]
    return deployments


def list_all_applications(
    kube_client: KubeClient, application_types: Sequence[Any]
) -> Dict[Tuple[str, str], List[Application]]:
    """
    List all applications in the cluster of the types from application_types.
    Only applications with complete set of labels are included (See is_valid_application()).
    :param kube_client:

    :param application_types:  types of applications
    :return: A mapping from (service, instance) to application
    """
    apps: Dict[Tuple[str, str], List[Application]] = {}
    for application_type in application_types:
        if application_type == V1Deployment:
            apps = {**apps, **list_paasta_managed_deployments(kube_client)}
        elif application_type == V1StatefulSet:
            apps.update(list_paasta_managed_stateful_sets(kube_client))
    return apps
