import logging
from typing import Any
from typing import Sequence

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


def list_namespaced_deployments(
    kube_client: KubeClient, namespace: str, **kwargs
) -> Sequence[DeploymentWrapper]:
    return [
        DeploymentWrapper(deployment)
        for deployment in kube_client.deployments.list_namespaced_deployment(
            namespace, **kwargs
        ).items
        if is_valid_application(deployment)
    ]


def list_namespaced_stateful_sets(
    kube_client: KubeClient, namespace: str, **kwargs
) -> Sequence[StatefulSetWrapper]:
    return [
        StatefulSetWrapper(deployment)
        for deployment in kube_client.deployments.list_namespaced_stateful_set(
            namespace, **kwargs
        ).items
        if is_valid_application(deployment)
    ]


def list_namespaced_applications(
    kube_client: KubeClient, namespace: str, application_types: Sequence[Any]
) -> Sequence[Application]:
    """
    List all applications in the namespace of the types from application_types.
    Only applications with complete set of labels are included (See is_valid_application()).
    :param kube_client:
    :param namespace:
    :param application_types:  types of applications
    :return:
    """
    apps = []  # type: ignore
    for application_type in application_types:
        if application_type == V1Deployment:
            apps.extend(list_namespaced_deployments(kube_client, namespace))
        elif application_type == V1StatefulSet:
            apps.extend(list_namespaced_stateful_sets(kube_client, namespace))
    return apps
