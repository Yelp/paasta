# Copyright 2015-2016 Yelp Inc.
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
import hashlib
import logging
from time import sleep
from typing import Optional
from typing import TypedDict

from kubernetes.client import AuthenticationV1TokenRequest
from kubernetes.client import V1Job
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PolicyRule
from kubernetes.client import V1Role
from kubernetes.client import V1RoleBinding
from kubernetes.client import V1RoleRef
from kubernetes.client import V1ServiceAccount
from kubernetes.client import V1Subject
from kubernetes.client import V1TokenRequestSpec
from kubernetes.client.exceptions import ApiException

from paasta_tools.eks_tools import load_eks_service_config
from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import get_all_service_accounts
from paasta_tools.kubernetes_tools import JOB_TYPE_LABEL_NAME
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import paasta_prefixed


logger = logging.getLogger(__name__)
REMOTE_RUN_JOB_LABEL = "remote-run"
POD_OWNER_LABEL = paasta_prefixed("pod_owner")


class RemoteRunError(Exception):
    pass


class RemoteRunOutcome(TypedDict, total=False):
    status: int
    message: str
    job_name: str
    pod_name: str
    namespace: str


def _format_remote_run_job_name(
    job: V1Job,
    user: str,
) -> str:
    """Format name for remote run job

    :param V1Job job: job definition
    :param str user: the user requesting the remote-run
    :return: job name
    """
    return limit_size_with_hash(f"remote-run-{user}-{job.metadata.name}")


def remote_run_start(
    service: str,
    instance: str,
    cluster: str,
    user: str,
    interactive: bool,
    recreate: bool,
    max_duration: int,
) -> RemoteRunOutcome:
    """Trigger remote-run job

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str user: the user requesting the remote-run sandbox
    :param bool interactive: whether it is expected to access the remote-run job interactively
    :param bool recreate: whether to recreate remote-run job if existing
    :param int max_duration: maximum allowed duration for the remote-ruh job
    :return: outcome of the operation, and resulting Kubernetes pod information
    """
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")

    # Load the service deployment settings
    deployment_config = load_eks_service_config(service, instance, cluster)

    # Set to interactive mode
    if interactive:
        deployment_config.config_dict["cmd"] = f"sleep {max_duration}"

    # Create the app with a new name
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL,
        deadline_seconds=max_duration,
    )
    job_name = _format_remote_run_job_name(formatted_job, user)
    formatted_job.metadata.name = job_name
    app_wrapper = get_application_wrapper(formatted_job)
    app_wrapper.soa_config = deployment_config

    # Launch pod
    logger.info(f"Starting {job_name}")
    try:
        app_wrapper.create(kube_client)
    except ApiException as e:
        if e.status != 409:
            raise
        if recreate:
            remote_run_stop(service, instance, cluster, user)
            return remote_run_start(
                service=service,
                instance=instance,
                cluster=cluster,
                user=user,
                interactive=interactive,
                recreate=False,
                max_duration=max_duration,
            )

    return {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": job_name,
    }


def remote_run_ready(
    service: str, instance: str, cluster: str, job_name: str
) -> RemoteRunOutcome:
    """Check if remote-run pod is ready

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str job_name: name of the remote-run job to check
    :return: job status, with pod info
    """
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")

    # Load the service deployment settings
    deployment_config = load_eks_service_config(service, instance, cluster)
    namespace = deployment_config.get_namespace()

    pod = find_job_pod(kube_client, namespace, job_name)
    if not pod:
        return {"status": 404, "message": "No pod found"}
    if pod.status.phase == "Running":
        return {
            "status": 200,
            "message": "Pod ready",
            "pod_name": pod.metadata.name,
            "namespace": namespace,
        }
    return {
        "status": 204,
        "message": "Pod not ready",
    }


def remote_run_stop(
    service: str, instance: str, cluster: str, user: str
) -> RemoteRunOutcome:
    """Stop remote-run job

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str user: the user requesting the remote-run sandbox
    :return: outcome of the operation
    """
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")

    # Load the service deployment settings
    deployment_config = load_eks_service_config(service, instance, cluster)

    # Rebuild the job metadata
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL
    )
    job_name = _format_remote_run_job_name(formatted_job, user)
    formatted_job.metadata.name = job_name

    # Stop the job
    logger.info(f"Stopping {job_name}")
    app_wrapper = get_application_wrapper(formatted_job)
    app_wrapper.soa_config = deployment_config
    app_wrapper.deep_delete(kube_client)

    return {"status": 200, "message": "Job successfully removed"}


def remote_run_token(
    service: str,
    instance: str,
    cluster: str,
    user: str,
) -> str:
    """Creates a short lived token for execing into a pod

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str user: the user requesting the remote-run sandbox
    """
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")

    # Load the service deployment settings
    deployment_config = load_eks_service_config(service, instance, cluster)
    namespace = deployment_config.get_namespace()

    # Rebuild the job metadata
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL
    )
    job_name = _format_remote_run_job_name(formatted_job, user)

    # Find pod and create exec token for it
    pod = find_job_pod(kube_client, namespace, job_name)
    if not pod:
        raise RemoteRunError(f"Pod for {job_name} not found")
    pod_name = pod.metadata.name
    logger.info(f"Generating temporary service account token for {pod_name}")
    service_account = create_remote_run_service_account(
        kube_client, namespace, pod_name, user
    )
    role = create_pod_scoped_role(kube_client, namespace, pod_name, user)
    bind_role_to_service_account(kube_client, namespace, service_account, role)
    return create_temp_exec_token(kube_client, namespace, service_account)


def find_job_pod(
    kube_client: KubeClient,
    namespace: str,
    job_name: str,
    job_label: str = REMOTE_RUN_JOB_LABEL,
    retries: int = 3,
) -> Optional[V1Pod]:
    """Locate pod for remote-run job

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: the pod namespace
    :param str job_name: remote-run job name
    :param int retries: maximum number of attemps
    :return: pod object if found
    """
    selectors = (
        f"{paasta_prefixed(JOB_TYPE_LABEL_NAME)}={job_label}",
        f"batch.kubernetes.io/job-name={job_name}",
    )
    for _ in range(retries):
        pod_list = kube_client.core.list_namespaced_pod(
            namespace,
            label_selector=",".join(selectors),
        )
        if pod_list.items:
            return pod_list.items[0]
        sleep(0.5)
    return None


def create_temp_exec_token(
    kube_client: KubeClient,
    namespace: str,
    service_account: str,
) -> str:
    """Create a short lived token for service account

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: service account namespace
    :param str service_account: service account name
    :return: token value
    """
    token_spec = V1TokenRequestSpec(
        expiration_seconds=600,  # minimum allowed by k8s
        audiences=[],
    )
    request = AuthenticationV1TokenRequest(spec=token_spec)
    response = kube_client.core.create_namespaced_service_account_token(
        service_account, namespace, request
    )
    return response.status.token


def create_remote_run_service_account(
    kube_client: KubeClient,
    namespace: str,
    pod_name: str,
    user: str,
) -> str:
    """Create service account to exec into remote-run pod

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: pod namespace
    :param str pod_name: pod name
    :param str user: user requiring credentials
    """
    pod_name_hash = hashlib.sha1(pod_name.encode("utf-8")).hexdigest()[:12]
    service_account_name = limit_size_with_hash(f"remote-run-{user}-{pod_name_hash}")
    service_accounts = get_all_service_accounts(
        kube_client,
        namespace=namespace,
        label_selector=f"{POD_OWNER_LABEL}={user}",
    )
    if any(item.metadata.name == service_account_name for item in service_accounts):
        return service_account_name
    service_account = V1ServiceAccount(
        metadata=V1ObjectMeta(
            name=service_account_name,
            namespace=namespace,
            labels={POD_OWNER_LABEL: user},
        )
    )
    kube_client.core.create_namespaced_service_account(
        namespace=namespace, body=service_account
    )
    return service_account_name


def create_pod_scoped_role(
    kube_client: KubeClient,
    namespace: str,
    pod_name: str,
    user: str,
) -> str:
    """Create role with execution access to specific pod

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: pod namespace
    :param str pod_name: pod name
    :param str user: user requiring the role
    :return: name of the role
    """
    pod_name_hash = hashlib.sha1(pod_name.encode("utf-8")).hexdigest()[:12]
    role_name = f"remote-run-role-{pod_name_hash}"
    policy = V1PolicyRule(
        verbs=["create", "get"],
        resources=["pods", "pods/exec"],
        resource_names=[pod_name],
        api_groups=[""],
    )
    role = V1Role(
        rules=[policy],
        metadata=V1ObjectMeta(
            name=role_name,
            labels={POD_OWNER_LABEL: user},
        ),
    )
    kube_client.core.create_namespaced_role(namespace=namespace, body=role)
    return role_name


def bind_role_to_service_account(
    kube_client: KubeClient,
    namespace: str,
    service_account: str,
    role: str,
) -> None:
    """Bind service account to role

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: service account namespace
    :param str service_account: service account name
    :param str role: role name
    """
    role_binding = V1RoleBinding(
        metadata=V1ObjectMeta(
            name=limit_size_with_hash(f"binding-{role}"),
            namespace=namespace,
        ),
        role_ref=V1RoleRef(
            api_group="rbac.authorization.k8s.io",
            kind="Role",
            name=role,
        ),
        subjects=[
            V1Subject(
                kind="ServiceAccount",
                name=service_account,
            ),
        ],
    )
    kube_client.rbac.create_namespaced_role_binding(
        namespace=namespace,
        body=role_binding,
    )
