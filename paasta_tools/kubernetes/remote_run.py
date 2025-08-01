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
from typing import List
from typing import Optional
from typing import Sequence
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

from paasta_tools.adhoc_tools import load_adhoc_job_config
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.eks_tools import load_eks_service_config
from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import get_all_service_accounts
from paasta_tools.kubernetes_tools import JOB_TYPE_LABEL_NAME
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError

logger = logging.getLogger(__name__)
REMOTE_RUN_JOB_LABEL = "remote-run"
POD_OWNER_LABEL = paasta_prefixed("pod_owner")
TOOLBOX_MOCK_SERVICE = "prod-toolbox"
DEFAULT_MAX_DURATION_LIMIT = 8 * 60 * 60  # 8 hours


class RemoteRunError(Exception):
    pass


class RemoteRunOutcome(TypedDict, total=False):
    status: int
    message: str
    job_name: str
    pod_name: str
    pod_address: str
    namespace: str


def format_remote_run_job_name(
    job_name: str,
    user: str,
) -> str:
    """Format name for remote run job

    :param V1Job job: job definition
    :param str user: the user requesting the remote-run
    :return: job name
    """
    return limit_size_with_hash(f"remote-run-{user}-{job_name}")


def load_eks_or_adhoc_deployment_config(
    service: str,
    instance: str,
    cluster: str,
    is_toolbox: bool = False,
    user: Optional[str] = None,
) -> EksDeploymentConfig:
    assert user or not is_toolbox, "User required for toolbox deployment"
    try:
        deployment_config = (
            generate_toolbox_deployment(service, cluster, user)
            if is_toolbox
            else load_eks_service_config(service, instance, cluster)
        )
    except NoConfigurationForServiceError:
        # Perhaps they are trying to use an adhoc instance
        deployment_config = load_adhoc_job_config(service, instance, cluster)
        deployment_config = EksDeploymentConfig(
            service,
            cluster,
            instance,
            config_dict=deployment_config.config_dict,
            branch_dict=deployment_config.branch_dict,
        )
        deployment_config.config_filename_prefix = "adhoc"
    return deployment_config


def remote_run_start(
    service: str,
    instance: str,
    cluster: str,
    user: str,
    interactive: bool,
    recreate: bool,
    max_duration: int,
    is_toolbox: bool,
    command: Optional[str] = None,
) -> RemoteRunOutcome:
    """Trigger remote-run job

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str user: the user requesting the remote-run sandbox
    :param bool interactive: whether it is expected to access the remote-run job interactively
    :param bool recreate: whether to recreate remote-run job if existing
    :param int max_duration: maximum allowed duration for the remote-ruh job
    :param bool is_toolbox: requested job is for a toolbox container
    :param str command: command override to execute in the job container
    :return: outcome of the operation, and resulting Kubernetes pod information
    """
    kube_client = KubeClient()

    # Load the service deployment settings
    deployment_config = load_eks_or_adhoc_deployment_config(
        service, instance, cluster, is_toolbox, user
    )

    # Set override command, or sleep for interactive mode
    if command and not is_toolbox:
        deployment_config.config_dict["cmd"] = command
    elif interactive and not is_toolbox:
        deployment_config.config_dict["cmd"] = f"sleep {max_duration}"

    # Create the app with a new name
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL,
        deadline_seconds=max_duration,
        keep_routable_ip=is_toolbox,
    )
    job_name = format_remote_run_job_name(formatted_job.metadata.name, user)
    formatted_job.metadata.name = job_name
    app_wrapper = get_application_wrapper(formatted_job)
    app_wrapper.soa_config = deployment_config
    app_wrapper.ensure_service_account(kube_client)

    # Launch pod
    logger.info(f"Starting {job_name}")
    try:
        app_wrapper.create(kube_client)
    except ApiException as e:
        if e.status != 409:
            raise
        if recreate:
            remote_run_stop(
                service=service,
                instance=instance,
                cluster=cluster,
                user=user,
                is_toolbox=is_toolbox,
            )
            return remote_run_start(
                service=service,
                instance=instance,
                cluster=cluster,
                user=user,
                interactive=interactive,
                recreate=False,
                max_duration=max_duration,
                is_toolbox=is_toolbox,
            )

    return {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": job_name,
    }


def remote_run_ready(
    service: str,
    instance: str,
    cluster: str,
    job_name: str,
    user: str,
    is_toolbox: bool,
) -> RemoteRunOutcome:
    """Check if remote-run pod is ready

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str job_name: name of the remote-run job to check
    :param bool is_toolbox: requested job is for a toolbox container
    :return: job status, with pod info
    """
    kube_client = KubeClient()

    # Load the service deployment settings
    deployment_config = load_eks_or_adhoc_deployment_config(
        service, instance, cluster, is_toolbox, user
    )
    namespace = deployment_config.get_namespace()

    pod = find_job_pod(kube_client, namespace, job_name)
    if not pod:
        return {"status": 404, "message": "No pod found"}
    if pod.status.phase == "Running":
        if pod.metadata.deletion_timestamp:
            return {"status": 409, "message": "Pod is terminating"}
        result: RemoteRunOutcome = {
            "status": 200,
            "message": "Pod ready",
            "pod_name": pod.metadata.name,
            "namespace": namespace,
        }
        if is_toolbox:
            result["pod_address"] = pod.status.pod_ip
        return result
    return {
        "status": 204,
        "message": "Pod not ready",
    }


def remote_run_stop(
    service: str,
    instance: str,
    cluster: str,
    user: str,
    is_toolbox: bool,
) -> RemoteRunOutcome:
    """Stop remote-run job

    :param str service: service name
    :param str instance: service instance
    :param str cluster: paasta cluster
    :param str user: the user requesting the remote-run sandbox
    :param bool is_toolbox: requested job is for a toolbox container
    :return: outcome of the operation
    """
    kube_client = KubeClient()

    # Load the service deployment settings
    deployment_config = load_eks_or_adhoc_deployment_config(
        service, instance, cluster, is_toolbox, user
    )

    # Rebuild the job metadata
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL
    )
    job_name = format_remote_run_job_name(formatted_job.metadata.name, user)
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
    kube_client = KubeClient()

    # Load the service deployment settings
    deployment_config = load_eks_or_adhoc_deployment_config(service, instance, cluster)
    namespace = deployment_config.get_namespace()

    # Rebuild the job metadata
    formatted_job = deployment_config.format_kubernetes_job(
        job_label=REMOTE_RUN_JOB_LABEL
    )
    job_name = format_remote_run_job_name(formatted_job.metadata.name, user)

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
    bind_role_to_service_account(kube_client, namespace, service_account, role, user)
    return create_temp_exec_token(kube_client, namespace, service_account)


def generate_toolbox_deployment(
    service: str, cluster: str, user: str
) -> EksDeploymentConfig:
    """Creates virtual EKS deployment for toolbox containers starting from adhoc configuration

    :param str service: toolbox name
    :param str cluster: target deployment cluster
    :param str user: user requesting the toolbox
    :return: deployment configuration
    """
    if not user.isalnum():
        raise RemoteRunError(
            f"Provided username contains non-alphanumeric characters: {user}"
        )
    # NOTE: API authorization is enforced by service, and we want different rules
    # for each toolbox, so clients send a combined service-instance string, and then
    # we split it here to load the correct instance settings.
    adhoc_instance = service[len(TOOLBOX_MOCK_SERVICE) + 1 :]
    adhoc_deployment = load_adhoc_job_config(
        TOOLBOX_MOCK_SERVICE,
        adhoc_instance,
        cluster,
        load_deployments=False,
    )
    # NOTE: we're explicitly dynamically mounting a single user's public keys
    # as we want these pods to only be usable by said user.
    adhoc_deployment.config_dict.setdefault("extra_volumes", []).append(
        {
            "containerPath": f"/etc/authorized_keys.d/{user}.pub",
            "hostPath": f"/etc/authorized_keys.d/{user}.pub",
            "mode": "RO",
        },
    )
    adhoc_deployment.config_dict.setdefault("env", {})["SANDBOX_USER"] = user
    adhoc_deployment.config_dict["routable_ip"] = True
    return EksDeploymentConfig(
        service=service,
        cluster=cluster,
        instance="main",
        config_dict=adhoc_deployment.config_dict,
        branch_dict=adhoc_deployment.branch_dict,
    )


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
        f"job-name={job_name}",
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


def get_remote_run_service_accounts(
    kube_client: KubeClient, namespace: str, user: str = ""
) -> Sequence[V1ServiceAccount]:
    """List all temporary service account related to remote-run

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: pod namespace
    :param str user: optionally filter by owning user
    :return: list of service accounts
    """
    return get_all_service_accounts(
        kube_client,
        namespace=namespace,
        label_selector=(f"{POD_OWNER_LABEL}={user}" if user else POD_OWNER_LABEL),
    )


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
    service_accounts = get_remote_run_service_accounts(kube_client, namespace, user)
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
        resources=["pods", "pods/exec", "pods/log"],
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
    try:
        kube_client.rbac.create_namespaced_role(namespace=namespace, body=role)
    except ApiException as e:
        if e.status != 409:
            raise
    return role_name


def bind_role_to_service_account(
    kube_client: KubeClient,
    namespace: str,
    service_account: str,
    role: str,
    user: str,
) -> None:
    """Bind service account to role

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: service account namespace
    :param str service_account: service account name
    :param str role: role name
    :param str user: user requiring the role
    """
    role_binding = V1RoleBinding(
        metadata=V1ObjectMeta(
            name=limit_size_with_hash(f"remote-run-binding-{role}"),
            namespace=namespace,
            labels={POD_OWNER_LABEL: user},
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
    try:
        kube_client.rbac.create_namespaced_role_binding(
            namespace=namespace,
            body=role_binding,
        )
    except ApiException as e:
        if e.status != 409:
            raise


def get_remote_run_roles(kube_client: KubeClient, namespace: str) -> List[V1Role]:
    """List all temporary roles related to remote-run

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: role namespace
    :return: list of roles
    """
    return kube_client.rbac.list_namespaced_role(
        namespace,
        label_selector=POD_OWNER_LABEL,
    ).items


def get_remote_run_role_bindings(
    kube_client: KubeClient, namespace: str
) -> List[V1RoleBinding]:
    """List all temporary role bindings related to remote-run

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: role namespace
    :return: list of roles
    """
    return kube_client.rbac.list_namespaced_role_binding(
        namespace,
        label_selector=POD_OWNER_LABEL,
    ).items


def get_remote_run_jobs(kube_client: KubeClient, namespace: str) -> List[V1Job]:
    """List all remote-run jobs

    :param KubeClient kube_client: Kubernetes client
    :param str namespace: job namespace
    """
    return kube_client.batches.list_namespaced_job(
        namespace,
        label_selector=f"{paasta_prefixed(JOB_TYPE_LABEL_NAME)}={REMOTE_RUN_JOB_LABEL}",
    ).items


def get_max_job_duration_limit() -> int:
    """Get maximum configured duration for a remote run job

    :return: max duration in seconds
    """
    system_config = load_system_paasta_config()
    return system_config.get_remote_run_duration_limit(DEFAULT_MAX_DURATION_LIMIT)
