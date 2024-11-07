import json
from time import sleep

from kubernetes.client.exceptions import ApiException

from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import create_temp_exec_token
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.utils import DEFAULT_SOA_DIR


def create_exec_token(service, instance, user, cluster):
    """Creates a short lived token for execing into a pod"""
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")
    # Load the service deployment settings
    deployment = load_kubernetes_service_config_no_cache(
        service, instance, cluster, DEFAULT_SOA_DIR
    )
    namespace = deployment.get_namespace()
    try:
        token = create_temp_exec_token(kube_client, namespace, user)
    except ApiException as E:
        raise
    return token.status.token


def remote_run_start(service, instance, user, cluster, interactive, recreate):
    # TODO Overriding the kube client config for now as the api has limited permissions
    kube_client = KubeClient(config_file="/etc/kubernetes/admin.conf")

    # TODO hardcoded for now
    is_eks = False

    # Load the service deployment settings
    deployment = load_kubernetes_service_config_no_cache(
        service, instance, cluster, DEFAULT_SOA_DIR
    )
    namespace = deployment.get_namespace()

    # Set to interactive mode
    if interactive:
        deployment.config_dict["cmd"] = "sleep 604800"  # One week

    # Create the app with a new name
    formatted_job = deployment.format_as_kubernetes_job()
    formatted_job.metadata.name = f"remote-run-{user}-{formatted_job.metadata.name}"
    job_name = formatted_job.metadata.name
    app_wrapper = get_application_wrapper(formatted_job)
    app_wrapper.load_local_config(DEFAULT_SOA_DIR, cluster, is_eks)

    # Launch pod
    try:
        app_wrapper.create(kube_client)
    except ApiException as e:
        if e.status == 409:
            # Job already running
            return json.dumps({"status": "409", "reason": "Job already running"})
        raise

    pod = wait_until_pod_running(kube_client, namespace, job_name)

    return json.dumps(
        {"status": "200", "pod_name": pod.metadata.name, "namespace": namespace}
    )


def wait_until_deployment_gone(kube_client, namespace, job_name):
    for retry in range(10):
        pod = find_pod(kube_client, namespace, job_name, 1)
        if not pod:
            return
        sleep(5)
    raise Exception("Pod still exists!")


def find_pod(kube_client, namespace, job_name, retries=5):
    # Get pod status and name
    for retry in range(retries):
        pod_list = kube_client.core.list_namespaced_pod(namespace)
        matching_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(job_name):
                matching_pod = pod
                break

        if not matching_pod:
            sleep(2)
            continue
        return matching_pod
    return None


def wait_until_pod_running(kube_client, namespace, job_name):
    for retry in range(5):
        pod = find_pod(kube_client, namespace, job_name)
        if not pod:
            raise Exception("No matching pod!")
        if pod.status.phase == "Running":
            break
        elif pod.status.phase not in ("Initializing", "Pending"):
            raise Exception(f"Pod state is {pod.status.phase}")
    return pod


# def remote_run_stop():
# TODO Should this happen here or should the client kill the deployment directly?
# Load the service deployment settings
# deployment = load_kubernetes_service_config_no_cache(
#    service, instance, cluster, DEFAULT_SOA_DIR
# )
