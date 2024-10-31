from time import sleep

from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.utils import DEFAULT_SOA_DIR


def remote_run_start(service, instance, user, cluster):
    kube_client = KubeClient()
    is_eks = False
    deployment = load_kubernetes_service_config_no_cache(
        service, instance, cluster, DEFAULT_SOA_DIR
    )
    namespace = deployment.get_namespace()

    formatted_application = deployment.format_kubernetes_app()
    formatted_application.metadata.name += f"-remote-run-{user}"
    pod_name = formatted_application.metadata.name
    app_wrapper = get_application_wrapper(formatted_application)
    app_wrapper.load_local_config(DEFAULT_SOA_DIR, cluster, is_eks)
    app_wrapper.create(kube_client)

    # Get pod status and name
    for retry in range(5):
        pod_list = kube_client.core.list_namespaced_pod(namespace)
        matching_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(pod_name):
                matching_pod = pod
                break

        if not matching_pod:
            sleep(1)
            continue

        if pod.status.phase == "Running":
            break
        elif pod.status.phase != "Initializing":
            raise Exception(f"Pod state is {pod.status.phase}")

    if not matching_pod:
        raise Exception("No matching pod")

    return {"Status": "Success!", "pod_name": pod.metadata.name, "namespace": namespace}


def remote_run_stop():
    pass
