from paasta_tools.contrib.get_running_task_allocation import get_kubernetes_resource_request
from kubernetes.client import V1ResourceRequirements

def test_get_kubernetes_resource_request():
    test_resource_req = V1ResourceRequirements(
                limits={
                    "cpu": "1.3",
                    "memory": "2048Mi",
                    "ephemeral-storage": "4096Mi"
                },
                requests={
                    "cpu": "0.3",
                    "memory": "2048Mi",
                    "ephemeral-storage": "4096Mi",
                },
            )

    assert get_kubernetes_resource_request(test_resource_req) == {'cpus': 0.3, 'cpus_limit': 1.3, 'disk': 4096.0, 'mem': 2048.0}