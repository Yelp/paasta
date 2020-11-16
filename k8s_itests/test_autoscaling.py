import os

from k8s_itests.utils import cmd
from k8s_itests.utils import init_all

terminate_on_exit = []


def setup_module(module):
    init_all()


class TestSetupKubernetesJobs:
    def test_autoscaling(self):
        cmd("kubectl get hpa -n paasta", False)

    def test_paasta_status(self):
        instance = "autoscaling"
        service = "compute-infra-test-service"
        cmd(
            f"python -m paasta_tools.cli.cli status  -c {os.environ['KIND_CLUSTER']} -s {service} -i {instance} -v",
            False,
        )
