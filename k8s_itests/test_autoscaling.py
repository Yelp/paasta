import os

from k8s_itests.tools.cmds import cmd

# import pytest


# @pytest.fixture(scope='session', autouse=True)
def test_cluster_info():
    print(os.environ)
    service_instances = cmd(
        f'python -m paasta_tools.list_kubernetes_service_instances -d {os.environ["SOA_DIR"]}',
        capture_output=True,
    )
    cmd(
        f'python -m paasta_tools.setup_kubernetes_job {service_instances.stdout.strip()} -v -d {os.environ["SOA_DIR"]}',
        False,
    )
