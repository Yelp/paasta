import os
from service_deployment_tools.paasta_cli.utils import is_file_in_dir


def smartstack_yaml_exists():
    """
    Return true if service is using smartstack.yaml
    """
    return is_file_in_dir('smartstack.yaml', os.getcwd())
