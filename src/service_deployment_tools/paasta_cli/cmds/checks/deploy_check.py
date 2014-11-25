import os
from service_deployment_tools.paasta_cli.utils import is_file_in_dir


def deploy_yaml_exists():
    """
    Return true if deploy.yaml is present in the service directory, else false
    """
    return is_file_in_dir('deploy.yaml', os.getcwd())
