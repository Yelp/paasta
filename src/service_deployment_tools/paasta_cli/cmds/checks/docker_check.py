import os
from service_deployment_tools.paasta_cli.utils import is_file_in_dir


def dockerfile_exists():
    """
    Return true if Dockerfile is present in the service directory, else false
    """
    return is_file_in_dir('Dockerfile', os.getcwd())
