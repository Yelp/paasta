import os
from service_deployment_tools.paasta_cli.utils import is_file_in_dir


def monitoring_yaml_exists():
    """
    Return true if service is using sensu monitoring.yaml
    """
    return is_file_in_dir('monitoring.yaml', os.getcwd())
