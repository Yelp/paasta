import os
from service_deployment_tools.paasta_cli.utils import \
    is_file_in_dir, x_mark, check_mark


def monitoring_yaml_exists():
    """
    Check whether monitoring.yaml exists in service directory
    """
    if is_file_in_dir('monitoring.yaml', os.getcwd()):
        print "%s Found monitoring.yaml file" % check_mark()
    else:
        print "%s Missing monitoring.yaml file" % x_mark()
