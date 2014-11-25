import os
from service_deployment_tools.paasta_cli.utils import \
    is_file_in_dir, x_mark, check_mark


def deploy_yaml_exists():
    """
    Check whether deploy.yaml exists in service directory
    """
    if is_file_in_dir('deploy.yaml', os.getcwd()):
        print "%s Found deploy.yaml file" % check_mark()
    else:
        print "%s Missing deploy.yaml file" % x_mark()
