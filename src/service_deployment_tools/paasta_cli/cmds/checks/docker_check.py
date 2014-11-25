import os
from service_deployment_tools.paasta_cli.utils import \
    is_file_in_dir, x_mark, check_mark


def dockerfile_exists():
    """
    Check whether Dockerfile exists in service directory
    """
    if is_file_in_dir('Dockerfile', os.getcwd()):
        print "%s Found Dockerfile file" % check_mark()
    else:
        print "%s Missing Dockerfile file" % x_mark()
