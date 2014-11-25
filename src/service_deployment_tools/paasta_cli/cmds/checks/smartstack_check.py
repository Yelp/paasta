import os
from service_deployment_tools.paasta_cli.utils import \
    is_file_in_dir, x_mark, check_mark


def smartstack_yaml_exists():
    """
    Check whether smartstack.yaml exists in service directory
    """
    if is_file_in_dir('smartstack.yaml', os.getcwd()):
        print "%s Found smartstack.yaml file" % check_mark()
    else:
        print "%s Missing smartstack.yaml file" % x_mark()
