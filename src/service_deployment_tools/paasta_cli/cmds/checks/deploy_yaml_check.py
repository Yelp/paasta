"""
Every file in this directory must implement 'check', 'success', 'fail'
"""

from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


def check():
    """
    Return true if deploy.yaml is present
    """
    # TODO: write logic
    return False


def success():
    print "%s Found deploy.yaml" % check_mark()


def fail():
    print "%s Missing deploy.yaml file" % x_mark()
