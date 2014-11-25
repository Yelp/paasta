"""
Every file in this directory must implement 'check', 'success', 'fail'
"""
from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


def check():
    """
    Return true if service is using sensu monitoring.yaml
    """
    # TODO: write logic
    return True


def success():
    print "%s Is using sensu monitoring.yaml" % check_mark()


def fail():
    print "%s Not utilizing sensu monitoring" % x_mark()
