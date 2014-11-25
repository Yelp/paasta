"""
Every file in this directory must implement 'check', 'success', 'fail'
"""
from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


def check():
    """
    Return true if dockerfile is present
    """
    # TODO: write logic
    return False


def success():
    print "%s Dockerfile present" % check_mark()


def fail():
    print "%s No dockerfile present" % x_mark()
