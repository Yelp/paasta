"""
Every file in this directory must implement 'check', 'success', 'fail'
"""
from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


def check():
    """
    Return the port number if service is running in smartstack, else False
    """
    # TODO: write logic
    return 80


def success():
    print "%s Is in smartstack, port %d" % (check_mark(), check())


def fail():
    print "%s Not in smartstack" % x_mark()
