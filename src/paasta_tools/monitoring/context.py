from paasta_tools.paasta_cli.utils import _run


def get_serviceinit_status(service, namespace):
    command = "paasta_serviceinit %s.%s status" % (service, namespace)
    return _run(command)[1]


def get_context(service, namespace):
    """Tries to get more context about why a service might not be OK.
    returns a string useful for inserting into monitoring email alerts."""
    status = get_serviceinit_status(service, namespace)
    context = "\nMore context from paasta status:\n%s" % status
    return context
