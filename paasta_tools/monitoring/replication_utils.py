from paasta_tools.smartstack_tools import retrieve_haproxy_csv


def get_replication_for_services(synapse_host_port, service_names):
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instances: A dictionary mapping the service names
                                  to an integer number of available
                                  replicas
    :returns None: If it cannot connect to the specified synapse_host_port
    """
    reader = retrieve_haproxy_csv(synapse_host_port)

    available_instances = dict([(service_name, 0) for
                                service_name in service_names])
    for line in reader:
        # clean up two irregularities of the CSV output, relative to
        # DictReader's behavior there's a leading "# " for no good reason:
        line['pxname'] = line.pop('# pxname')
        # and there's a trailing comma on every line:
        line.pop('')

        # Look for the service in question and ignore the fictional
        # FRONTEND/BACKEND hosts, use starts_with so that hosts that are UP
        # with 1/X healthchecks to go before going down get counted as UP:
        slave, service = line['svname'], line['pxname']
        if (service in service_names and
                slave not in ('FRONTEND', 'BACKEND') and
                str(line['status']).startswith('UP')):
            available_instances[service] += 1

    return available_instances
