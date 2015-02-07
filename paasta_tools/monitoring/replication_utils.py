import socket
from paasta_tools.smartstack_tools import retrieve_haproxy_csv


def get_replication_for_services(synapse_host_port, service_names):
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instance_counts: A dictionary mapping the service names
                                  to an integer number of available
                                  replicas
    :returns None: If it cannot connect to the specified synapse_host_port
    """
    available_instances = get_all_registered_ip_ports_for_services(
        synapse_host_port,
        service_names
    )

    return dict([
        (service_name, len(instances))
        for (service_name, instances)
        in available_instances.iteritems()
    ])


def get_all_registered_ip_ports_for_services(synapse_host_port, service_names):
    """Returns the ips and ports of all registered instances for the
    provided services.

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instances: A dictionary mapping the service names
                                  to a list of (ip, port) tuples.
    :returns None: If it cannot connect to the specified synapse_host_port
    """
    reader = retrieve_haproxy_csv(synapse_host_port)

    available_instances = dict([(service_name, []) for
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
            ip, port_hostname = slave.split(':', 1)
            port, _ = port_hostname.split('_', 1)
            available_instances[service].append((ip, int(port)))

    return available_instances


def get_registered_marathon_tasks(
    synapse_host_port,
    service_name,
    marathon_app,
):
    """Returns the marathon tasks that are registered in haproxy under a given
    service_name (nerve_ns).

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.
    :param marathon_app: A MarathonApp object, whose tasks we will check for in
                         the HAProxy status.
    """
    haproxy_ip_ports = get_all_registered_ip_ports_for_services(
        synapse_host_port,
        service_name
    )

    healthy_tasks = []
    for task in marathon_app.tasks:
        ip = socket.gethostbyname(task.host)
        if any(
            ((ip, port) in haproxy_ip_ports)
            for port
            in task.service_ports
        ):
            healthy_tasks.append(task)
    return healthy_tasks
