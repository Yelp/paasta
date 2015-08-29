import collections
import socket
from paasta_tools.smartstack_tools import get_multiple_backends


def get_replication_for_services(synapse_host, synapse_port, service_names):
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host: The hose that this check should contact for replication information.
    :param synapse_port: The port number that this check should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instance_counts: A dictionary mapping the service names
                                  to an integer number of available
                                  replicas
    :returns None: If it cannot connect to the specified synapse host and port
    """
    backends = get_multiple_backends(
        services=service_names,
        synapse_host=synapse_host,
        synapse_port=synapse_port,
    )

    counter = collections.Counter([b['pxname'] for b in backends if str(b['status']).startswith('UP')])
    return dict((sn, counter[sn]) for sn in service_names)


def ip_port_hostname_from_svname(svname):
    """This parses the haproxy svname that smartstack creates, which is in the form ip:port_hostname.

    :param svname: A string in the format ip:port_hostname
    :returns ip_port_hostname: A tuple of ip, port, hostname.
    """
    ip, port_hostname = svname.split(':', 1)
    port, hostname = port_hostname.split('_', 1)
    return ip, int(port), hostname


def get_registered_marathon_tasks(
    synapse_host,
    synapse_port,
    service_name,
    marathon_tasks,
):
    """Returns the marathon tasks that are registered in haproxy under a given service_name (nerve_ns).

    :param synapse_host: The host that this check should contact for replication information.
    :param synapse_port: The port that this check should contact for replication information.
    :param service_names: A list of strings that are the service names that should be checked for replication.
    :param marathon_tasks: A list of MarathonTask objects, whose tasks we will check for in the HAProxy status.
    """
    backends = get_multiple_backends([service_name], synapse_host=synapse_host, synapse_port=synapse_port)
    healthy_tasks = []
    for backend, task in match_backends_and_tasks(backends, marathon_tasks):
        if backend is not None and task is not None and backend['status'].startswith('UP'):
            healthy_tasks.append(task)
    return healthy_tasks


def match_backends_and_tasks(backends, tasks):
    """Returns tuples of matching (backend, task) pairs, as matched by IP and port. Each backend will be listed exactly
    once, and each task will be listed once per port. If a backend does not match with a task, (backend, None) will
    be included. If a task's port does not match with any backends, (None, task) will be included.

    :param backends: An iterable of haproxy backend dictionaries, e.g. the list returned by
                     smartstack_tools.get_multiple_backends.
    :param tasks: An iterable of MarathonTask objects.
    """
    backends_by_ip_port = collections.defaultdict(list)  # { (ip, port) : [backend1, backend2], ... }
    backend_task_pairs = []

    for backend in backends:
        ip, port, _ = ip_port_hostname_from_svname(backend['svname'])
        backends_by_ip_port[ip, port].append(backend)

    for task in tasks:
        ip = socket.gethostbyname(task.host)
        for port in task.ports:
            for backend in backends_by_ip_port.pop((ip, port), [None]):
                backend_task_pairs.append((backend, task))

    # we've been popping in the above loop, so anything left didn't match a marathon task.
    for backends in backends_by_ip_port.values():
        for backend in backends:
            backend_task_pairs.append((backend, None))

    return backend_task_pairs
