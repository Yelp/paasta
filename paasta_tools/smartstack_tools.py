# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import csv
import socket

import requests

from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_user_agent


def retrieve_haproxy_csv(synapse_host, synapse_port, synapse_haproxy_url_format):
    """Retrieves the haproxy csv from the haproxy web interface

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns reader: a csv.DictReader object
    """
    synapse_uri = synapse_haproxy_url_format.format(host=synapse_host, port=synapse_port)

    # timeout after 1 second and retry 3 times
    haproxy_request = requests.Session()
    haproxy_request.headers.update({'User-Agent': get_user_agent()})
    haproxy_request.mount(
        'http://',
        requests.adapters.HTTPAdapter(max_retries=3))
    haproxy_request.mount(
        'https://',
        requests.adapters.HTTPAdapter(max_retries=3))
    haproxy_response = haproxy_request.get(synapse_uri, timeout=1)
    haproxy_data = haproxy_response.text
    reader = csv.DictReader(haproxy_data.splitlines())
    return reader


def get_backends(service, synapse_host, synapse_port, synapse_haproxy_url_format):
    """Fetches the CSV from haproxy and returns a list of backends,
    regardless of their state.

    :param service: If None, return backends for all services, otherwise only return backends for this particular
                    service.
    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """
    if service:
        services = [service]
    else:
        services = None
    return get_multiple_backends(services, synapse_host=synapse_host, synapse_port=synapse_port,
                                 synapse_haproxy_url_format=synapse_haproxy_url_format)


def get_multiple_backends(services, synapse_host, synapse_port, synapse_haproxy_url_format):
    """Fetches the CSV from haproxy and returns a list of backends,
    regardless of their state.

    :param services: If None, return backends for all services, otherwise only return backends for these particular
                     services.
    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """

    reader = retrieve_haproxy_csv(synapse_host, synapse_port, synapse_haproxy_url_format=synapse_haproxy_url_format)
    backends = []

    for line in reader:
        # clean up two irregularities of the CSV output, relative to
        # DictReader's behavior there's a leading "# " for no good reason:
        line['pxname'] = line.pop('# pxname')
        # and there's a trailing comma on every line:
        line.pop('')

        # Look for the service in question and ignore the fictional
        # FRONTEND/BACKEND hosts, use starts_with so that hosts that are UP
        # with 1/X healthchecks to go before going down get counted as UP:
        ha_slave, ha_service = line['svname'], line['pxname']
        if (services is None or ha_service in services) and ha_slave not in ('FRONTEND', 'BACKEND'):
            backends.append(line)

    return backends


def load_smartstack_info_for_service(service, namespace, blacklist, system_paasta_config, soa_dir=DEFAULT_SOA_DIR):
    """Retrives number of available backends for given services

    :param service_instances: A list of tuples of (service, instance)
    :param namespaces: list of Smartstack namespaces
    :param blacklist: A list of blacklisted location tuples in the form (location, value)
    :param system_paasta_config: A SystemPaastaConfig object representing the system configuration.
    :returns: a dictionary of the form

    ::

        {
          'location_type': {
              'unique_location_name': {
                  'service.instance': <# ofavailable backends>
              },
              'other_unique_location_name': ...
          }
        }

    """
    service_namespace_config = marathon_tools.load_service_namespace_config(service, namespace,
                                                                            soa_dir=soa_dir)
    discover_location_type = service_namespace_config.get_discover()
    return get_smartstack_replication_for_attribute(
        attribute=discover_location_type,
        service=service,
        namespace=namespace,
        blacklist=blacklist,
        system_paasta_config=system_paasta_config,
    )


def get_smartstack_replication_for_attribute(attribute, service, namespace, blacklist, system_paasta_config):
    """Loads smartstack replication from a host with the specified attribute

    :param attribute: a Mesos attribute
    :param service: A service name, like 'example_service'
    :param namespace: A particular smartstack namespace to inspect, like 'main'
    :param constraints: A list of Marathon constraints to restrict which synapse hosts to query
    :param blacklist: A list of blacklisted location tuples in the form of (location, value)
    :param system_paasta_config: A SystemPaastaConfig object representing the system configuration.
    :returns: a dictionary of the form {'<unique_attribute_value>': <smartstack replication hash>}
              (the dictionary will contain keys for unique all attribute values)
    """
    replication_info = {}
    filtered_slaves = mesos_tools.get_all_slaves_for_blacklist_whitelist(
        blacklist=blacklist,
        whitelist=[],
    )
    if not filtered_slaves:
        raise mesos_tools.NoSlavesAvailableError

    attribute_slave_dict = mesos_tools.get_mesos_slaves_grouped_by_attribute(
        slaves=filtered_slaves,
        attribute=attribute
    )

    full_name = compose_job_id(service, namespace)

    for value, hosts in attribute_slave_dict.iteritems():
        # arbitrarily choose the first host with a given attribute to query for replication stats
        synapse_host = hosts[0]['hostname']
        repl_info = get_replication_for_services(
            synapse_host=synapse_host,
            synapse_port=system_paasta_config.get_synapse_port(),
            synapse_haproxy_url_format=system_paasta_config.get_synapse_haproxy_url_format(),
            services=[full_name],
        )
        replication_info[value] = repl_info

    return replication_info


def get_replication_for_services(synapse_host, synapse_port, synapse_haproxy_url_format, services):
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host: The hose that this check should contact for replication information.
    :param synapse_port: The port number that this check should contact for replication information.
    :param synapse_haproxy_url_format: The format of the synapse haproxy URL.
    :param services: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instance_counts: A dictionary mapping the service names
                                  to an integer number of available
                                  replicas
    :returns None: If it cannot connect to the specified synapse host and port
    """
    backends = get_multiple_backends(
        services=services,
        synapse_host=synapse_host,
        synapse_port=synapse_port,
        synapse_haproxy_url_format=synapse_haproxy_url_format,
    )

    counter = collections.Counter([b['pxname'] for b in backends if backend_is_up(b)])
    return dict((sn, counter[sn]) for sn in services)


def backend_is_up(backend):
    """Returns whether a server is receiving traffic in HAProxy.

    :param backend: backend dict, like one of those returned by smartstack_tools.get_multiple_backends.

    :returns is_up: Whether the backend is in a state that receives traffic.
    """
    return str(backend['status']).startswith('UP')


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
    synapse_haproxy_url_format,
    service,
    marathon_tasks,
):
    """Returns the marathon tasks that are registered in haproxy under a given service (nerve_ns).

    :param synapse_host: The host that this check should contact for replication information.
    :param synapse_port: The port that this check should contact for replication information.
    :param synapse_haproxy_url_format: The format of the synapse haproxy URL.
    :param service: A list of strings that are the service names that should be checked for replication.
    :param marathon_tasks: A list of MarathonTask objects, whose tasks we will check for in the HAProxy status.
    """
    backends = get_multiple_backends([service], synapse_host=synapse_host, synapse_port=synapse_port,
                                     synapse_haproxy_url_format=synapse_haproxy_url_format)
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
