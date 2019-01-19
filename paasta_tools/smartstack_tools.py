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
import abc
import collections
import csv
import socket
from typing import cast
from typing import Collection
from typing import Container
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar

import requests
from kubernetes.client import V1Node
from mypy_extensions import TypedDict

from paasta_tools import kubernetes_tools
from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import SystemPaastaConfig


HaproxyBackend = TypedDict(
    'HaproxyBackend',
    {
        'pxname': str,
        'svname': str,
        'status': str,
    },
    total=False,
)


def retrieve_haproxy_csv(
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
) -> Iterable[Dict[str, str]]:
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
        requests.adapters.HTTPAdapter(max_retries=3),
    )
    haproxy_request.mount(
        'https://',
        requests.adapters.HTTPAdapter(max_retries=3),
    )
    haproxy_response = haproxy_request.get(synapse_uri, timeout=1)
    haproxy_data = haproxy_response.text
    reader = csv.DictReader(haproxy_data.splitlines())
    return reader


def get_backends(
    service: str,
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
) -> List[HaproxyBackend]:
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
    return get_multiple_backends(
        services, synapse_host=synapse_host, synapse_port=synapse_port,
        synapse_haproxy_url_format=synapse_haproxy_url_format,
    )


def get_multiple_backends(
    services: Optional[Container[str]],
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
) -> List[HaproxyBackend]:
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
            backends.append(cast(HaproxyBackend, line))

    return backends


def load_smartstack_info_for_service(
    service: str,
    namespace: str,
    blacklist: DeployBlacklist,
    system_paasta_config: SystemPaastaConfig,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Dict[str, Dict[str, int]]:
    """Retrieves number of available backends for given services

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
    service_namespace_config = marathon_tools.load_service_namespace_config(
        service=service, namespace=namespace, soa_dir=soa_dir,
    )
    discover_location_type = service_namespace_config.get_discover()
    return get_smartstack_replication_for_attribute(
        attribute=discover_location_type,
        service=service,
        namespace=namespace,
        blacklist=blacklist,
        system_paasta_config=system_paasta_config,
    )


def get_smartstack_replication_for_attribute(
    attribute: str,
    service: str,
    namespace: str,
    blacklist: DeployBlacklist,
    system_paasta_config: SystemPaastaConfig,
) -> Dict[str, Dict[str, int]]:
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
        whitelist=None,
    )
    if not filtered_slaves:
        raise NoSlavesAvailableError

    attribute_slave_dict = mesos_tools.get_mesos_slaves_grouped_by_attribute(
        slaves=filtered_slaves,
        attribute=attribute,
    )

    full_name = compose_job_id(service, namespace)

    for value, hosts in attribute_slave_dict.items():
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


def get_replication_for_all_services(
        synapse_host: str,
        synapse_port: int,
        synapse_haproxy_url_format: str,
) -> Dict[str, int]:
    """Returns the replication level for all services known to this synapse haproxy

    :param synapse_host: The host that this check should contact for replication information.
    :param synapse_port: The port number that this check should contact for replication information.
    :param synapse_haproxy_url_format: The format of the synapse haproxy URL.
    :returns available_instance_counts: A dictionary mapping the service names
                                        to an integer number of available replicas.
    """
    backends = get_multiple_backends(
        services=None,
        synapse_host=synapse_host,
        synapse_port=synapse_port,
        synapse_haproxy_url_format=synapse_haproxy_url_format,
    )
    return collections.Counter([b['pxname'] for b in backends if backend_is_up(b)])


def get_replication_for_services(
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
    services: Collection[str],
) -> Dict[str, int]:
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host: The host that this check should contact for replication information.
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
    return {sn: counter[sn] for sn in services}


def backend_is_up(backend: HaproxyBackend) -> bool:
    """Returns whether a server is receiving traffic in HAProxy.

    :param backend: backend dict, like one of those returned by smartstack_tools.get_multiple_backends.

    :returns is_up: Whether the backend is in a state that receives traffic.
    """
    return str(backend['status']).startswith('UP')


def ip_port_hostname_from_svname(svname: str) -> Tuple[str, int, str]:
    """This parses the haproxy svname that smartstack creates.
    In old versions of synapse, this is in the format ip:port_hostname.
    In versions newer than dd5843c987740a5d5ce1c83b12b258b7253784a8 it is
    hostname_ip:port

    :param svname: A svname, in either of the formats described above
    :returns ip_port_hostname: A tuple of ip, port, hostname.
    """
    # split into parts
    parts = set(svname.split("_"))

    # find those that can be split by : - this is the ip:port
    # there will only be 1 of these
    ip_ports = {part for part in parts if len(part.split(":")) == 2}

    # the one *not* in the list is the hostname
    hostname = parts.difference(ip_ports).pop()

    ip, port = ip_ports.pop().split(":")
    return ip, int(port), hostname


def get_registered_marathon_tasks(
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
    service: str,
    marathon_tasks: Iterable[marathon_tools.MarathonTask],
) -> List[marathon_tools.MarathonTask]:
    """Returns the marathon tasks that are registered in haproxy under a given service (nerve_ns).

    :param synapse_host: The host that this check should contact for replication information.
    :param synapse_port: The port that this check should contact for replication information.
    :param synapse_haproxy_url_format: The format of the synapse haproxy URL.
    :param service: A list of strings that are the service names that should be checked for replication.
    :param marathon_tasks: A list of MarathonTask objects, whose tasks we will check for in the HAProxy status.
    """
    backends = get_multiple_backends(
        [service], synapse_host=synapse_host, synapse_port=synapse_port,
        synapse_haproxy_url_format=synapse_haproxy_url_format,
    )
    healthy_tasks = []
    for backend, task in match_backends_and_tasks(backends, marathon_tasks):
        if backend is not None and task is not None and backend['status'].startswith('UP'):
            healthy_tasks.append(task)
    return healthy_tasks


def are_services_up_on_ip_port(
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
    services: Collection[str],
    host_ip: str,
    host_port: int,
) -> bool:
    backends = get_multiple_backends(
        services, synapse_host=synapse_host, synapse_port=synapse_port,
        synapse_haproxy_url_format=synapse_haproxy_url_format,
    )
    backends_by_ip_port: DefaultDict[Tuple[str, int], List[HaproxyBackend]] = collections.defaultdict(list)

    for backend in backends:
        ip, port, _ = ip_port_hostname_from_svname(backend['svname'])
        backends_by_ip_port[ip, port].append(backend)

    backends_on_ip = backends_by_ip_port[host_ip, host_port]
    # any backend being up is okay because a previous backend
    # may have had the same IP and synapse only removes them completely
    # after some time
    services_with_atleast_one_backend_up = {service: False for service in services}
    for service in services:
        for be in backends_on_ip:
            if be['pxname'] == service and backend_is_up(be):
                services_with_atleast_one_backend_up[service] = True
    return all(services_with_atleast_one_backend_up.values())


def match_backends_and_tasks(
    backends: Iterable[HaproxyBackend],
    tasks: Iterable[marathon_tools.MarathonTask],
) -> List[Tuple[Optional[HaproxyBackend], Optional[marathon_tools.MarathonTask]]]:
    """Returns tuples of matching (backend, task) pairs, as matched by IP and port. Each backend will be listed exactly
    once, and each task will be listed once per port. If a backend does not match with a task, (backend, None) will
    be included. If a task's port does not match with any backends, (None, task) will be included.

    :param backends: An iterable of haproxy backend dictionaries, e.g. the list returned by
                     smartstack_tools.get_multiple_backends.
    :param tasks: An iterable of MarathonTask objects.
    """

    # { (ip, port) : [backend1, backend2], ... }
    backends_by_ip_port: DefaultDict[Tuple[str, int], List[HaproxyBackend]] = collections.defaultdict(list)
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


_MesosSlaveDict = TypeVar('_MesosSlaveDict', bound=Dict)  # no type has been defined in mesos_tools for these yet.


class SmartstackHost(NamedTuple):
    hostname: str
    pool: str


class SmartstackReplicationChecker(abc.ABC):
    """Base class for checking smartstack replication. Extendable for different frameworks.

    Optimized for multiple queries. Gets the list of backends from synapse-`roxy
    only once per location and reuse it in all subsequent calls of
    SmartstackReplicationChecker.get_replication_for_instance().

    _get_allowed_locations_and_hosts must be implemented in sub class
    """

    def __init__(
        self,
        system_paasta_config: SystemPaastaConfig,
    ) -> None:
        self._synapse_port = system_paasta_config.get_synapse_port()
        self._synapse_haproxy_url_format = system_paasta_config.get_synapse_haproxy_url_format()
        self._system_paasta_config = system_paasta_config
        self._cache: Dict[str, Dict[str, int]] = {}

    @abc.abstractmethod
    def _get_allowed_locations_and_hosts(self, instance_config: InstanceConfig) -> Dict[str, Sequence[SmartstackHost]]:
        pass

    def get_replication_for_instance(
        self,
        instance_config: InstanceConfig,
    ) -> Dict[str, Dict[str, int]]:
        """Returns the number of registered instances in each discoverable location.

        :param instance_config: An instance of MarathonServiceConfig.
        :returns: a dict {'location_type': {'service.instance': int}}
        """
        replication_info = {}
        attribute_host_dict = self._get_allowed_locations_and_hosts(instance_config)
        instance_pool = instance_config.get_pool()
        for location, hosts in attribute_host_dict.items():
            hostname = self._get_first_host_in_pool(hosts, instance_pool)
            replication_info[location] = self._get_replication_info(location, hostname, instance_config)
        return replication_info

    def _get_first_host_in_pool(
        self,
        hosts: Sequence[SmartstackHost],
        pool: str,
    ) -> str:
        for host in hosts:
            if host.pool == pool:
                return host.hostname
        return hosts[0].hostname

    def _get_replication_info(
        self,
        location: str,
        hostname: str,
        instance_config: InstanceConfig,
    ) -> Dict[str, int]:
        """Returns service.instance and the number of instances registered in smartstack
        at the location as a dict.

        :param location: A string that identifies a habitat, a region and etc.
        :param hostname: A mesos slave hostname to read replication information from.
        :param instance_config: An instance of MarathonServiceConfig.
        :returns: A dict {"service.instance": number_of_instances}.
        """
        full_name = compose_job_id(instance_config.service, instance_config.instance)
        if location not in self._cache:
            self._cache[location] = get_replication_for_all_services(
                synapse_host=hostname,
                synapse_port=self._synapse_port,
                synapse_haproxy_url_format=self._synapse_haproxy_url_format,
            )
        return {full_name: self._cache[location][full_name]}


class MesosSmartstackReplicationChecker(SmartstackReplicationChecker):
    """Retrieves the number of registered instances in each discoverable location.

    Based on SmartstackReplicationChecker takes mesos slaves as an argument to filter
    which services are allowed to run where.
    :Example:

    >>> from paasta_tools.mesos_tools import get_slaves
    >>> from paasta_tools.utils import load_system_paasta_config
    >>> from paasta_tools.marathon_tools import load_marathon_service_config
    >>> from paasta_tools.smartstack_tools import SmartstackReplicationChecker
    >>>
    >>> mesos_slaves = get_slaves()
    >>> system_paasta_config = load_system_paasta_config()
    >>> instance_config = load_marathon_service_config(service='fake_service',
    ...                       instance='fake_instance', cluster='norcal-stagef')
    >>>
    >>> c = SmartstackReplicationChecker(mesos_slaves, system_paasta_config)
    >>> c.get_replication_for_instance(instance_config)
    {'uswest1-stagef': {'fake_service.fake_instance': 2}}
    >>>
    """

    def __init__(
        self,
        mesos_slaves: List[_MesosSlaveDict],
        system_paasta_config: SystemPaastaConfig,
    ) -> None:
        self._mesos_slaves = mesos_slaves
        super().__init__(system_paasta_config=system_paasta_config)

    def _get_allowed_locations_and_hosts(self, instance_config: InstanceConfig) -> Dict[str, Sequence[SmartstackHost]]:
        """Returns a dict of locations and lists of corresponding mesos slaves
        where deployment of the instance is allowed.

        :param instance_config: An instance of MarathonServiceConfig
        :returns: A dict {"uswest1-prod": [SmartstackHost(), SmartstackHost(), ...]}
        """
        monitoring_blacklist = instance_config.get_monitoring_blacklist(
            system_deploy_blacklist=self._system_paasta_config.get_deploy_blacklist(),
        )
        filtered_slaves = mesos_tools.filter_mesos_slaves_by_blacklist(
            slaves=self._mesos_slaves,
            blacklist=monitoring_blacklist,
            whitelist=None,
        )
        discover_location_type = marathon_tools.load_service_namespace_config(
            service=instance_config.service,
            namespace=instance_config.instance,
            soa_dir=instance_config.soa_dir,
        ).get_discover()
        attribute_to_slaves = mesos_tools.get_mesos_slaves_grouped_by_attribute(
            slaves=filtered_slaves,
            attribute=discover_location_type,
        )
        ret: Dict[str, Sequence[SmartstackHost]] = {}
        for attr, slaves in attribute_to_slaves.items():
            ret[attr] = [
                SmartstackHost(hostname=slave['hostname'], pool=slave['attributes']['pool']) for slave in slaves
            ]
        return ret


class KubeSmartstackReplicationChecker(SmartstackReplicationChecker):

    def __init__(
        self,
        nodes: Sequence[V1Node],
        system_paasta_config: SystemPaastaConfig,
    ) -> None:
        self.nodes = nodes
        super().__init__(system_paasta_config=system_paasta_config)

    def _get_allowed_locations_and_hosts(self, instance_config: InstanceConfig) -> Dict[str, Sequence[SmartstackHost]]:

        monitoring_blacklist = instance_config.get_monitoring_blacklist(
            system_deploy_blacklist=self._system_paasta_config.get_deploy_blacklist(),
        )
        filtered_nodes = kubernetes_tools.filter_nodes_by_blacklist(
            nodes=self.nodes,
            blacklist=monitoring_blacklist,
            whitelist=None,
        )
        discover_location_type = kubernetes_tools.load_service_namespace_config(
            service=instance_config.service,
            namespace=instance_config.instance,
            soa_dir=instance_config.soa_dir,
        ).get_discover()
        attribute_to_nodes = kubernetes_tools.get_nodes_grouped_by_attribute(
            nodes=filtered_nodes,
            attribute=discover_location_type,
        )
        ret: Dict[str, Sequence[SmartstackHost]] = {}
        for attr, nodes in attribute_to_nodes.items():
            ret[attr] = [
                SmartstackHost(
                    hostname=node.metadata.labels['yelp.com/hostname'],
                    pool=node.metadata.labels['yelp.com/pool'],
                ) for node in nodes
            ]
        return ret
