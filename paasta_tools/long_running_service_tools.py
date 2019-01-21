import logging
import socket
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import service_configuration_lib
from kazoo.exceptions import NoNodeError
from mypy_extensions import TypedDict

from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InstanceConfigDict
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import ZookeeperPool

DEFAULT_CONTAINER_PORT = 8888

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)

AUTOSCALING_ZK_ROOT = '/autoscaling'
ZK_PAUSE_AUTOSCALE_PATH = '/autoscaling/paused'


class LongRunningServiceConfigDict(InstanceConfigDict, total=False):
    drain_method: str
    container_port: int
    drain_method_params: Dict
    healthcheck_cmd: str
    healthcheck_grace_period_seconds: float
    healthcheck_interval_seconds: float
    healthcheck_max_consecutive_failures: int
    healthcheck_mode: str
    healthcheck_timeout_seconds: float
    healthcheck_uri: str
    instances: int
    max_instances: int
    min_instances: int
    nerve_ns: str
    registrations: List[str]
    replication_threshold: int
    bounce_priority: int


# Defined here to avoid import cycles -- this gets used in bounce_lib and subclassed in marathon_tools.
BounceMethodConfigDict = TypedDict('BounceMethodConfigDict', {"instances": int})


class ServiceNamespaceConfig(dict):

    def get_healthcheck_mode(self) -> str:
        """Get the healthcheck mode for the service. In most cases, this will match the mode
        of the service, but we do provide the opportunity for users to specify both. Default to the mode
        if no healthcheck_mode is specified.
        """
        healthcheck_mode = self.get('healthcheck_mode', None)
        if not healthcheck_mode:
            return self.get_mode()
        else:
            return healthcheck_mode

    def get_mode(self) -> str:
        """Get the mode that the service runs in and check that we support it.
        If the mode is not specified, we check whether the service uses smartstack
        in order to determine the appropriate default value. If proxy_port is specified
        in the config, the service uses smartstack, and we can thus safely assume its mode is http.
        If the mode is not defined and the service does not use smartstack, we set the mode to None.
        """
        mode = self.get('mode', None)
        if mode is None:
            if not self.is_in_smartstack():
                return None
            else:
                return 'http'
        elif mode in ['http', 'tcp', 'https']:
            return mode
        else:
            raise InvalidSmartstackMode("Unknown mode: %s" % mode)

    def get_healthcheck_uri(self) -> str:
        return self.get('healthcheck_uri', '/status')

    def get_discover(self) -> str:
        return self.get('discover', 'region')

    def is_in_smartstack(self) -> bool:
        if self.get('proxy_port') is not None:
            return True
        else:
            return False


class LongRunningServiceConfig(InstanceConfig):
    config_dict: LongRunningServiceConfigDict

    def __init__(
        self, service: str, cluster: str, instance: str, config_dict: LongRunningServiceConfigDict,
        branch_dict: Optional[BranchDictV2], soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def get_drain_method(self, service_namespace_config: ServiceNamespaceConfig) -> str:
        """Get the drain method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain method specified in the config, or 'noop' if not specified"""
        default = 'noop'
        # Default to hacheck draining if the service is in smartstack
        if service_namespace_config.is_in_smartstack():
            default = 'hacheck'
        return self.config_dict.get('drain_method', default)

    def get_drain_method_params(self, service_namespace_config: ServiceNamespaceConfig) -> Dict:
        """Get the drain method parameters specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain_method_params dictionary specified in the config, or {} if not specified"""
        default: Dict = {}
        if service_namespace_config.is_in_smartstack():
            default = {'delay': 60}
        return self.config_dict.get('drain_method_params', default)

    # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
    def get_nerve_namespace(self) -> str:
        return decompose_job_id(self.get_registrations()[0])[1]

    def get_registrations(self) -> List[str]:
        registrations = self.config_dict.get('registrations', [])
        for registration in registrations:
            try:
                decompose_job_id(registration)
            except InvalidJobNameError:
                log.error(
                    'Provided registration {} for service '
                    '{} is invalid'.format(registration, self.service),
                )

        # Backwards compatibility with nerve_ns
        # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
        if not registrations and 'nerve_ns' in self.config_dict:
            registrations.append(
                compose_job_id(self.service, self.config_dict['nerve_ns']),
            )

        return registrations or [compose_job_id(self.service, self.instance)]

    def get_replication_crit_percentage(self) -> int:
        return self.config_dict.get('replication_threshold', 50)

    def get_healthcheck_uri(self, service_namespace_config: ServiceNamespaceConfig) -> str:
        return self.config_dict.get('healthcheck_uri', service_namespace_config.get_healthcheck_uri())

    def get_healthcheck_cmd(self) -> str:
        cmd = self.config_dict.get('healthcheck_cmd', None)
        if cmd is None:
            raise InvalidInstanceConfig("healthcheck mode 'cmd' requires a healthcheck_cmd to run")
        else:
            return cmd

    def get_healthcheck_grace_period_seconds(self) -> float:
        """How long Marathon should give a service to come up before counting failed healthchecks."""
        return self.config_dict.get('healthcheck_grace_period_seconds', 60)

    def get_healthcheck_interval_seconds(self) -> float:
        return self.config_dict.get('healthcheck_interval_seconds', 10)

    def get_healthcheck_timeout_seconds(self) -> float:
        return self.config_dict.get('healthcheck_timeout_seconds', 10)

    def get_healthcheck_max_consecutive_failures(self) -> int:
        return self.config_dict.get('healthcheck_max_consecutive_failures', 30)

    def get_healthcheck_mode(self, service_namespace_config: ServiceNamespaceConfig) -> str:
        mode = self.config_dict.get('healthcheck_mode', None)
        if mode is None:
            mode = service_namespace_config.get_healthcheck_mode()
        elif mode not in ['http', 'https', 'tcp', 'cmd', None]:
            raise InvalidHealthcheckMode("Unknown mode: %s" % mode)
        return mode

    def get_bounce_priority(self) -> int:
        """Gives a priority to each service instance which deployd will use to prioritise services.
        Higher numbers are higher priority. This affects the order in which deployd workers pick
        instances from the bounce queue.

        NB: we multiply by -1 here because *internally* lower numbers are higher priority.
        """
        return self.config_dict.get('bounce_priority', 0) * -1

    def get_instances(self, with_limit: bool = True) -> int:
        """Gets the number of instances for a service, ignoring whether the user has requested
        the service to be started or stopped"""
        if self.get_max_instances() is not None:
            try:
                zk_instances = get_instances_from_zookeeper(
                    service=self.service,
                    instance=self.instance,
                )
                log.debug("Got %d instances out of zookeeper" % zk_instances)
            except NoNodeError:
                log.debug("No zookeeper data, returning max_instances (%d)" % self.get_max_instances())
                return self.get_max_instances()
            else:
                limited_instances = self.limit_instance_count(zk_instances) if with_limit else zk_instances
                return limited_instances
        else:
            instances = self.config_dict.get('instances', 1)
            log.debug("Autoscaling not enabled, returning %d instances" % instances)
            return instances

    def get_min_instances(self) -> int:
        return self.config_dict.get('min_instances', 1)

    def get_max_instances(self) -> int:
        return self.config_dict.get('max_instances', None)

    def get_desired_instances(self) -> int:
        """Get the number of instances specified in zookeeper or the service's marathon configuration.
        If the number of instances in zookeeper is less than min_instances, returns min_instances.
        If the number of instances in zookeeper is greater than max_instances, returns max_instances.

        Defaults to 0 if not specified in the config.

        :returns: The number of instances specified in the config, 0 if not
                  specified or if desired_state is not 'start'.
                  """
        if self.get_desired_state() == 'start':
            return self.get_instances()
        else:
            log.debug("Instance is set to stop. Returning '0' instances")
            return 0

    def limit_instance_count(self, instances: int) -> int:
        """
        Returns param instances if it is between min_instances and max_instances.
        Returns max_instances if instances > max_instances
        Returns min_instances if instances < min_instances
        """
        return max(
            self.get_min_instances(),
            min(self.get_max_instances(), instances),
        )

    def get_container_port(self) -> int:
        return self.config_dict.get('container_port', DEFAULT_CONTAINER_PORT)


class InvalidHealthcheckMode(Exception):
    pass


def get_healthcheck_for_instance(
    service: str,
    instance: str,
    service_manifest: LongRunningServiceConfig,
    random_port: int,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns healthcheck for a given service instance in the form of a tuple (mode, healthcheck_command)
    or (None, None) if no healthcheck
    """
    namespace = service_manifest.get_nerve_namespace()
    smartstack_config = load_service_namespace_config(
        service=service,
        namespace=namespace,
        soa_dir=soa_dir,
    )
    mode = service_manifest.get_healthcheck_mode(smartstack_config)
    hostname = socket.getfqdn()

    if mode == "http" or mode == "https":
        path = service_manifest.get_healthcheck_uri(smartstack_config)
        healthcheck_command = '%s://%s:%d%s' % (mode, hostname, random_port, path)
    elif mode == "tcp":
        healthcheck_command = '%s://%s:%d' % (mode, hostname, random_port)
    elif mode == 'cmd':
        healthcheck_command = service_manifest.get_healthcheck_cmd()
    else:
        mode = None
        healthcheck_command = None
    return (mode, healthcheck_command)


def load_service_namespace_config(
    service: str,
    namespace: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> ServiceNamespaceConfig:
    """Attempt to read the configuration for a service's namespace in a more strict fashion.

    Retrieves the following keys:

    - proxy_port: the proxy port defined for the given namespace
    - healthcheck_mode: the mode for the healthcheck (http or tcp)
    - healthcheck_port: An alternate port to use for health checking
    - healthcheck_uri: URI target for healthchecking
    - healthcheck_timeout_s: healthcheck timeout in seconds
    - healthcheck_body_expect: an expected string in healthcheck response body
    - updown_timeout_s: updown_service timeout in seconds
    - timeout_connect_ms: proxy frontend timeout in milliseconds
    - timeout_server_ms: proxy server backend timeout in milliseconds
    - timeout_client_ms: proxy server client timeout in milliseconds
    - retries: the number of retries on a proxy backend
    - mode: the mode the service is run in (http or tcp)
    - routes: a list of tuples of (source, destination)
    - discover: the scope at which to discover services e.g. 'habitat'
    - advertise: a list of scopes to advertise services at e.g. ['habitat', 'region']
    - extra_advertise: a list of tuples of (source, destination)
      e.g. [('region:dc6-prod', 'region:useast1-prod')]
    - extra_healthcheck_headers: a dict of HTTP headers that must
      be supplied when health checking. E.g. { 'Host': 'example.com' }

    :param service: The service name
    :param namespace: The namespace to read
    :param soa_dir: The SOA config directory to read from
    :returns: A dict of the above keys, if they were defined
    """

    service_config = service_configuration_lib.read_service_configuration(
        service_name=service, soa_dir=soa_dir,
    )
    smartstack_config = service_config.get('smartstack', {})
    namespace_config_from_file = smartstack_config.get(namespace, {})

    service_namespace_config = ServiceNamespaceConfig()
    # We can't really use .get, as we don't want the key to be in the returned
    # dict at all if it doesn't exist in the config file.
    # We also can't just copy the whole dict, as we only care about some keys
    # and there's other things that appear in the smartstack section in
    # several cases.
    key_whitelist = {
        'healthcheck_mode',
        'healthcheck_uri',
        'healthcheck_port',
        'healthcheck_timeout_s',
        'healthcheck_body_expect',
        'updown_timeout_s',
        'proxy_port',
        'timeout_connect_ms',
        'timeout_server_ms',
        'timeout_client_ms',
        'retries',
        'mode',
        'discover',
        'advertise',
        'extra_healthcheck_headers',
    }

    for key, value in namespace_config_from_file.items():
        if key in key_whitelist:
            service_namespace_config[key] = value

    # Other code in paasta_tools checks 'mode' after the config file
    # is loaded, so this ensures that it is set to the appropriate default
    # if not otherwise specified, even if appropriate default is None.
    service_namespace_config['mode'] = service_namespace_config.get_mode()

    if 'routes' in namespace_config_from_file:
        service_namespace_config['routes'] = [(route['source'], dest)
                                              for route in namespace_config_from_file['routes']
                                              for dest in route['destinations']]

    if 'extra_advertise' in namespace_config_from_file:
        service_namespace_config['extra_advertise'] = [
            (src, dst)
            for src in namespace_config_from_file['extra_advertise']
            for dst in namespace_config_from_file['extra_advertise'][src]
        ]

    return service_namespace_config


class InvalidSmartstackMode(Exception):
    pass


def get_instances_from_zookeeper(service: str, instance: str) -> int:
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get('%s/instances' % compose_autoscaling_zookeeper_root(service, instance))
        return int(instances)


def compose_autoscaling_zookeeper_root(service: str, instance: str) -> str:
    return f'{AUTOSCALING_ZK_ROOT}/{service}/{instance}'


def set_instances_for_marathon_service(
    service: str,
    instance: str,
    instance_count: int,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> None:
    zookeeper_path = '%s/instances' % compose_autoscaling_zookeeper_root(service, instance)
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count).encode('utf8'))


def get_proxy_port_for_instance(
    service_config: LongRunningServiceConfig,
) -> Optional[int]:
    """Get the proxy_port defined in the first namespace configuration for a
    service instance.

    This means that the namespace first has to be loaded from the service instance's
    configuration, and then the proxy_port has to loaded from the smartstack configuration
    for that namespace.

    :param service_config: The instance of the services LongRunningServiceConfig
    :returns: The proxy_port for the service instance, or None if not defined"""
    registration = service_config.get_registrations()[0]
    service, namespace, _, __ = decompose_job_id(registration)
    nerve_dict = load_service_namespace_config(
        service=service, namespace=namespace, soa_dir=service_config.soa_dir,
    )
    return nerve_dict.get('proxy_port')


def host_passes_blacklist(host_attributes: Mapping[str, str], blacklist: DeployBlacklist) -> bool:
    """
    :param host: A single host attributes dict
    :param blacklist: A list of lists like [["location_type", "location"], ["foo", "bar"]]
    :returns: boolean, True if the host gets passed the blacklist
    """
    try:
        for location_type, location in blacklist:
            if host_attributes.get(location_type) == location:
                return False
    except ValueError as e:
        log.error(f"Errors processing the following blacklist: {blacklist}")
        log.error("I will assume the host does not pass\nError was: %s" % e)
        return False
    return True


def host_passes_whitelist(host_attributes: Mapping[str, str], whitelist: DeployWhitelist) -> bool:
    """
    :param host: A single host attributes dict.
    :param whitelist: A 2 item list like ["location_type", ["location1", 'location2']]
    :returns: boolean, True if the host gets past the whitelist
    """
    # No whitelist, so disable whitelisting behavior.
    if whitelist is None or len(whitelist) == 0:
        return True
    try:
        (location_type, locations) = whitelist
        if host_attributes.get(location_type) in locations:
            return True
    except ValueError as e:
        log.error(f"Errors processing the following whitelist: {whitelist}")
        log.error("I will assume the host does not pass\nError was: %s" % e)
        return False
    return False
