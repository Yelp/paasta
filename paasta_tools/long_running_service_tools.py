from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import socket

import service_configuration_lib
from kazoo.exceptions import NoNodeError

from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import ZookeeperPool

DEFAULT_CONTAINER_PORT = 8888

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)

AUTOSCALING_ZK_ROOT = '/autoscaling'


class LongRunningServiceConfig(InstanceConfig):
    def __init__(self, service, cluster, instance, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        super(LongRunningServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def get_drain_method(self, service_namespace_config):
        """Get the drain method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain method specified in the config, or 'noop' if not specified"""
        default = 'noop'
        # Default to hacheck draining if the service is in smartstack
        if service_namespace_config.is_in_smartstack():
            default = 'hacheck'
        return self.config_dict.get('drain_method', default)

    def get_drain_method_params(self, service_namespace_config):
        """Get the drain method parameters specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain_method_params dictionary specified in the config, or {} if not specified"""
        default = {}
        if service_namespace_config.is_in_smartstack():
            default = {'delay': 60}
        return self.config_dict.get('drain_method_params', default)

    # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
    def get_nerve_namespace(self):
        return decompose_job_id(self.get_registrations()[0])[1]

    def get_registrations(self):
        registrations = self.config_dict.get('registrations', [])
        for registration in registrations:
            try:
                decompose_job_id(registration)
            except InvalidJobNameError:
                log.error(
                    'Provided registration {} for service '
                    '{} is invalid'.format(registration, self.service)
                )

        # Backwards compatbility with nerve_ns
        # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
        if not registrations and 'nerve_ns' in self.config_dict:
            registrations.append(
                compose_job_id(self.service, self.config_dict['nerve_ns'])
            )

        return registrations or [compose_job_id(self.service, self.instance)]

    def get_healthcheck_uri(self, service_namespace_config):
        return self.config_dict.get('healthcheck_uri', service_namespace_config.get_healthcheck_uri())

    def get_healthcheck_cmd(self):
        cmd = self.config_dict.get('healthcheck_cmd', None)
        if cmd is None:
            raise InvalidInstanceConfig("healthcheck mode 'cmd' requires a healthcheck_cmd to run")
        else:
            return cmd

    def get_healthcheck_grace_period_seconds(self):
        """How long Marathon should give a service to come up before counting failed healthchecks."""
        return self.config_dict.get('healthcheck_grace_period_seconds', 60)

    def get_healthcheck_interval_seconds(self):
        return self.config_dict.get('healthcheck_interval_seconds', 10)

    def get_healthcheck_timeout_seconds(self):
        return self.config_dict.get('healthcheck_timeout_seconds', 10)

    def get_healthcheck_max_consecutive_failures(self):
        return self.config_dict.get('healthcheck_max_consecutive_failures', 30)

    def get_healthcheck_mode(self, service_namespace_config):
        mode = self.config_dict.get('healthcheck_mode', None)
        if mode is None:
            mode = service_namespace_config.get_mode()
        elif mode not in ['http', 'tcp', 'cmd', None]:
            raise InvalidHealthcheckMode("Unknown mode: %s" % mode)
        return mode

    def get_instances(self):
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
                limited_instances = self.limit_instance_count(zk_instances)
                if limited_instances != zk_instances:
                    log.warning("Returning limited instance count %d. (zk had %d)" % (
                                limited_instances, zk_instances))
                return limited_instances
        else:
            instances = self.config_dict.get('instances', 1)
            log.debug("Autoscaling not enabled, returning %d instances" % instances)
            return instances

    def get_min_instances(self):
        return self.config_dict.get('min_instances', 1)

    def get_max_instances(self):
        return self.config_dict.get('max_instances', None)

    def get_desired_instances(self):
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

    def limit_instance_count(self, instances):
        """
        Returns param instances if it is between min_instances and max_instances.
        Returns max_instances if instances > max_instances
        Returns min_instances if instances < min_instances
        """
        return max(
            self.get_min_instances(),
            min(self.get_max_instances(), instances),
        )

    def get_container_port(self):
        return self.config_dict.get('container_port', DEFAULT_CONTAINER_PORT)


class InvalidHealthcheckMode(Exception):
    pass


def get_healthcheck_for_instance(service, instance, service_manifest, random_port, soa_dir=DEFAULT_SOA_DIR):
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

    if mode == "http":
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


def load_service_namespace_config(service, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Attempt to read the configuration for a service's namespace in a more strict fashion.

    Retrevies the following keys:

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
        service_name=service, soa_dir=soa_dir)
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
        'extra_healthcheck_headers'
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


class ServiceNamespaceConfig(dict):

    def get_mode(self):
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

    def get_healthcheck_uri(self):
        return self.get('healthcheck_uri', '/status')

    def get_discover(self):
        return self.get('discover', 'region')

    def is_in_smartstack(self):
        if self.get('proxy_port') is not None:
            return True
        else:
            return False


class InvalidSmartstackMode(Exception):
    pass


def get_instances_from_zookeeper(service, instance):
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get('%s/instances' % compose_autoscaling_zookeeper_root(service, instance))
        return int(instances)


def compose_autoscaling_zookeeper_root(service, instance):
    return '%s/%s/%s' % (AUTOSCALING_ZK_ROOT, service, instance)


def set_instances_for_marathon_service(service, instance, instance_count, soa_dir=DEFAULT_SOA_DIR):
    zookeeper_path = '%s/instances' % compose_autoscaling_zookeeper_root(service, instance)
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count))
