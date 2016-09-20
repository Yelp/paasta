import logging

import service_configuration_lib

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)


class LongRunningServiceConfig(InstanceConfig):
    def __init__(self, service, cluster, instance, config_dict, branch_dict):
        super(LongRunningServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
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
        return self.get_registration_namespaces()[0]

    def get_registration_namespaces(self):
        registration_namespaces = self.config_dict.get('registration_namespaces', [])
        # Backwards compatbility with nerve_ns
        # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
        if not registration_namespaces and 'nerve_ns' in self.config_dict:
            registration_namespaces.append(self.config_dict.get('nerve_ns'))

        return registration_namespaces or [self.instance]


def load_service_namespace_config(service, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Attempt to read the configuration for a service's namespace in a more strict fashion.

    Retrevies the following keys:

    - proxy_port: the proxy port defined for the given namespace
    - healthcheck_mode: the mode for the healthcheck (http or tcp)
    - healthcheck_uri: URI target for healthchecking
    - healthcheck_timeout_s: healthcheck timeout in seconds
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

    service_config = service_configuration_lib.read_service_configuration(service, soa_dir)
    smartstack_config = service_config.get('smartstack', {})
    namespace_config_from_file = smartstack_config.get(namespace, {})

    service_namespace_config = ServiceNamespaceConfig()
    # We can't really use .get, as we don't want the key to be in the returned
    # dict at all if it doesn't exist in the config file.
    # We also can't just copy the whole dict, as we only care about some keys
    # and there's other things that appear in the smartstack section in
    # several cases.
    key_whitelist = set([
        'healthcheck_mode',
        'healthcheck_uri',
        'healthcheck_timeout_s',
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
    ])

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
