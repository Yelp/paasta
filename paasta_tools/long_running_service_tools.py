import copy
import logging
import os
import socket
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

import service_configuration_lib

from paasta_tools.autoscaling.utils import AutoscalingParamsDict
from paasta_tools.autoscaling.utils import MetricsProviderDict
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InstanceConfigDict
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig

log = logging.getLogger(__name__)
logging.getLogger("long_running_service_tools").setLevel(logging.WARNING)

ZK_PAUSE_AUTOSCALE_PATH = "/autoscaling/paused"
DEFAULT_CONTAINER_PORT = 8888

DEFAULT_AUTOSCALING_SETPOINT = 0.8
DEFAULT_DESIRED_ACTIVE_REQUESTS_PER_REPLICA = 1
DEFAULT_ACTIVE_REQUESTS_AUTOSCALING_MOVING_AVERAGE_WINDOW = 1800
DEFAULT_UWSGI_AUTOSCALING_MOVING_AVERAGE_WINDOW = 1800
DEFAULT_PISCINA_AUTOSCALING_MOVING_AVERAGE_WINDOW = 1800
DEFAULT_GUNICORN_AUTOSCALING_MOVING_AVERAGE_WINDOW = 1800

METRICS_PROVIDER_CPU = "cpu"
METRICS_PROVIDER_UWSGI = "uwsgi"
METRICS_PROVIDER_GUNICORN = "gunicorn"
METRICS_PROVIDER_PISCINA = "piscina"
METRICS_PROVIDER_ACTIVE_REQUESTS = "active-requests"
METRICS_PROVIDER_PROMQL = "arbitrary_promql"

ALL_METRICS_PROVIDERS = [
    METRICS_PROVIDER_CPU,
    METRICS_PROVIDER_UWSGI,
    METRICS_PROVIDER_GUNICORN,
    METRICS_PROVIDER_PISCINA,
    METRICS_PROVIDER_ACTIVE_REQUESTS,
    METRICS_PROVIDER_PROMQL,
]


class LongRunningServiceConfigDict(InstanceConfigDict, total=False):
    autoscaling: AutoscalingParamsDict
    drain_method: str
    fs_group: int
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
    network_mode: str
    registrations: List[str]
    replication_threshold: int
    bounce_start_deadline: float
    bounce_margin_factor: float
    should_ping_for_unhealthy_pods: bool
    weight: int


class ServiceNamespaceConfig(dict):
    def get_healthcheck_mode(self) -> str:
        """Get the healthcheck mode for the service. In most cases, this will match the mode
        of the service, but we do provide the opportunity for users to specify both. Default to the mode
        if no healthcheck_mode is specified.
        """
        healthcheck_mode = self.get("healthcheck_mode", None)
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
        mode = self.get("mode", None)
        if mode is None:
            if not self.is_in_smartstack():
                return None
            else:
                return "http"
        elif mode in ["http", "tcp", "https"]:
            return mode
        else:
            raise InvalidSmartstackMode("Unknown mode: %s" % mode)

    def get_healthcheck_uri(self) -> str:
        return self.get("healthcheck_uri", "/status")

    def get_discover(self) -> str:
        return self.get("discover", "region")

    def is_in_smartstack(self) -> bool:
        return "proxy_port" in self


class LongRunningServiceConfig(InstanceConfig):
    config_dict: LongRunningServiceConfigDict

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: LongRunningServiceConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def get_bounce_method(self) -> str:
        raise NotImplementedError

    def get_namespace(self) -> str:
        """Get namespace from config"""
        raise NotImplementedError

    def get_kubernetes_namespace(self) -> str:
        """
        Only needed on kubernetes LongRunningServiceConfig
        """
        raise NotImplementedError

    def get_sanitised_deployment_name(self) -> str:
        """
        Only needed on kubernetes LongRunningServiceConfig
        """
        raise NotImplementedError

    def get_service_name_smartstack(self) -> str:
        """
        This is just the service name here
        For cassandra we have to override this to support apollo
        """
        return self.get_service()

    def get_env(
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
    ) -> Dict[str, str]:
        env = super().get_env(system_paasta_config=system_paasta_config)
        env["PAASTA_PORT"] = str(self.get_container_port())
        return env

    def get_container_port(self) -> int:
        return self.config_dict.get("container_port", DEFAULT_CONTAINER_PORT)

    def get_drain_method(self, service_namespace_config: ServiceNamespaceConfig) -> str:
        """Get the drain method specified in the service's configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain method specified in the config, or 'noop' if not specified"""
        default = "noop"
        # Default to hacheck draining if the service is in smartstack
        if service_namespace_config.is_in_smartstack():
            default = "hacheck"
        return self.config_dict.get("drain_method", default)

    def get_drain_method_params(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> Dict:
        """Get the drain method parameters specified in the service's configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain_method_params dictionary specified in the config, or {} if not specified"""
        default: Dict = {}
        if service_namespace_config.is_in_smartstack():
            default = {"delay": 60}
        return self.config_dict.get("drain_method_params", default)

    # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
    def get_nerve_namespace(self) -> str:
        return decompose_job_id(self.get_registrations()[0])[1]

    def get_registrations(self) -> List[str]:
        for registration in self.get_invalid_registrations():
            log.error(
                "Provided registration {} for service "
                "{} is invalid".format(registration, self.service)
            )

        registrations = self.config_dict.get("registrations", [])

        # Backwards compatibility with nerve_ns
        # FIXME(jlynch|2016-08-02, PAASTA-4964): DEPRECATE nerve_ns and remove it
        if not registrations and "nerve_ns" in self.config_dict:
            registrations.append(
                compose_job_id(self.service, self.config_dict["nerve_ns"])
            )

        return registrations or [compose_job_id(self.service, self.instance)]

    def get_invalid_registrations(self) -> List[str]:
        registrations = self.config_dict.get("registrations", [])
        invalid_registrations: List[str] = []
        for registration in registrations:
            try:
                decompose_job_id(registration)
            except InvalidJobNameError:
                invalid_registrations.append(registration)
        return invalid_registrations

    def get_replication_crit_percentage(self) -> int:
        return self.config_dict.get("replication_threshold", 50)

    def get_fs_group(self) -> Optional[int]:
        return self.config_dict.get("fs_group")

    def get_healthcheck_uri(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> str:
        return self.config_dict.get(
            "healthcheck_uri", service_namespace_config.get_healthcheck_uri()
        )

    def get_healthcheck_cmd(self) -> str:
        cmd = self.config_dict.get("healthcheck_cmd", None)
        if cmd is None:
            raise InvalidInstanceConfig(
                "healthcheck mode 'cmd' requires a healthcheck_cmd to run"
            )
        else:
            return cmd

    def get_healthcheck_grace_period_seconds(self) -> float:
        """
        How long before kubernetes will start sending healthcheck and liveness probes.
        """
        return self.config_dict.get("healthcheck_grace_period_seconds", 60)

    def get_healthcheck_interval_seconds(self) -> float:
        return self.config_dict.get("healthcheck_interval_seconds", 10)

    def get_healthcheck_timeout_seconds(self) -> float:
        return self.config_dict.get("healthcheck_timeout_seconds", 10)

    def get_healthcheck_max_consecutive_failures(self) -> int:
        return self.config_dict.get("healthcheck_max_consecutive_failures", 30)

    def get_healthcheck_mode(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> str:
        mode = self.config_dict.get("healthcheck_mode", None)
        if mode is None:
            mode = service_namespace_config.get_healthcheck_mode()
        elif mode not in ["http", "https", "tcp", "cmd", None]:
            raise InvalidHealthcheckMode("Unknown mode: %s" % mode)
        return mode

    def get_bounce_start_deadline(self) -> float:
        return self.config_dict.get("bounce_start_deadline", 0)

    def get_autoscaled_instances(self) -> int:
        raise NotImplementedError()

    def get_instances(self, with_limit: bool = True) -> int:
        """Gets the number of instances for a service, ignoring whether the user has requested
        the service to be started or stopped"""
        if self.is_autoscaling_enabled():
            autoscaled_instances = self.get_autoscaled_instances()
            if autoscaled_instances is None:
                return self.get_max_instances()
            else:
                limited_instances = (
                    self.limit_instance_count(autoscaled_instances)
                    if with_limit
                    else autoscaled_instances
                )
                return limited_instances
        else:
            instances = self.config_dict.get("instances", 1)
            log.debug("Autoscaling not enabled, returning %d instances" % instances)
            return instances

    def get_min_instances(self) -> int:
        return self.config_dict.get("min_instances", 1)

    def is_autoscaling_enabled(self) -> bool:
        return self.get_max_instances() is not None

    def get_max_instances(self) -> Optional[int]:
        return self.config_dict.get("max_instances", None)

    def get_desired_instances(self) -> int:
        """Get the number of instances specified in zookeeper or the service's configuration.
        If the number of instances in zookeeper is less than min_instances, returns min_instances.
        If the number of instances in zookeeper is greater than max_instances, returns max_instances.

        Defaults to 0 if not specified in the config.

        :returns: The number of instances specified in the config, 0 if not
                  specified or if desired_state is not 'start'.
        """
        if self.get_desired_state() == "start":
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
        return max(self.get_min_instances(), min(self.get_max_instances(), instances))

    def get_autoscaling_params(self) -> AutoscalingParamsDict:
        default_provider_params: MetricsProviderDict = {
            "type": METRICS_PROVIDER_CPU,
            "decision_policy": "proportional",
            "setpoint": DEFAULT_AUTOSCALING_SETPOINT,
        }

        params = copy.deepcopy(
            self.config_dict.get("autoscaling", AutoscalingParamsDict({}))
        )
        if "metrics_providers" not in params or len(params["metrics_providers"]) == 0:
            params["metrics_providers"] = [default_provider_params]
        else:
            params["metrics_providers"] = [
                deep_merge_dictionaries(
                    overrides=provider,
                    defaults=default_provider_params,
                )
                for provider in params["metrics_providers"]
            ]
        return params

    def get_autoscaling_metrics_provider(
        self, provider_type: str
    ) -> Optional[MetricsProviderDict]:
        autoscaling_params = self.get_autoscaling_params()
        # We only allow one metric provider of each type, so we can bail early if we find a match
        for provider in autoscaling_params["metrics_providers"]:
            if provider["type"] == provider_type:
                return provider
        return None

    def should_use_metrics_provider(self, provider_type: str) -> bool:
        return (
            self.is_autoscaling_enabled()
            and self.get_autoscaling_metrics_provider(provider_type) is not None
        )

    def validate(
        self,
        params: Optional[List[str]] = None,
    ) -> List[str]:
        error_messages = super().validate(params=params)
        invalid_registrations = self.get_invalid_registrations()
        if invalid_registrations:
            service_instance = compose_job_id(self.service, self.instance)
            registrations_str = ", ".join(invalid_registrations)
            error_messages.append(
                f"Service registrations must be of the form service.registration. "
                f"The following registrations for {service_instance} are "
                f"invalid: {registrations_str}"
            )
        return error_messages

    def get_bounce_margin_factor(self) -> float:
        return self.config_dict.get("bounce_margin_factor", 0.95)

    def get_should_ping_for_unhealthy_pods(self, default: bool) -> bool:
        return self.config_dict.get("should_ping_for_unhealthy_pods", default)

    def get_weight(self) -> int:
        return self.config_dict.get("weight", 10)


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
        service=service, namespace=namespace, soa_dir=soa_dir
    )
    mode = service_manifest.get_healthcheck_mode(smartstack_config)
    hostname = socket.getfqdn()

    if mode == "http" or mode == "https":
        path = service_manifest.get_healthcheck_uri(smartstack_config)
        healthcheck_command = "%s://%s:%d%s" % (mode, hostname, random_port, path)
    elif mode == "tcp":
        healthcheck_command = "%s://%s:%d" % (mode, hostname, random_port)
    elif mode == "cmd":
        healthcheck_command = service_manifest.get_healthcheck_cmd()
    else:
        mode = None
        healthcheck_command = None
    return (mode, healthcheck_command)


def load_service_namespace_config(
    service: str, namespace: str, soa_dir: str = DEFAULT_SOA_DIR
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
    - retries: the number of retries on a proxy backend
    - mode: the mode the service is run in (http or tcp)
    - routes: a list of tuples of (source, destination)
    - discover: the scope at which to discover services e.g. 'habitat'
    - advertise: a list of scopes to advertise services at e.g. ['habitat', 'region']
    - extra_advertise: a list of tuples of (source, destination)
      e.g. [('region:dc6-prod', 'region:useast1-prod')]
    - extra_healthcheck_headers: a dict of HTTP headers that must
      be supplied when health checking. E.g. { 'Host': 'example.com' }
    - lb_policy: Envoy load balancer policies. E.g. "ROUND_ROBIN"

    :param service: The service name
    :param namespace: The namespace to read
    :param soa_dir: The SOA config directory to read from
    :returns: A dict of the above keys, if they were defined
    """

    smartstack_config = service_configuration_lib.read_extra_service_information(
        service_name=service,
        extra_info="smartstack",
        soa_dir=soa_dir,
        deepcopy=False,
    )

    namespace_config_from_file = smartstack_config.get(namespace, {})

    service_namespace_config = ServiceNamespaceConfig()
    # We can't really use .get, as we don't want the key to be in the returned
    # dict at all if it doesn't exist in the config file.
    # We also can't just copy the whole dict, as we only care about some keys
    # and there's other things that appear in the smartstack section in
    # several cases.
    key_whitelist = {
        "healthcheck_mode",
        "healthcheck_uri",
        "healthcheck_port",
        "healthcheck_timeout_s",
        "healthcheck_body_expect",
        "updown_timeout_s",
        "proxy_port",
        "timeout_connect_ms",
        "timeout_server_ms",
        "retries",
        "mode",
        "discover",
        "advertise",
        "extra_healthcheck_headers",
        "lb_policy",
    }

    for key, value in namespace_config_from_file.items():
        if key in key_whitelist:
            service_namespace_config[key] = value

    # Other code in paasta_tools checks 'mode' after the config file
    # is loaded, so this ensures that it is set to the appropriate default
    # if not otherwise specified, even if appropriate default is None.
    service_namespace_config["mode"] = service_namespace_config.get_mode()

    if "routes" in namespace_config_from_file:
        service_namespace_config["routes"] = [
            (route["source"], dest)
            for route in namespace_config_from_file["routes"]
            for dest in route["destinations"]
        ]

    if "extra_advertise" in namespace_config_from_file:
        service_namespace_config["extra_advertise"] = [
            (src, dst)
            for src in namespace_config_from_file["extra_advertise"]
            for dst in namespace_config_from_file["extra_advertise"][src]
        ]

    return service_namespace_config


class InvalidSmartstackMode(Exception):
    pass


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
        service=service, namespace=namespace, soa_dir=service_config.soa_dir
    )
    return nerve_dict.get("proxy_port")


def host_passes_blacklist(
    host_attributes: Mapping[str, str], blacklist: DeployBlacklist
) -> bool:
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


def host_passes_whitelist(
    host_attributes: Mapping[str, str], whitelist: DeployWhitelist
) -> bool:
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


def get_all_namespaces(
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    """Get all the smartstack namespaces across all services.
    This is mostly so synapse can get everything it needs in one call.

    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service.namespace, namespace_config)"""
    rootdir = os.path.abspath(soa_dir)
    namespace_list: List[Tuple[str, ServiceNamespaceConfig]] = []
    for srv_dir in os.listdir(rootdir):
        namespace_list.extend(get_all_namespaces_for_service(srv_dir, soa_dir))
    return namespace_list


def get_all_namespaces_for_service(
    service: str, soa_dir: str = DEFAULT_SOA_DIR, full_name: bool = True
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    """Get all the smartstack namespaces listed for a given service name.

    :param service: The service name
    :param soa_dir: The SOA config directory to read from
    :param full_name: A boolean indicating if the service name should be prepended to the namespace in the
                      returned tuples as described below (Default: True)
    :returns: A list of tuples of the form (service<SPACER>namespace, namespace_config) if full_name is true,
              otherwise of the form (namespace, namespace_config)
    """
    service_config = service_configuration_lib.read_service_configuration(
        service, soa_dir
    )
    smartstack = service_config.get("smartstack", {})
    namespace_list = []
    for namespace in smartstack:
        if full_name:
            name = compose_job_id(service, namespace)
        else:
            name = namespace
        namespace_list.append((name, smartstack[namespace]))
    return namespace_list


def get_expected_instance_count_for_namespace(
    service: str,
    namespace: str,
    instance_type_class: Type[LongRunningServiceConfig],
    cluster: str = None,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> int:
    """Get the number of expected instances for a namespace, based on the number
    of instances set to run on that namespace as specified in service configuration files.

    :param service: The service's name
    :param namespace: The namespace for that service to check
    instance_type_class: The type of the instance, options are e.g. KubernetesDeploymentConfig,
    :param soa_dir: The SOA configuration directory to read from
    :returns: An integer value of the # of expected instances for the namespace"""
    total_expected = 0
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()

    pscl = PaastaServiceConfigLoader(
        service=service, soa_dir=soa_dir, load_deployments=False
    )
    for job_config in pscl.instance_configs(
        cluster=cluster, instance_type_class=instance_type_class
    ):
        if f"{service}.{namespace}" in job_config.get_registrations():
            total_expected += job_config.get_instances()
    return total_expected
