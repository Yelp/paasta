import os

import service_configuration_lib

DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR


INFRA_ZK_PATH = "/nail/etc/zookeeper_discovery/infrastructure/"

DEFAULT_DOCKERCFG_LOCATION = "file:///root/.dockercfg"
DEPLOY_PIPELINE_NON_DEPLOY_STEPS = (
    "itest",
    "itest-and-push-to-registry",
    "security-check",
    "performance-check",
    "push-to-registry",
)

DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT = (
    "http://{host:s}:{port:d}/;csv;norefresh;scope={scope:s}"
)


DEFAULT_SOA_CONFIGS_GIT_URL = "sysgit.yelpcorp.com"


# DO NOT CHANGE SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
SPACER = "."
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = os.environ.get(
    "PAASTA_SYSTEM_CONFIG_DIR", "/etc/paasta/"
)
AUTO_SOACONFIG_SUBDIR = "autotuned_defaults"

DEFAULT_CPU_PERIOD = 100000
DEFAULT_CPU_BURST_ADD = 1

INSTANCE_TYPES = (
    "marathon",
    "paasta_native",
    "adhoc",
    "kubernetes",
    "tron",
    "flink",
    "cassandracluster",
    "kafkacluster",
    "nrtsearchservice",
)
