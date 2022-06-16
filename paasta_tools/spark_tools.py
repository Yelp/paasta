import copy
import logging
import socket
from functools import lru_cache
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import yaml
from mypy_extensions import TypedDict
from service_configuration_lib.spark_config import _adjust_spark_requested_resources
from service_configuration_lib.spark_config import _append_sql_shuffle_partitions_conf
from service_configuration_lib.spark_config import DEFAULT_SPARK_RUN_CONFIG

from paasta_tools.utils import DockerVolume
from paasta_tools.utils import get_runtimeenv

KUBERNETES_NAMESPACE = "paasta-spark"
DEFAULT_SPARK_SERVICE = "spark"

log = logging.getLogger(__name__)


class SparkConfig(TypedDict):
    account_id: str
    default_event_log_dir: str
    history_server: str


class SparkEnvironmentConfig(TypedDict):
    """
    There's no set schema for the spark configuration from DEFAULT_SPARK_RUN_CONFIG,
    but at the time of writing, this file looks like (with the addition of a `prod` key):
    environments:
        dev:
            account_id: 'SOME_ACCOUNT_ID'
            default_event_log_dir: s3a://SOME_BUCKET  # currently the only thing paasta reads
            history_server: http://SOME_URL/
    """

    environments: Dict[str, SparkConfig]


SparkEventLogConfiguration = TypedDict(
    "SparkEventLogConfiguration",
    {"spark.eventLog.enabled": str, "spark.eventLog.dir": str},
    total=False,
)


@lru_cache(maxsize=1)
def get_default_spark_configuration() -> Optional[SparkEnvironmentConfig]:
    """
    Read the globally distributed Spark configuration file and return the contents as a dictionary.

    At the time this comment was written, the only bit of information that we care about from this file
    is the default event log location on S3. See the TypedDict of the retval for the bits that we care about
    """
    try:
        with open(DEFAULT_SPARK_RUN_CONFIG, mode="r") as f:
            return yaml.safe_load(f.read())
    except OSError:
        log.error(
            f"Unable to open {DEFAULT_SPARK_RUN_CONFIG} and get default configuration values!"
        )
    except yaml.YAMLError:
        log.error(
            f"Unable to parse {DEFAULT_SPARK_RUN_CONFIG} and get default configuration values!"
        )

    return None


def get_webui_url(port: str) -> str:
    return f"http://{socket.getfqdn()}:{port}"


def setup_event_log_configuration(spark_args: Dict[str, str]) -> Dict[str, str]:
    """
    Adjusts user settings to provide a default event log storage path if event logging is
    enabled but not configured.

    If event logging is not enabled or is fully configured, this function will functionally noop.
    """
    # don't enable event logging if explicitly disabled
    if spark_args.get("spark.eventLog.enabled", "true") != "true":
        # Note: we provide an empty dict as our return value as the expected
        # usage of this function is something like CONF.update(setup_event_log_configuration(...))
        # in in this case, we don't want to update the existing config
        return {}

    # user set an explicit event log location - there's nothing else for us to
    # do here
    if spark_args.get("spark.eventLog.dir") is not None:
        # so, same as above, we return an empty dict so that there are no updates
        return {}

    default_spark_conf = get_default_spark_configuration()
    if default_spark_conf is None:
        log.error(
            "Unable to access default Spark configuration, event log will be disabled"
        )
        # Note: we don't return an empty dict here since we want to make sure that our
        # caller will overwrite the enabled option with our return value (see the first
        # `if` block in this function for more details)
        return {"spark.eventLog.enabled": "false"}

    environment_config = default_spark_conf.get("environments", {}).get(
        get_runtimeenv()
    )
    if environment_config is None:
        log.error(
            f"{get_runtimeenv()} not found in {DEFAULT_SPARK_RUN_CONFIG}, event log will be disabled"
        )
        return {"spark.eventLog.enabled": "false"}

    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": environment_config["default_event_log_dir"],
    }


def adjust_spark_resources(
    spark_args: Dict[str, str], desired_pool: str
) -> Dict[str, str]:
    """
    Wrapper around _adjust_spark_requested_resources from service_configuration_lib.

    We have some code that will do some QoL translations from Mesos->K8s arguments as well
    as set some more Yelpy defaults than what Spark uses.
    """
    # TODO: would be nice if _adjust_spark_requested_resources only returned the stuff it
    # modified
    return _adjust_spark_requested_resources(
        # additionally, _adjust_spark_requested_resources modifies the dict you pass in
        # so we make a copy to make things less confusing - consider dropping the
        # service_configuration_lib dependency here so that we can do things in a slightly
        # cleaner way
        user_spark_opts=copy.copy(spark_args),
        cluster_manager="kubernetes",
        pool=desired_pool,
    )


def setup_shuffle_partitions(spark_args: Dict[str, str]) -> Dict[str, str]:
    """
    Wrapper around _append_sql_shuffle_partitions_conf from service_configuration_lib.

    For now, this really just sets a default number of partitions based on # of cores.
    """
    # as above, this function also returns everything + mutates the passed in dictionary
    # which is not ideal
    return _append_sql_shuffle_partitions_conf(
        spark_opts=copy.copy(spark_args),
    )


def setup_volume_mounts(volumes: List[DockerVolume]) -> Dict[str, str]:
    """
    Returns Docker volume mount configurations in the format expected by Spark.
    """
    conf = {}

    # XXX: why are these necessary?
    extra_volumes: List[DockerVolume] = cast(
        "List[DockerVolume]",
        [
            {"containerPath": "/etc/passwd", "hostPath": "/etc/passwd", "mode": "RO"},
            {"containerPath": "/etc/group", "hostPath": "/etc/group", "mode": "RO"},
        ],
    )
    seen_paths: Set[str] = set()  # dedupe volumes, just in case
    for index, volume in enumerate(volumes + extra_volumes):
        host_path, container_path, mode = (
            volume["hostPath"],
            volume["containerPath"],
            volume["mode"],
        )

        if host_path in seen_paths:
            log.warn(f"Skipping {host_path} - already added a binding for it.")
            continue
        seen_paths.add(host_path)

        # the names here don't matter too much, so we just use the index in the volume
        # list as an arbitrary name
        conf[
            f"spark.kubernetes.executor.volumes.hostPath.{index}.mount.path"
        ] = container_path
        conf[
            f"spark.kubernetes.executor.volumes.hostPath.{index}.options.path"
        ] = host_path
        conf[
            f"spark.kubernetes.executor.volumes.hostPath.{index}.mount.readOnly"
        ] = str(mode.lower() == "ro").lower()

    return conf


def inject_spark_conf_str(original_docker_cmd: str, spark_conf_str: str) -> str:
    for base_cmd in ("pyspark", "spark-shell", "spark-submit"):
        if base_cmd in original_docker_cmd:
            return original_docker_cmd.replace(
                base_cmd, base_cmd + " " + spark_conf_str, 1
            )
    return original_docker_cmd
