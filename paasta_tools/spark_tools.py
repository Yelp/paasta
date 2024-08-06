import logging
import re
import socket
import sys
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Mapping
from typing import Set

from mypy_extensions import TypedDict

from paasta_tools.utils import DockerVolume
from paasta_tools.utils import PaastaColors

KUBERNETES_NAMESPACE = "paasta-spark"
DEFAULT_SPARK_SERVICE = "spark"
DEFAULT_SPARK_RUNTIME_TIMEOUT = "12h"
SPARK_AWS_CREDS_PROVIDER = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
SPARK_EXECUTOR_NAMESPACE = "paasta-spark"
SPARK_DRIVER_POOL = "stable"
SPARK_JOB_USER = "TRON"
SPARK_PROMETHEUS_SHARD = "ml-compute"
SPARK_DNS_POD_TEMPLATE = "/nail/srv/configs/spark_dns_pod_template.yaml"
MEM_MULTIPLIER = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
SPARK_DRIVER_DEFAULT_DISK_MB = 5120  # 5GB

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


def get_webui_url(port: str) -> str:
    return f"http://{socket.getfqdn()}:{port}"


def get_volumes_from_spark_mesos_configs(spark_conf: Mapping[str, str]) -> List[str]:
    return (
        spark_conf.get("spark.mesos.executor.docker.volumes", "").split(",")
        if spark_conf.get("spark.mesos.executor.docker.volumes", "") != ""
        else []
    )


def get_volumes_from_spark_k8s_configs(spark_conf: Mapping[str, str]) -> List[str]:
    volume_names = []
    for key in list(spark_conf.keys()):
        if (
            "spark.kubernetes.executor.volumes.hostPath." in key
            and ".mount.path" in key
        ):
            v_name = re.match(
                r"spark.kubernetes.executor.volumes.hostPath.([a-z0-9]([-a-z0-9]*[a-z0-9])?).mount.path",
                key,
            )
            if v_name:
                volume_names.append(v_name.group(1))
            else:
                log.error(
                    f"Volume names must consist of lower case alphanumeric characters or '-', "
                    f"and must start and end with an alphanumeric character. Config -> '{key}' must be fixed."
                )
                # Failing here because the k8s pod fails to start if the volume names
                # don't follow the lowercase RFC 1123 standard.
                sys.exit(1)

    volumes = []
    for volume_name in volume_names:
        read_only = (
            "ro"
            if spark_conf.get(
                f"spark.kubernetes.executor.volumes.hostPath.{volume_name}.mount.readOnly"
            )
            == "true"
            else "rw"
        )
        container_path = spark_conf.get(
            f"spark.kubernetes.executor.volumes.hostPath.{volume_name}.mount.path"
        )
        host_path = spark_conf.get(
            f"spark.kubernetes.executor.volumes.hostPath.{volume_name}.options.path"
        )
        volumes.append(f"{host_path}:{container_path}:{read_only}")
    return volumes


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


def create_spark_config_str(spark_config_dict: Dict[str, Any], is_mrjob: bool) -> str:
    conf_option = "--jobconf" if is_mrjob else "--conf"
    spark_config_entries = list()

    if is_mrjob:
        spark_master = spark_config_dict["spark.master"]
        spark_config_entries.append(f"--spark-master={spark_master}")
        spark_config_dict.pop("spark.master", None)

    for opt, val in spark_config_dict.items():
        # Process Spark configs with multiple space separated values to be in single quotes
        if isinstance(val, str) and " " in val:
            val = f"'{val}'"
        spark_config_entries.append(f"{conf_option} {opt}={val}")
    return " ".join(spark_config_entries)


def inject_spark_conf_str(original_cmd: str, spark_conf_str: str) -> str:
    for base_cmd in ("pyspark", "spark-shell", "spark-submit"):
        if base_cmd in original_cmd:
            return original_cmd.replace(base_cmd, base_cmd + " " + spark_conf_str, 1)
    return original_cmd


def auto_add_timeout_for_spark_job(cmd: str, timeout_job_runtime: str) -> str:
    # Timeout only to be added for spark-submit commands
    # TODO: Add timeout for jobs using mrjob with spark-runner
    if "spark-submit" not in cmd:
        return cmd
    try:
        timeout_present = re.match(
            r"^.*timeout[\s]+[\d]+[\.]?[\d]*[m|h][\s]+spark-submit .*$", cmd
        )
        if not timeout_present:
            split_cmd = cmd.split("spark-submit")
            # split_cmd[0] will always be an empty string or end with a space
            cmd = f"{split_cmd[0]}timeout {timeout_job_runtime} spark-submit{split_cmd[1]}"
            log.info(
                PaastaColors.blue(
                    f"NOTE: Job will exit in given time {timeout_job_runtime}. "
                    f"Adjust timeout value using --timeout-job-timeout. "
                    f"New Updated Command with timeout: {cmd}"
                ),
            )
    except Exception as e:
        err_msg = (
            f"'timeout' could not be added to command: '{cmd}' due to error '{e}'. "
            "Please report to #spark."
        )
        log.warn(err_msg)
        print(PaastaColors.red(err_msg))
    return cmd


def build_spark_command(
    original_cmd: str,
    spark_config_dict: Dict[str, Any],
    is_mrjob: bool,
    timeout_job_runtime: str,
) -> str:
    command = f"{inject_spark_conf_str(original_cmd, create_spark_config_str(spark_config_dict, is_mrjob=is_mrjob))}"
    return auto_add_timeout_for_spark_job(command, timeout_job_runtime)


def get_spark_ports_from_config(spark_conf: Dict[str, str]) -> List[int]:
    ports = [int(v) for k, v in spark_conf.items() if k.endswith(".port")]
    return ports


# TODO: Reuse by ad-hoc Spark-driver-on-k8s
def get_spark_driver_monitoring_annotations(
    spark_config: Dict[str, str],
) -> Dict[str, str]:
    """
    Returns Spark driver pod annotations - currently used for Prometheus metadata.
    """
    ui_port_str = str(spark_config.get("spark.ui.port", ""))
    annotations = {
        "prometheus.io/port": ui_port_str,
        "prometheus.io/path": "/metrics/prometheus",
    }
    return annotations


def get_spark_driver_monitoring_labels(
    spark_config: Dict[str, str],
) -> Dict[str, str]:
    """
    Returns Spark driver pod labels - generally for Prometheus metric relabeling.
    """
    ui_port_str = str(spark_config.get("spark.ui.port", ""))
    labels = {
        "paasta.yelp.com/prometheus_shard": SPARK_PROMETHEUS_SHARD,
        "spark.yelp.com/user": SPARK_JOB_USER,
        "spark.yelp.com/driver_ui_port": ui_port_str,
    }
    return labels
