import logging
import os
import socket
import sys
from typing import Mapping
from typing import Optional
from typing import Tuple

import boto3
from boto3 import Session
from ruamel.yaml import YAML
from typing_extensions import TypedDict

from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.utils import PaastaColors

AWS_CREDENTIALS_DIR = "/etc/boto_cfg/"
DEFAULT_SPARK_MESOS_SECRET_FILE = "/nail/etc/paasta_spark_secret"
DEFAULT_SPARK_RUN_CONFIG = "/nail/srv/configs/spark.yaml"
DEFAULT_SPARK_SERVICE = "spark"
clusterman_metrics, CLUSTERMAN_YAML_FILE_PATH = get_clusterman_metrics()
log = logging.getLogger(__name__)


class DockerVolumeDict(TypedDict):
    hostPath: str
    containerPath: str
    mode: str


def _load_aws_credentials_from_yaml(yaml_file_path) -> Tuple[str, str, Optional[str]]:
    with open(yaml_file_path, "r") as yaml_file:
        try:
            credentials_yaml = YAML().load(yaml_file.read())
        except Exception as e:
            print(
                PaastaColors.red(
                    "Encountered %s when trying to parse AWS credentials yaml %s. "
                    "Suppressing further output to avoid leaking credentials."
                    % (type(e), yaml_file_path)
                )
            )
            sys.exit(1)

        return (
            credentials_yaml["aws_access_key_id"],
            credentials_yaml["aws_secret_access_key"],
            credentials_yaml.get("aws_session_token", None),
        )


def get_aws_credentials(
    service: str = DEFAULT_SPARK_SERVICE,
    no_aws_credentials: bool = False,
    aws_credentials_yaml: Optional[str] = None,
    profile_name: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if no_aws_credentials:
        return None, None, None
    elif aws_credentials_yaml:
        return _load_aws_credentials_from_yaml(aws_credentials_yaml)
    elif service != DEFAULT_SPARK_SERVICE:
        service_credentials_path = os.path.join(AWS_CREDENTIALS_DIR, f"{service}.yaml")
        if os.path.exists(service_credentials_path):
            return _load_aws_credentials_from_yaml(service_credentials_path)
        else:
            print(
                PaastaColors.yellow(
                    "Did not find service AWS credentials at %s.  Falling back to "
                    "user credentials." % (service_credentials_path)
                )
            )

    
    creds = Session(profile_name=profile_name).get_credentials()
    return (
        creds.access_key,
        creds.secret_key,
        creds.token,
    )


def get_default_event_log_dir(**kwargs) -> str:
    access_key, secret_key, session_token = kwargs["access_key"], kwargs["secret_key"], kwargs["session_token"]
    if access_key is None:
        log.warning(
            "Since no AWS credentials were provided, spark event logging "
            "will be disabled"
        )
        return None

    try:
        with open(DEFAULT_SPARK_RUN_CONFIG) as fp:
            spark_run_conf = YAML().load(fp.read())
    except Exception as e:
        log.warning(f"Failed to load {DEFAULT_SPARK_RUN_CONFIG}: {e}")
        log.warning("Returning empty default configuration")
        spark_run_conf = {}

    try:
        return boto3.client(
            "sts",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
        ).get_caller_identity().get("Account")
    except Exception as e:
        log.warning("Failed to identify account ID, error: {}".format(str(e)))
        return None

    for conf in spark_run_conf.get("environments", {}).values():
        if account_id == conf["account_id"]:
            default_event_log_dir = conf["default_event_log_dir"]
            print(f"default event logging at: {default_event_log_dir}")
            return default_event_log_dir
    return None


def load_mesos_secret_for_spark():
    try:
        with open(DEFAULT_SPARK_MESOS_SECRET_FILE, "r") as f:
            return f.read()
    except IOError as e:
        print(
            "Cannot load mesos secret from %s" % DEFAULT_SPARK_MESOS_SECRET_FILE,
            file=sys.stderr,
        )
        raise e


def _calculate_memory_per_executor(spark_memory_string, memory_overhead):
    # expected to be in format "dg" where d is an integer
    base_memory_per_executor = 1024 * int(spark_memory_string[:-1])

    # by default, spark adds an overhead of 10% of the executor memory, with
    # a minimum of 384mb
    if memory_overhead is None:
        memory_overhead = max(384, int(0.1 * base_memory_per_executor))
    else:
        memory_overhead = int(memory_overhead)

    return base_memory_per_executor + memory_overhead


def get_spark_resource_requirements(
    spark_config_dict: Mapping[str, str], webui_url: str,
) -> Mapping[str, Tuple[str, int]]:
    if not clusterman_metrics:
        return {}
    num_executors = int(spark_config_dict["spark.cores.max"]) / int(
        spark_config_dict["spark.executor.cores"]
    )
    memory_per_executor = _calculate_memory_per_executor(
        spark_config_dict["spark.executor.memory"],
        spark_config_dict.get("spark.mesos.executor.memoryOverhead"),
    )

    desired_resources = {
        "cpus": int(spark_config_dict["spark.cores.max"]),
        "mem": memory_per_executor * num_executors,
        # rough guess since spark does not collect this information
        "disk": memory_per_executor * num_executors,
    }
    dimensions = {
        "framework_name": spark_config_dict["spark.app.name"],
        "webui_url": webui_url,
    }
    qualified_resources = {}
    for resource, quantity in desired_resources.items():
        qualified_resources[resource] = (
            clusterman_metrics.generate_key_with_dimensions(
                f"requested_{resource}", dimensions
            ),
            desired_resources[resource],
        )

    return qualified_resources


def get_webui_url(port: int) -> str:
    return f"http://{socket.getfqdn()}:{port}"


def inject_spark_conf_str(original_docker_cmd, spark_conf_str):
    for base_cmd in ("pyspark", "spark-shell", "spark-submit"):
        if base_cmd in original_docker_cmd:
            return original_docker_cmd.replace(
                base_cmd, base_cmd + " " + spark_conf_str, 1
            )
    return original_docker_cmd
