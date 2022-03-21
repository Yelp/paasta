import socket
from functools import lru_cache
from typing import Dict
from typing import Optional

import yaml
from mypy_extensions import TypedDict

KUBERNETES_NAMESPACE = "paasta-spark"
DEFAULT_SPARK_SERVICE = "spark"

DEFAULT_SPARK_RUN_CONFIG = "/nail/srv/configs/spark.yaml"


class SparkConfig(TypedDict):
    account_id: str
    default_event_log_dir: str
    history_server: str


class SparkEnvironmentConfig(TypedDict):
    environments: Dict[str, SparkConfig]


@lru_cache(maxsize=1)
def get_default_spark_configuration() -> Optional[SparkEnvironmentConfig]:
    with open(DEFAULT_SPARK_RUN_CONFIG, mode="r") as f:
        return yaml.safe_load(f.read())


def get_webui_url(port: str) -> str:
    return f"http://{socket.getfqdn()}:{port}"


def inject_spark_conf_str(original_docker_cmd: str, spark_conf_str: str) -> str:
    for base_cmd in ("pyspark", "spark-shell", "spark-submit"):
        if base_cmd in original_docker_cmd:
            return original_docker_cmd.replace(
                base_cmd, base_cmd + " " + spark_conf_str, 1
            )
    return original_docker_cmd
