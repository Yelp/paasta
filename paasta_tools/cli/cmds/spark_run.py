import argparse
import json
import logging
import os
import re
import shlex
import socket
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

from service_configuration_lib import read_service_configuration
from service_configuration_lib import read_yaml_file
from service_configuration_lib import spark_config
from service_configuration_lib.spark_config import get_aws_credentials
from service_configuration_lib.spark_config import get_grafana_url
from service_configuration_lib.spark_config import get_resources_requested
from service_configuration_lib.spark_config import get_spark_hourly_cost
from service_configuration_lib.spark_config import UnsupportedClusterManagerException

from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import get_service_auth_token
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.spark_tools import auto_add_timeout_for_spark_job
from paasta_tools.spark_tools import create_spark_config_str
from paasta_tools.spark_tools import DEFAULT_SPARK_RUNTIME_TIMEOUT
from paasta_tools.spark_tools import DEFAULT_SPARK_SERVICE
from paasta_tools.spark_tools import get_volumes_from_spark_k8s_configs
from paasta_tools.spark_tools import get_webui_url
from paasta_tools.spark_tools import inject_spark_conf_str
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import filter_templates_from_config
from paasta_tools.utils import get_k8s_url_for_cluster
from paasta_tools.utils import get_possible_launched_by_user_variable_from_env
from paasta_tools.utils import get_username
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import is_using_unprivileged_containers
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import PoolsNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import validate_pool


DEFAULT_AWS_REGION = "us-west-2"
DEFAULT_SPARK_WORK_DIR = "/spark_driver"
DEFAULT_SPARK_DOCKER_IMAGE_PREFIX = "paasta-spark-run"
DEFAULT_SPARK_DOCKER_REGISTRY = "docker-dev.yelpcorp.com"
SENSITIVE_ENV = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
clusterman_metrics, CLUSTERMAN_YAML_FILE_PATH = get_clusterman_metrics()
CLUSTER_MANAGER_K8S = "kubernetes"
CLUSTER_MANAGER_LOCAL = "local"
CLUSTER_MANAGERS = {CLUSTER_MANAGER_K8S, CLUSTER_MANAGER_LOCAL}
DEFAULT_DOCKER_SHM_SIZE = "64m"
# Reference: https://spark.apache.org/docs/latest/configuration.html#application-properties
DEFAULT_DRIVER_CORES_BY_SPARK = 1
DEFAULT_DRIVER_MEMORY_BY_SPARK = "1g"
# Extra room for memory overhead and for any other running inside container
DOCKER_RESOURCE_ADJUSTMENT_FACTOR = 2

DEPRECATED_OPTS = {
    "j": "spark.jars",
    "jars": "spark.jars",
}

SPARK_COMMANDS = {"pyspark", "spark-submit"}

log = logging.getLogger(__name__)


class DeprecatedAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs="?", **kwargs):
        super().__init__(option_strings, dest, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print(
            PaastaColors.red(
                f"Use of {option_string} is deprecated. "
                + (
                    f"Please use {DEPRECATED_OPTS.get(option_string.strip('-'), '')}=value in --spark-args."
                    if option_string.strip("-") in DEPRECATED_OPTS
                    else ""
                )
            )
        )


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "spark-run",
        help="Run Spark on the PaaSTA cluster",
        description=(
            "'paasta spark-run' launches a Spark cluster on PaaSTA. "
            "It analyzes soa-configs and command line arguments to invoke "
            "a 'docker run'. By default, it will pull the Spark service "
            "image from the registry unless the --build option is used.\n\n"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Deprecated args kept to avoid failures
    # TODO: Remove these deprecated args later
    list_parser.add_argument(
        "--jars",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "--executor-memory",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "--executor-cores",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "--max-cores",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "-e",
        "--enable-compact-bin-packing",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "--enable-dra",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )
    list_parser.add_argument(
        "--force-use-eks",
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    group = list_parser.add_mutually_exclusive_group()
    group.add_argument(
        "-b",
        "--build",
        help="Build the docker image from scratch using the local Makefile's cook-image target.",
        action="store_true",
        default=False,
    )
    group.add_argument(
        "-I",
        "--image",
        help="Use the provided image to start the Spark driver and executors.",
    )
    list_parser.add_argument(
        "--docker-memory-limit",
        help=(
            "Set docker memory limit. Should be greater than driver memory. Defaults to 2x spark.driver.memory. Example: 2g, 500m, Max: 64g"
            " Note: If memory limit provided is greater than associated with the batch instance, it will default to max memory of the box."
        ),
        default=None,
    )
    list_parser.add_argument(
        "--docker-cpu-limit",
        help=(
            "Set docker cpus limit. Should be greater than driver cores. Defaults to 1x spark.driver.cores."
            " Note: The job will fail if the limit provided is greater than number of cores present on batch box (8 for production batch boxes)."
        ),
        default=None,
    )

    list_parser.add_argument(
        "--docker-shm-size",
        help=(
            "Set docker shared memory size limit for the driver's container. This is the same as setting docker run --shm-size and the shared"
            " memory is mounted to /dev/shm in the container. Anything written to the shared memory mount point counts towards the docker memory"
            " limit for the driver's container. Therefore, this should be less than --docker-memory-limit."
            f" Defaults to {DEFAULT_DOCKER_SHM_SIZE}. Example: 8g, 256m"
            " Note: this option is mainly useful when training TensorFlow models in the driver, with multiple GPUs using NCCL. The shared memory"
            f" space is used to sync gradient updates between GPUs during training. The default value of {DEFAULT_DOCKER_SHM_SIZE} is typically not large enough for"
            " this inter-gpu communication to run efficiently. We recommend a starting value of 8g to ensure that the entire set of model parameters"
            " can fit in the shared memory. This can be less if you are training a smaller model (<1g parameters) or more if you are using a larger model (>2.5g parameters)"
            " If you are observing low, average GPU utilization during epoch training (<65-70 percent) you can also try increasing this value; you may be"
            " resource constrained when GPUs sync training weights between mini-batches (there are other potential bottlenecks that could cause this as well)."
            " A tool such as nvidia-smi can be use to check GPU utilization."
            " This option also adds the --ulimit memlock=-1 to the docker run command since this is recommended for TensorFlow applications that use NCCL."
            " Please refer to docker run documentation for more details on --shm-size and --ulimit memlock=-1."
        ),
        default=None,
    )
    list_parser.add_argument(
        "--force-spark-resource-configs",
        help=(
            "Skip the resource/instances recalculation. "
            "This is strongly not recommended."
        ),
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "--docker-registry",
        help="Docker registry to push the Spark image built.",
        default=None,
    )

    list_parser.add_argument(
        "-s",
        "--service",
        help="The name of the service from which the Spark image is built.",
        default=DEFAULT_SPARK_SERVICE,
    ).completer = lazy_choices_completer(list_services)

    list_parser.add_argument(
        "-i",
        "--instance",
        help="Start a docker run for a particular instance of the service.",
        default="adhoc",
    ).completer = lazy_choices_completer(list_instances)

    try:
        system_paasta_config = load_system_paasta_config()
        valid_clusters = system_paasta_config.get_clusters()
        default_spark_cluster = system_paasta_config.get_spark_run_config().get(
            "default_cluster"
        )
        default_spark_pool = system_paasta_config.get_spark_run_config().get(
            "default_pool"
        )
    except PaastaNotConfiguredError:
        default_spark_cluster = "pnw-devc"
        default_spark_pool = "batch"
        valid_clusters = ["spark-pnw-prod", "pnw-devc"]

    list_parser.add_argument(
        "-c",
        "--cluster",
        help="The name of the cluster you wish to run Spark on.",
        choices=valid_clusters,
        default=default_spark_cluster,
    )

    list_parser.add_argument(
        "-p",
        "--pool",
        help="Name of the resource pool to run the Spark job.",
        default=default_spark_pool,
    )

    list_parser.add_argument(
        "-w",
        "--work-dir",
        default="{}:{}".format(os.getcwd(), DEFAULT_SPARK_WORK_DIR),
        help="The read-write volume to mount in format local_abs_dir:container_abs_dir",
    )

    list_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        help="A directory from which yelpsoa-configs should be read from.",
        default=DEFAULT_SOA_DIR,
    )

    list_parser.add_argument(
        "-C",
        "--cmd",
        help="Run the spark-shell, pyspark, spark-submit, jupyter-lab, or history-server command.",
    )

    list_parser.add_argument(
        "--timeout-job-runtime",
        type=str,
        help="Timeout value which will be added before spark-submit. Job will exit if it doesn't finish in given "
        "runtime. Recommended value: 2 * expected runtime. Example: 1h, 30m",
        default=DEFAULT_SPARK_RUNTIME_TIMEOUT,
    )

    list_parser.add_argument(
        "-d",
        "--dry-run",
        help="Shows the arguments supplied to docker as json.",
        action="store_true",
        default=False,
    )

    list_parser.add_argument(
        "--spark-args",
        help="Spark configurations documented in https://spark.apache.org/docs/latest/configuration.html, "
        'separated by space. For example, --spark-args "spark.executor.cores=1 spark.executor.memory=7g '
        'spark.executor.instances=2".',
    )

    list_parser.add_argument(
        "--nvidia",
        help="Use nvidia docker runtime for Spark driver process (requires GPU)",
        action="store_true",
        default=False,
    )

    list_parser.add_argument(
        "--mrjob",
        help="Pass Spark arguments to invoked command in the format expected by mrjobs",
        action="store_true",
        default=False,
    )

    list_parser.add_argument(
        "--cluster-manager",
        help="Specify which cluster manager to use. Support for certain cluster managers may be experimental",
        dest="cluster_manager",
        choices=CLUSTER_MANAGERS,
        default=CLUSTER_MANAGER_K8S,
    )

    list_parser.add_argument(
        "--tronfig",
        help="Load the Tron config yaml. Use with --job-id.",
        type=str,
        default=None,
    )

    list_parser.add_argument(
        "--job-id",
        help="Tron job id <job_name>.<action_name> in the Tronfig to run. Use wuth --tronfig.",
        type=str,
        default=None,
    )

    list_parser.add_argument(
        "--use-service-auth-token",
        help=(
            "Acquire service authentication token for the underlying instance,"
            " and set it in the container environment"
        ),
        action="store_true",
        dest="use_service_auth_token",
        required=False,
        default=False,
    )

    aws_group = list_parser.add_argument_group(
        title="AWS credentials options",
        description="If --aws-credentials-yaml is specified, it overrides all "
        "other options. Otherwise, if -s/--service is specified, spark-run "
        "looks for service credentials in /etc/boto_cfg/[service].yaml. If "
        "it does not find the service credentials or no service is "
        "specified, spark-run falls back to the boto default behavior "
        "(checking ~/.aws/credentials, ~/.boto, etc).",
    )

    aws_group.add_argument(
        "--aws-credentials-yaml",
        help="Load aws keys from the provided yaml file. The yaml file must "
        "have keys for aws_access_key_id and aws_secret_access_key.",
    )

    aws_group.add_argument(
        "--aws-profile",
        help="Name of the AWS profile to load credentials from. Only used when "
        "--aws-credentials-yaml is not specified and --service is either "
        "not specified or the service does not have credentials in "
        "/etc/boto_cfg",
    )

    aws_group.add_argument(
        "--aws-region",
        help=f"Specify an aws region. If the region is not specified, we will"
        f"default to using {DEFAULT_AWS_REGION}.",
        default=DEFAULT_AWS_REGION,
    )

    aws_group.add_argument(
        "--assume-aws-role",
        help=(
            "Takes an AWS IAM role ARN and attempts to create a session using "
            "spark_role_assumer"
        ),
    )

    aws_group.add_argument(
        "--aws-role-duration",
        help=(
            "Duration in seconds for the role if --assume-aws-role provided. "
            "The maximum is 43200, but by default, roles may only allow 3600."
        ),
        type=int,
        default=43200,
    )

    aws_group.add_argument(
        "--use-web-identity",
        help=(
            "If the current environment contains AWS_ROLE_ARN and "
            "AWS_WEB_IDENTITY_TOKEN_FILE, creates a session to use. These "
            "ENV vars must be present, and will be in the context of a pod-"
            "identity enabled pod."
        ),
        action="store_true",
        default=False,
    )

    jupyter_group = list_parser.add_argument_group(
        title="Jupyter kernel culling options",
        description="Idle kernels will be culled by default. Idle "
        "kernels with connections can be overridden not to be culled.",
    )

    jupyter_group.add_argument(
        "--cull-idle-timeout",
        type=int,
        default=7200,
        help="Timeout (in seconds) after which a kernel is considered idle and "
        "ready to be culled.",
    )

    jupyter_group.add_argument(
        "--not-cull-connected",
        action="store_true",
        default=False,
        help="By default, connected idle kernels are culled after timeout. "
        "They can be skipped if not-cull-connected is specified.",
    )

    list_parser.set_defaults(command=paasta_spark_run)


def sanitize_container_name(container_name):
    # container_name only allows [a-zA-Z0-9][a-zA-Z0-9_.-]
    return re.sub("[^a-zA-Z0-9_.-]", "_", re.sub("^[^a-zA-Z0-9]+", "", container_name))


def get_docker_run_cmd(
    container_name,
    volumes,
    env,
    docker_img,
    docker_cmd,
    nvidia,
    docker_memory_limit,
    docker_shm_size,
    docker_cpu_limit,
):
    print(
        f"Setting docker memory, shared memory, and cpu limits as {docker_memory_limit}, {docker_shm_size}, and {docker_cpu_limit} core(s) respectively."
    )
    cmd = ["paasta_docker_wrapper", "run"]
    cmd.append(f"--memory={docker_memory_limit}")
    if docker_shm_size is not None:
        cmd.append(f"--shm-size={docker_shm_size}")
        cmd.append("--ulimit")
        cmd.append("memlock=-1")
    cmd.append(f"--cpus={docker_cpu_limit}")
    cmd.append("--rm")
    cmd.append("--net=host")

    non_interactive_cmd = ["spark-submit", "history-server"]
    if not any(c in docker_cmd for c in non_interactive_cmd):
        cmd.append("--interactive=true")
        if sys.stdout.isatty():
            cmd.append("--tty=true")

    container_user = (
        # root inside container == current user outside
        (0, 0)
        if is_using_unprivileged_containers()
        else (os.geteuid(), os.getegid())
    )
    cmd.append("--user=%d:%d" % container_user)
    cmd.append("--name=%s" % sanitize_container_name(container_name))
    for k, v in env.items():
        cmd.append("--env")
        if k in SENSITIVE_ENV:
            cmd.append(k)
        else:
            cmd.append(f"{k}={v}")
    if is_using_unprivileged_containers():
        cmd.append("--env")
        cmd.append(f"HOME=/nail/home/{get_username()}")
    if nvidia:
        cmd.append("--env")
        cmd.append("NVIDIA_VISIBLE_DEVICES=all")
        cmd.append("--runtime=nvidia")
    for volume in volumes:
        cmd.append("--volume=%s" % volume)
    cmd.append("%s" % docker_img)
    cmd.extend(("sh", "-c", docker_cmd))

    return cmd


def get_docker_image(
    args: argparse.Namespace, instance_config: InstanceConfig
) -> Optional[str]:
    """
    Since the Docker image digest used to launch the Spark cluster is obtained by inspecting local
    Docker images, we need to ensure that the Docker image exists locally or is pulled in all scenarios.
    """
    # docker image is built locally then pushed
    if args.build:
        return build_and_push_docker_image(args)

    docker_url = ""
    if args.image:
        docker_url = args.image
    else:
        try:
            docker_url = instance_config.get_docker_url()
        except NoDockerImageError:
            print(
                PaastaColors.red(
                    "Error: No sha has been marked for deployment for the %s deploy group.\n"
                    "Please ensure this service has either run through a jenkins pipeline "
                    "or paasta mark-for-deployment has been run for %s\n"
                    % (instance_config.get_deploy_group(), args.service)
                ),
                sep="",
                file=sys.stderr,
            )
            return None

    print(
        "Please wait while the image (%s) is pulled (times out after 5m)..."
        % docker_url,
        file=sys.stderr,
    )
    # Need sudo for credentials when pulling images from paasta docker registry (docker-paasta.yelpcorp.com)
    # However, in CI env, we can't connect to docker via root and we can pull with user `jenkins`
    is_ci_env = "CI" in os.environ
    cmd_prefix = "" if is_ci_env else "sudo -H "
    retcode, _ = _run(f"{cmd_prefix}docker pull {docker_url}", stream=True, timeout=300)
    if retcode != 0:
        print(
            "\nPull failed. Are you authorized to run docker commands?",
            file=sys.stderr,
        )
        return None
    return docker_url


def get_smart_paasta_instance_name(args):
    if os.environ.get("TRON_JOB_NAMESPACE"):
        tron_job = os.environ.get("TRON_JOB_NAME")
        tron_action = os.environ.get("TRON_ACTION")
        return f"{tron_job}.{tron_action}"
    else:
        how_submitted = None
        if args.mrjob:
            how_submitted = "mrjob"
        else:
            for spark_cmd in SPARK_COMMANDS:
                if spark_cmd in args.cmd:
                    how_submitted = spark_cmd
                    break
        how_submitted = how_submitted or "other"
        return f"{args.instance}_{get_username()}_{how_submitted}"


def get_spark_env(
    args: argparse.Namespace,
    spark_conf_str: str,
    aws_creds: Tuple[Optional[str], Optional[str], Optional[str]],
    ui_port: str,
    system_paasta_config: SystemPaastaConfig,
) -> Dict[str, str]:
    """Create the env config dict to configure on the docker container"""

    spark_env = {}
    access_key, secret_key, session_token = aws_creds
    if access_key:
        spark_env["AWS_ACCESS_KEY_ID"] = access_key
        spark_env["AWS_SECRET_ACCESS_KEY"] = secret_key
        if session_token is not None:
            spark_env["AWS_SESSION_TOKEN"] = session_token

    spark_env["AWS_DEFAULT_REGION"] = args.aws_region
    spark_env["PAASTA_LAUNCHED_BY"] = get_possible_launched_by_user_variable_from_env()
    spark_env["PAASTA_INSTANCE_TYPE"] = "spark"

    # Run spark (and mesos framework) as root.
    spark_env["SPARK_USER"] = "root"
    spark_env["SPARK_OPTS"] = spark_conf_str

    # Default configs to start the jupyter notebook server
    if args.cmd == "jupyter-lab":
        spark_env["JUPYTER_RUNTIME_DIR"] = "/source/.jupyter"
        spark_env["JUPYTER_DATA_DIR"] = "/source/.jupyter"
        spark_env["JUPYTER_CONFIG_DIR"] = "/source/.jupyter"
    elif args.cmd == "history-server":
        dirs = args.work_dir.split(":")
        spark_env["SPARK_LOG_DIR"] = dirs[1]
        if not args.spark_args or not args.spark_args.startswith(
            "spark.history.fs.logDirectory"
        ):
            print(
                "history-server requires spark.history.fs.logDirectory in spark-args",
                file=sys.stderr,
            )
            sys.exit(1)
        spark_env["SPARK_HISTORY_OPTS"] = (
            f"-D{args.spark_args} " f"-Dspark.history.ui.port={ui_port}"
        )
        spark_env["SPARK_DAEMON_CLASSPATH"] = "/opt/spark/extra_jars/*"
        spark_env["SPARK_NO_DAEMONIZE"] = "true"

    spark_env["KUBECONFIG"] = system_paasta_config.get_spark_kubeconfig()

    return spark_env


def _parse_user_spark_args(
    spark_args: str,
) -> Dict[str, str]:

    user_spark_opts = {}
    if spark_args:
        for spark_arg in spark_args.split():
            fields = spark_arg.split("=", 1)
            if len(fields) != 2:
                print(
                    PaastaColors.red(
                        "Spark option %s is not in format option=value." % spark_arg
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)
            user_spark_opts[fields[0]] = fields[1]

    return user_spark_opts


def run_docker_container(
    container_name,
    volumes,
    environment,
    docker_img,
    docker_cmd,
    dry_run,
    nvidia,
    docker_memory_limit,
    docker_shm_size,
    docker_cpu_limit,
) -> int:

    docker_run_args = dict(
        container_name=container_name,
        volumes=volumes,
        env=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        nvidia=nvidia,
        docker_memory_limit=docker_memory_limit,
        docker_shm_size=docker_shm_size,
        docker_cpu_limit=docker_cpu_limit,
    )
    docker_run_cmd = get_docker_run_cmd(**docker_run_args)
    if dry_run:
        print(json.dumps(docker_run_cmd))
        return 0

    merged_env = {**os.environ, **environment}
    os.execlpe("paasta_docker_wrapper", *docker_run_cmd, merged_env)
    return 0


def get_spark_app_name(original_docker_cmd: Union[Any, str, List[str]]) -> str:
    """Use submitted batch name as default spark_run job name"""
    docker_cmds = (
        shlex.split(original_docker_cmd)
        if isinstance(original_docker_cmd, str)
        else original_docker_cmd
    )
    spark_app_name = None
    after_spark_submit = False
    for arg in docker_cmds:
        if arg == "spark-submit":
            after_spark_submit = True
        elif after_spark_submit and arg.endswith(".py"):
            batch_name = arg.split("/")[-1].replace(".py", "")
            spark_app_name = "paasta_" + batch_name
            break
        elif arg == "jupyter-lab":
            spark_app_name = "paasta_jupyter"
            break

    if spark_app_name is None:
        spark_app_name = "paasta_spark_run"

    spark_app_name += f"_{get_username()}"

    return spark_app_name


def _calculate_docker_memory_limit(
    spark_conf: Mapping[str, str], memory_limit: Optional[str]
) -> str:
    """In Order of preference:
    1. Argument: --docker-memory-limit
    2. --spark-args or spark-submit: spark.driver.memory
    3. Default
    """
    if memory_limit:
        return memory_limit

    try:
        docker_memory_limit_str = spark_conf.get(
            "spark.driver.memory", DEFAULT_DRIVER_MEMORY_BY_SPARK
        )
        adjustment_factor = DOCKER_RESOURCE_ADJUSTMENT_FACTOR
        match = re.match(r"([0-9]+)([a-z]*)", docker_memory_limit_str)
        memory_val = int(match[1]) * adjustment_factor
        memory_unit = match[2]
        docker_memory_limit = f"{memory_val}{memory_unit}"
    except Exception as e:
        # For any reason it fails, continue with default value
        print(
            f"ERROR: Failed to parse docker memory limit. Error: {e}. Example values: 1g, 200m."
        )
        raise

    return docker_memory_limit


def _calculate_docker_shared_memory_size(shm_size: Optional[str]) -> str:
    """In Order of preference:
    1. Argument: --docker-shm-size
    3. Default
    """
    if shm_size:
        return shm_size

    return DEFAULT_DOCKER_SHM_SIZE


def _calculate_docker_cpu_limit(
    spark_conf: Mapping[str, str], cpu_limit: Optional[str]
) -> str:
    """In Order of preference:
    1. Argument: --docker-cpu-limit
    2. --spark-args or spark-submit: spark.driver.cores
    3. Default
    """
    return (
        cpu_limit
        if cpu_limit
        else spark_conf.get("spark.driver.cores", str(DEFAULT_DRIVER_CORES_BY_SPARK))
    )


def configure_and_run_docker_container(
    args: argparse.Namespace,
    docker_img: str,
    instance_config: InstanceConfig,
    system_paasta_config: SystemPaastaConfig,
    spark_conf: Dict[str, str],
    aws_creds: Tuple[Optional[str], Optional[str], Optional[str]],
    cluster_manager: str,
    pod_template_path: str,
    extra_driver_envs: Dict[str, str] = dict(),
) -> int:
    docker_memory_limit = _calculate_docker_memory_limit(
        spark_conf, args.docker_memory_limit
    )
    docker_shm_size = _calculate_docker_shared_memory_size(args.docker_shm_size)
    docker_cpu_limit = _calculate_docker_cpu_limit(
        spark_conf,
        args.docker_cpu_limit,
    )

    if cluster_manager in {CLUSTER_MANAGER_K8S, CLUSTER_MANAGER_LOCAL}:
        # service_configuration_lib puts volumes into the k8s
        # configs for local mode
        volumes = get_volumes_from_spark_k8s_configs(spark_conf)
    else:
        raise UnsupportedClusterManagerException(cluster_manager)

    volumes.append("%s:rw" % args.work_dir)
    volumes.append("/nail/home:/nail/home:rw")

    if pod_template_path:
        volumes.append(f"{pod_template_path}:{pod_template_path}:rw")

    volumes.append(
        f"{system_paasta_config.get_spark_kubeconfig()}:{system_paasta_config.get_spark_kubeconfig()}:ro"
    )

    environment = instance_config.get_env_dictionary()  # type: ignore
    spark_conf_str = create_spark_config_str(spark_conf, is_mrjob=args.mrjob)
    environment.update(
        get_spark_env(
            args=args,
            spark_conf_str=spark_conf_str,
            aws_creds=aws_creds,
            ui_port=spark_conf["spark.ui.port"],
            system_paasta_config=system_paasta_config,
        )
    )  # type:ignore
    environment.update(extra_driver_envs)

    if args.use_service_auth_token:
        environment["YELP_SVC_AUTHZ_TOKEN"] = get_service_auth_token()

    webui_url = get_webui_url(spark_conf["spark.ui.port"])
    webui_url_msg = PaastaColors.green(f"\nSpark monitoring URL: ") + f"{webui_url}\n"

    docker_cmd = get_docker_cmd(args, instance_config, spark_conf_str)
    if "history-server" in docker_cmd:
        print(PaastaColors.green(f"\nSpark history server URL: ") + f"{webui_url}\n")
    elif any(c in docker_cmd for c in ["pyspark", "spark-shell", "spark-submit"]):
        grafana_url = get_grafana_url(spark_conf)
        dashboard_url_msg = (
            PaastaColors.green(f"\nGrafana dashboard: ") + f"{grafana_url}\n"
        )
        print(webui_url_msg)
        print(dashboard_url_msg)
        log.info(webui_url_msg)
        log.info(dashboard_url_msg)
        spark_conf_builder = spark_config.SparkConfBuilder()
        history_server_url = spark_conf_builder.get_history_url(spark_conf)
        if history_server_url:
            history_server_url_msg = (
                f"\nAfter the job is finished, you can find the spark UI from {history_server_url}\n"
                "Check y/spark-recent-history for faster access to prod logs\n"
            )
            print(history_server_url_msg)
            log.info(history_server_url_msg)
    print(f"Selected cluster manager: {cluster_manager}\n")

    if clusterman_metrics and _should_get_resource_requirements(docker_cmd, args.mrjob):
        resources = get_resources_requested(spark_conf)
        hourly_cost = get_spark_hourly_cost(
            clusterman_metrics,
            resources,
            spark_conf["spark.executorEnv.PAASTA_CLUSTER"],
            args.pool,
        )
        message = (
            f"Resource request ({resources['cpus']} cpus and {resources['mem']} MB memory total)"
            f" is estimated to cost ${hourly_cost} per hour"
        )
        if clusterman_metrics.util.costs.should_warn(hourly_cost):
            print(PaastaColors.red(f"WARNING: {message}"))
        else:
            print(message)

    return run_docker_container(
        container_name=spark_conf["spark.app.name"],
        volumes=volumes,
        environment=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        dry_run=args.dry_run,
        nvidia=args.nvidia,
        docker_memory_limit=docker_memory_limit,
        docker_shm_size=docker_shm_size,
        docker_cpu_limit=docker_cpu_limit,
    )


def _should_get_resource_requirements(docker_cmd: str, is_mrjob: bool) -> bool:
    return is_mrjob or any(
        c in docker_cmd for c in ["pyspark", "spark-shell", "spark-submit"]
    )


def get_docker_cmd(
    args: argparse.Namespace, instance_config: InstanceConfig, spark_conf_str: str
) -> str:
    original_docker_cmd = str(args.cmd or instance_config.get_cmd())

    if args.mrjob:
        return original_docker_cmd + " " + spark_conf_str
    # Default cli options to start the jupyter notebook server.
    elif original_docker_cmd == "jupyter-lab":
        cull_opts = (
            "--MappingKernelManager.cull_idle_timeout=%s " % args.cull_idle_timeout
        )
        if args.not_cull_connected is False:
            cull_opts += "--MappingKernelManager.cull_connected=True "

        return "SHELL=bash USER={} /source/virtualenv_run_jupyter/bin/jupyter-lab -y --ip={} {}".format(
            get_username(), socket.getfqdn(), cull_opts
        )
    elif original_docker_cmd == "history-server":
        return "start-history-server.sh"
    # Spark options are passed as options to pyspark and spark-shell.
    # For jupyter, environment variable SPARK_OPTS is set instead.
    else:
        return inject_spark_conf_str(original_docker_cmd, spark_conf_str)


def _get_adhoc_docker_registry(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> str:
    if service is None:
        raise NotImplementedError('"None" is not a valid service')

    service_configuration = read_service_configuration(service, soa_dir)
    return service_configuration.get("docker_registry", DEFAULT_SPARK_DOCKER_REGISTRY)


def build_and_push_docker_image(args: argparse.Namespace) -> Optional[str]:
    """
    Build an image if the default Spark service image is not preferred.
    The image needs to be pushed to a registry for the Spark executors
    to pull.
    """
    if not makefile_responds_to("cook-image"):
        print(
            "A local Makefile with a 'cook-image' target is required for --build",
            file=sys.stderr,
        )
        return None

    default_tag = "{}-{}".format(DEFAULT_SPARK_DOCKER_IMAGE_PREFIX, get_username())
    docker_tag = os.environ.get("DOCKER_TAG", default_tag)
    os.environ["DOCKER_TAG"] = docker_tag

    cook_return = paasta_cook_image(
        args=None, service=args.service, soa_dir=args.yelpsoa_config_root
    )
    if cook_return != 0:
        return None

    registry_uri = args.docker_registry or _get_adhoc_docker_registry(
        service=args.service,
        soa_dir=args.yelpsoa_config_root,
    )

    docker_url = f"{registry_uri}/{docker_tag}"
    command = f"docker tag {docker_tag} {docker_url}"
    print(PaastaColors.grey(command))
    retcode, _ = _run(command, stream=True)
    if retcode != 0:
        return None

    if registry_uri != DEFAULT_SPARK_DOCKER_REGISTRY:
        command = "sudo -H docker push %s" % docker_url
    else:
        command = "docker push %s" % docker_url

    print(PaastaColors.grey(command))
    retcode, output = _run(command, stream=False)
    if retcode != 0:
        return None

    # With unprivileged docker, the digest on the remote registry may not match the digest
    # in the local environment. Because of this, we have to parse the digest message from the
    # server response and use downstream when launching spark executors

    # Output from `docker push` with unprivileged docker looks like
    #  Using default tag: latest
    #  The push refers to repository [docker-dev.yelpcorp.com/paasta-spark-run-dpopes:latest]
    #  latest: digest: sha256:0a43aa65174a400bd280d48d460b73eb49b0ded4072c9e173f919543bf693557

    # With privileged docker, the last line has an extra "size: 123"
    #  latest: digest: sha256:0a43aa65174a400bd280d48d460b73eb49b0ded4072c9e173f919543bf693557 size: 52

    digest_line = output.split("\n")[-1]
    digest_match = re.match(r"[^:]*: [^:]*: (?P<digest>[^\s]*)", digest_line)
    if not digest_match:
        raise ValueError(f"Could not determine digest from output: {output}")
    digest = digest_match.group("digest")

    image_url = f"{docker_url}@{digest}"

    # If the local digest doesn't match the remote digest AND the registry is
    # non-default (which requires requires authentication, and consequently sudo),
    # downstream `docker run` commands will fail trying to authenticate.
    # To work around this, we can proactively `sudo docker pull` here so that
    # the image exists locally and can be `docker run` without sudo
    if registry_uri != DEFAULT_SPARK_DOCKER_REGISTRY:
        command = f"sudo -H docker pull {image_url}"
        print(PaastaColors.grey(command))
        retcode, output = _run(command, stream=False)
        if retcode != 0:
            raise NoDockerImageError(f"Could not pull {image_url}: {output}")

    return image_url


def validate_work_dir(s):
    dirs = s.split(":")
    if len(dirs) != 2:
        print(
            "work-dir %s is not in format local_abs_dir:container_abs_dir" % s,
            file=sys.stderr,
        )
        sys.exit(1)

    for d in dirs:
        if not os.path.isabs(d):
            print("%s is not an absolute path" % d, file=sys.stderr)
            sys.exit(1)


def parse_tronfig(tronfig_path: str, job_id: str) -> Optional[Dict[str, Any]]:
    splitted = job_id.split(".")
    if len(splitted) != 2:
        return None
    job_name, action_name = splitted

    file_content = read_yaml_file(tronfig_path)
    jobs = filter_templates_from_config(file_content)
    if job_name not in jobs or action_name not in jobs[job_name].get("actions", {}):
        return None
    return jobs[job_name]["actions"][action_name]


def update_args_from_tronfig(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    """
    Load and check the following config fields from the provided Tronfig.
      - executor
      - pool
      - iam_role
      - iam_role_provider
      - force_spark_resource_configs
      - max_runtime
      - command
      - env
      - spark_args

    Returns: environment variables dictionary or None if failed.
    """
    action_dict = parse_tronfig(args.tronfig, args.job_id)
    if action_dict is None:
        print(
            PaastaColors.red(f"Unable to get configs from job-id: {args.job_id}"),
            file=sys.stderr,
        )
        return None

    # executor === spark
    if action_dict.get("executor", "") != "spark":
        print(
            PaastaColors.red("Invalid Tronfig: executor should be 'spark'"),
            file=sys.stderr,
        )
        return None

    # iam_role / aws_profile
    if (
        "iam_role" in action_dict
        and action_dict.get("iam_role_provider", "aws") != "aws"
    ):
        print(
            PaastaColors.red("Invalid Tronfig: iam_role_provider should be 'aws'"),
            file=sys.stderr,
        )
        return None

    # Other args: map Tronfig YAML fields to spark-run CLI args
    fields_to_args = {
        "pool": "pool",
        "iam_role": "assume_aws_role",
        "force_spark_resource_configs": "force_spark_resource_configs",
        "max_runtime": "timeout_job_runtime",
        "command": "cmd",
        "spark_args": "spark_args",
    }
    for field_name, arg_name in fields_to_args.items():
        if field_name in action_dict:
            value = action_dict[field_name]

            # Convert spark_args values from dict to a string "k1=v1 k2=v2"
            if field_name == "spark_args":
                value = " ".join([f"{k}={v}" for k, v in dict(value).items()])

            # Beautify for printing
            arg_name_str = (f"--{arg_name.replace('_', '-')}").ljust(31, " ")

            # Only load iam_role value if --aws-profile is not set
            if field_name == "iam_role" and args.aws_profile is not None:
                print(
                    PaastaColors.yellow(
                        f"Ignoring Tronfig: `{field_name} : {value}`, since `--aws-profile` is provided. "
                        f"We are giving higher priority to `--aws-profile` in case of paasta spark-run adhoc runs."
                    ),
                )
                continue

            if hasattr(args, arg_name):
                print(
                    PaastaColors.yellow(
                        f"Overwriting args with Tronfig: {arg_name_str} => {field_name} : {value}"
                    ),
                )
            setattr(args, arg_name, value)

    # env (currently paasta spark-run does not support Spark driver secrets environment variables)
    return action_dict.get("env", dict())


def paasta_spark_run(args: argparse.Namespace) -> int:
    driver_envs_from_tronfig: Dict[str, str] = dict()
    if args.tronfig is not None:
        if args.job_id is None:
            print(
                PaastaColors.red("Missing --job-id when --tronfig is provided"),
                file=sys.stderr,
            )
            return False
        driver_envs_from_tronfig = update_args_from_tronfig(args)
        if driver_envs_from_tronfig is None:
            return False

    # argparse does not work as expected with both default and
    # type=validate_work_dir.
    validate_work_dir(args.work_dir)

    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and local-run may not behave"
                "the same way it would behave on a server configured for PaaSTA."
            ),
            sep="\n",
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, "/etc/paasta")

    if args.cmd == "jupyter-lab" and not args.build and not args.image:
        print(
            PaastaColors.red(
                "The jupyter-lab command requires a prebuilt image with -I or --image."
            ),
            file=sys.stderr,
        )
        return 1

    # validate pool
    try:
        if not validate_pool(args.cluster, args.pool, system_paasta_config):
            print(
                PaastaColors.red(
                    f"Invalid --pool value. List of valid pools for cluster `{args.cluster}`: "
                    f"{system_paasta_config.get_pools_for_cluster(args.cluster)}"
                ),
                file=sys.stderr,
            )
            return 1
    except PoolsNotConfiguredError:
        log.warning(
            PaastaColors.yellow(
                f"Could not fetch allowed_pools for `{args.cluster}`. Skipping pool validation.\n"
            )
        )

    # annoyingly, there's two layers of aliases: one for the soaconfigs to read from
    # (that's this alias lookup) - and then another layer later when figuring out what
    # k8s server url to use ;_;
    cluster = system_paasta_config.get_cluster_aliases().get(args.cluster, args.cluster)
    # Use the default spark:client instance configs if not provided
    try:
        instance_config = get_instance_config(
            service=args.service,
            instance=args.instance,
            cluster=cluster,
            load_deployments=args.build is False and args.image is None,
            soa_dir=args.yelpsoa_config_root,
        )
    except NoConfigurationForServiceError as e:
        print(str(e), file=sys.stderr)
        return 1
    except NoDeploymentsAvailable:
        print(
            PaastaColors.red(
                "Error: No deployments.json found in %(soa_dir)s/%(service)s."
                "You can generate this by running:"
                "generate_deployments_for_service -d %(soa_dir)s -s %(service)s"
                % {"soa_dir": args.yelpsoa_config_root, "service": args.service}
            ),
            sep="\n",
            file=sys.stderr,
        )
        return 1

    if not args.cmd and not instance_config.get_cmd():
        print(
            "A command is required, pyspark, spark-shell, spark-submit or jupyter",
            file=sys.stderr,
        )
        return 1

    aws_creds = get_aws_credentials(
        service=args.service,
        aws_credentials_yaml=args.aws_credentials_yaml,
        profile_name=args.aws_profile,
        assume_aws_role_arn=args.assume_aws_role,
        session_duration=args.aws_role_duration,
        use_web_identity=args.use_web_identity,
    )
    docker_image_digest = get_docker_image(args, instance_config)
    if docker_image_digest is None:
        return 1

    volumes = instance_config.get_volumes(
        system_paasta_config.get_volumes(),
        system_paasta_config.get_uses_bulkdata_default(),
    )
    app_base_name = get_spark_app_name(args.cmd or instance_config.get_cmd())

    user_spark_opts = _parse_user_spark_args(args.spark_args)

    args.cmd = auto_add_timeout_for_spark_job(args.cmd, args.timeout_job_runtime)

    # This is required if configs are provided as part of `spark-submit`
    # Other way to provide is with --spark-args
    sub_cmds = args.cmd.split(" ")  # spark.driver.memory=10g
    for cmd in sub_cmds:
        if cmd.startswith("spark.driver.memory") or cmd.startswith(
            "spark.driver.cores"
        ):
            key, value = cmd.split("=")
            user_spark_opts[key] = value

    paasta_instance = get_smart_paasta_instance_name(args)

    k8s_server_address = get_k8s_url_for_cluster(args.cluster)
    paasta_cluster = system_paasta_config.get_eks_cluster_aliases().get(
        args.cluster, args.cluster
    )

    spark_conf_builder = spark_config.SparkConfBuilder()
    spark_conf = spark_conf_builder.get_spark_conf(
        cluster_manager=args.cluster_manager,
        spark_app_base_name=app_base_name,
        docker_img=docker_image_digest,
        user_spark_opts=user_spark_opts,
        paasta_cluster=paasta_cluster,
        paasta_pool=args.pool,
        paasta_service=args.service,
        paasta_instance=paasta_instance,
        extra_volumes=volumes,
        aws_creds=aws_creds,
        aws_region=args.aws_region,
        force_spark_resource_configs=args.force_spark_resource_configs,
        use_eks=True,
        k8s_server_address=k8s_server_address,
    )

    return configure_and_run_docker_container(
        args,
        docker_img=docker_image_digest,
        instance_config=instance_config,
        system_paasta_config=system_paasta_config,
        spark_conf=spark_conf,
        aws_creds=aws_creds,
        cluster_manager=args.cluster_manager,
        pod_template_path=spark_conf.get(
            "spark.kubernetes.executor.podTemplateFile", ""
        ),
        extra_driver_envs=driver_envs_from_tronfig,
    )
