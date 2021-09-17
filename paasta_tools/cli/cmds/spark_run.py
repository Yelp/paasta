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

import yaml
from boto3.exceptions import Boto3Error
from service_configuration_lib.spark_config import get_aws_credentials
from service_configuration_lib.spark_config import get_history_url
from service_configuration_lib.spark_config import get_signalfx_url
from service_configuration_lib.spark_config import get_spark_conf
from service_configuration_lib.spark_config import send_and_calculate_resources_cost

from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.spark_tools import DEFAULT_SPARK_SERVICE
from paasta_tools.spark_tools import get_webui_url
from paasta_tools.spark_tools import inject_spark_conf_str
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_possible_launched_by_user_variable_from_env
from paasta_tools.utils import get_username
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig


DEFAULT_AWS_REGION = "us-west-2"
DEFAULT_SPARK_WORK_DIR = "/spark_driver"
DEFAULT_SPARK_DOCKER_IMAGE_PREFIX = "paasta-spark-run"
DEFAULT_SPARK_DOCKER_REGISTRY = "docker-dev.yelpcorp.com"
SENSITIVE_ENV = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
clusterman_metrics, CLUSTERMAN_YAML_FILE_PATH = get_clusterman_metrics()
CLUSTER_MANAGER_MESOS = "mesos"
CLUSTER_MANAGER_K8S = "kubernetes"
CLUSTER_MANAGERS = {CLUSTER_MANAGER_MESOS, CLUSTER_MANAGER_K8S}

POD_TEMPLATE_PATH = "/nail/tmp/podTemplate.yaml"

POD_TEMPLATE = """
apiVersion: v1
kind: Pod
metadata:
  labels:
    spark: exec-{spark_app_name}
spec:
  affinity:
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 95
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: spark
              operator: In
              values:
              - exec-{spark_app_name}
          topologyKey: topology.kubernetes.io/hostname
"""

deprecated_opts = {
    "j": "spark.jars",
    "jars": "spark.jars",
    "max-cores": "spark.cores.max",
    "executor-cores": "spark.executor.cores",
    "executor-memory": "spark.executor.memory",
    "driver-max-result-size": "spark.driver.maxResultSize",
    "driver-cores": "spark.driver.cores",
    "driver-memory": "spark.driver.memory",
}

SPARK_COMMANDS = {"pyspark", "spark-submit"}

log = logging.getLogger(__name__)


class DeprecatedAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        print(
            PaastaColors.red(
                "Use of {} is deprecated. Please use {}=value in --spark-args.".format(
                    option_string, deprecated_opts[option_string.strip("-")]
                )
            )
        )
        sys.exit(1)


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
        "-e",
        "--enable-k8s-autogen",
        help="Auto generate pod template with weighted pod affinity used in executors.",
        action="store_true",
        default=True,
    )
    list_parser.add_argument(
        "--docker-registry",
        help="Docker registry to push the Spark image built.",
        default=DEFAULT_SPARK_DOCKER_REGISTRY,
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
        help=("Start a docker run for a particular instance of the service."),
        default="adhoc",
    ).completer = lazy_choices_completer(list_instances)

    try:
        system_paasta_config = load_system_paasta_config()
        default_spark_cluster = system_paasta_config.get_spark_run_config().get(
            "default_cluster"
        )
        default_spark_pool = system_paasta_config.get_spark_run_config().get(
            "default_pool"
        )
    except PaastaNotConfiguredError:
        default_spark_cluster = "pnw-devc"
        default_spark_pool = "batch"

    list_parser.add_argument(
        "-c",
        "--cluster",
        help=("The name of the cluster you wish to run Spark on."),
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
        "-d",
        "--dry-run",
        help="Shows the arguments supplied to docker as json.",
        action="store_true",
        default=False,
    )

    list_parser.add_argument(
        "--spark-args",
        help="Spark configurations documented in https://spark.apache.org/docs/latest/configuration.html. "
        r'For example, --spark-args "spark.mesos.constraints=pool:default\;instance_type:m4.10xlarge '
        'spark.executor.cores=4".',
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
        default=CLUSTER_MANAGER_MESOS,
    )

    if clusterman_metrics:
        list_parser.add_argument(
            "--suppress-clusterman-metrics-errors",
            help="Continue even if sending resource requirements to Clusterman fails. This may result in the job "
            "failing to acquire resources.",
            action="store_true",
        )

    list_parser.add_argument(
        "-j", "--jars", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--executor-memory", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--executor-cores", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--max-cores", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--driver-max-result-size", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--driver-memory", help=argparse.SUPPRESS, action=DeprecatedAction
    )

    list_parser.add_argument(
        "--driver-cores", help=argparse.SUPPRESS, action=DeprecatedAction
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
        default="default",
    )

    aws_group.add_argument(
        "--no-aws-credentials",
        help="Do not load any AWS credentials; allow the Spark job to use its "
        "own logic to load credentials",
        action="store_true",
        default=False,
    )

    aws_group.add_argument(
        "--aws-region",
        help=f"Specify an aws region. If the region is not specified, we will"
        f"default to using {DEFAULT_AWS_REGION}.",
        default=DEFAULT_AWS_REGION,
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


def get_docker_run_cmd(container_name, volumes, env, docker_img, docker_cmd, nvidia):
    cmd = ["paasta_docker_wrapper", "run"]
    cmd.append("--rm")
    cmd.append("--net=host")

    sensitive_env = {}

    non_interactive_cmd = ["spark-submit", "history-server"]
    if not any(c in docker_cmd for c in non_interactive_cmd):
        cmd.append("--interactive=true")
        if sys.stdout.isatty():
            cmd.append("--tty=true")

    cmd.append("--user=%d:%d" % (os.geteuid(), os.getegid()))
    cmd.append("--name=%s" % sanitize_container_name(container_name))
    for k, v in env.items():
        cmd.append("--env")
        if k in SENSITIVE_ENV:
            sensitive_env[k] = v
            cmd.append(k)
        else:
            cmd.append(f"{k}={v}")
    if nvidia:
        cmd.append("--env")
        cmd.append("NVIDIA_VISIBLE_DEVICES=all")
        cmd.append("--runtime=nvidia")
    for volume in volumes:
        cmd.append("--volume=%s" % volume)
    cmd.append("%s" % docker_img)
    cmd.extend(("sh", "-c", docker_cmd))
    cmd.append(sensitive_env)

    return cmd


def get_docker_image(args, instance_config):
    if args.build:
        return build_and_push_docker_image(args)
    if args.image:
        return args.image

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
    retcode, _ = _run("sudo -H docker pull %s" % docker_url, stream=True, timeout=300)
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
) -> Dict[str, str]:
    """Create the env config dict to configure on the docker container"""

    spark_env = {}

    access_key, secret_key, session_token = aws_creds
    if access_key:
        spark_env["AWS_ACCESS_KEY_ID"] = access_key
        spark_env["AWS_SECRET_ACCESS_KEY"] = secret_key
        spark_env["AWS_DEFAULT_REGION"] = args.aws_region
        if session_token is not None:
            spark_env["AWS_SESSION_TOKEN"] = session_token
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

    return spark_env


def _parse_user_spark_args(
    spark_args: Optional[str], enable_k8s_autogen: bool = True
) -> Dict[str, str]:
    if not spark_args:
        return {}

    user_spark_opts = {}
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

    if enable_k8s_autogen:
        user_spark_opts["spark.kubernetes.executor.podTemplateFile"] = POD_TEMPLATE_PATH

    return user_spark_opts


def create_spark_config_str(spark_config_dict, is_mrjob):
    conf_option = "--jobconf" if is_mrjob else "--conf"
    spark_config_entries = list()

    if is_mrjob:
        spark_master = spark_config_dict["spark.master"]
        spark_config_entries.append(f"--spark-master={spark_master}")

    for opt, val in spark_config_dict.items():
        # mrjob use separate options to configure master
        if is_mrjob and opt == "spark.master":
            continue
        spark_config_entries.append(f"{conf_option} {opt}={val}")
    return " ".join(spark_config_entries)


def run_docker_container(
    container_name, volumes, environment, docker_img, docker_cmd, dry_run, nvidia
) -> int:

    docker_run_args = dict(
        container_name=container_name,
        volumes=volumes,
        env=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        nvidia=nvidia,
    )
    docker_run_cmd = get_docker_run_cmd(**docker_run_args)

    if dry_run:
        print(json.dumps(docker_run_cmd))
        return 0

    os.execlpe("paasta_docker_wrapper", *docker_run_cmd)
    return 0


def get_spark_app_name(
    original_docker_cmd: Union[Any, str, List[str]], enable_k8s_autogen: bool = True
) -> str:
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
    if enable_k8s_autogen:
        document = POD_TEMPLATE.format(spark_app_name=spark_app_name)
        parsed_pod_template = yaml.load(document)
        with open(POD_TEMPLATE_PATH, "w") as f:
            yaml.dump(parsed_pod_template, f)

    return spark_app_name


def configure_and_run_docker_container(
    args: argparse.Namespace,
    docker_img: str,
    instance_config: InstanceConfig,
    system_paasta_config: SystemPaastaConfig,
    spark_conf: Mapping[str, str],
    aws_creds: Tuple[Optional[str], Optional[str], Optional[str]],
    cluster_manager: str,
) -> int:

    # driver specific volumes
    volumes: List[str] = []
    if cluster_manager == CLUSTER_MANAGER_MESOS:
        volumes = (
            spark_conf.get("spark.mesos.executor.docker.volumes", "").split(",")
            if spark_conf.get("spark.mesos.executor.docker.volumes", "") != ""
            else []
        )
    elif cluster_manager == CLUSTER_MANAGER_K8S:
        volume_names = [
            re.match(
                r"spark.kubernetes.executor.volumes.hostPath.(\d+).mount.path", key
            ).group(1)
            for key in spark_conf.keys()
            if "spark.kubernetes.executor.volumes.hostPath." in key
            and ".mount.path" in key
        ]
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

    volumes.append("%s:rw" % args.work_dir)
    volumes.append("/nail/home:/nail/home:rw")
    if args.enable_k8s_autogen:
        volumes.append(f"{POD_TEMPLATE_PATH}:{POD_TEMPLATE_PATH}:rw")

    environment = instance_config.get_env_dictionary()  # type: ignore
    spark_conf_str = create_spark_config_str(spark_conf, is_mrjob=args.mrjob)
    environment.update(
        get_spark_env(args, spark_conf_str, aws_creds, spark_conf["spark.ui.port"])
    )  # type:ignore

    webui_url = get_webui_url(spark_conf["spark.ui.port"])

    docker_cmd = get_docker_cmd(args, instance_config, spark_conf_str)
    if "history-server" in docker_cmd:
        print(f"\nSpark history server URL {webui_url}\n")
    elif any(c in docker_cmd for c in ["pyspark", "spark-shell", "spark-submit"]):
        signalfx_url = get_signalfx_url(spark_conf)
        print(f"\nSpark monitoring URL {webui_url}\n")
        print(f"\nSignalfx dashboard: {signalfx_url}\n")
        history_server_url = get_history_url(spark_conf)
        if history_server_url:
            print(
                f"\nAfter the job is finished, you can find the spark UI from {history_server_url}\n"
            )
    print(f"Selected cluster manager: {cluster_manager}\n")

    if clusterman_metrics and _should_emit_resource_requirements(
        docker_cmd, args.mrjob
    ):
        try:
            print("Sending resource request metrics to Clusterman")
            hourly_cost, resources = send_and_calculate_resources_cost(
                clusterman_metrics, spark_conf, webui_url, args.pool
            )
            message = (
                f"Resource request ({resources['cpus']} cpus and {resources['mem']} MB memory total)"
                f" is estimated to cost ${hourly_cost} per hour"
            )
            if clusterman_metrics.util.costs.should_warn(hourly_cost):
                print(PaastaColors.red(f"WARNING: {message}"))
            else:
                print(message)
        except Boto3Error as e:
            print(
                PaastaColors.red(
                    f"Encountered {e} while attempting to send resource requirements to Clusterman."
                )
            )
            if args.suppress_clusterman_metrics_errors:
                print(
                    "Continuing anyway since --suppress-clusterman-metrics-errors was passed"
                )
            else:
                raise

    return run_docker_container(
        container_name=spark_conf["spark.app.name"],
        volumes=volumes,
        environment=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        dry_run=args.dry_run,
        nvidia=args.nvidia,
    )


def _should_emit_resource_requirements(docker_cmd, is_mrjob):
    return is_mrjob or any(
        c in docker_cmd for c in ["pyspark", "spark-shell", "spark-submit"]
    )


def get_docker_cmd(args, instance_config, spark_conf_str):
    original_docker_cmd = args.cmd or instance_config.get_cmd()

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


def build_and_push_docker_image(args):
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

    docker_url = f"{args.docker_registry}/{docker_tag}"
    command = f"docker tag {docker_tag} {docker_url}"
    print(PaastaColors.grey(command))
    retcode, _ = _run(command, stream=True)
    if retcode != 0:
        return None

    if args.docker_registry != DEFAULT_SPARK_DOCKER_REGISTRY:
        command = "sudo -H docker push %s" % docker_url
    else:
        command = "docker push %s" % docker_url

    print(PaastaColors.grey(command))
    retcode, output = _run(command, stream=True)
    if retcode != 0:
        return None

    return docker_url


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


def paasta_spark_run(args):
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

    # Use the default spark:client instance configs if not provided
    try:
        instance_config = get_instance_config(
            service=args.service,
            instance=args.instance,
            cluster=args.cluster,
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
        no_aws_credentials=args.no_aws_credentials,
        aws_credentials_yaml=args.aws_credentials_yaml,
        profile_name=args.aws_profile,
    )
    docker_image = get_docker_image(args, instance_config)
    if docker_image is None:
        return 1

    volumes = instance_config.get_volumes(system_paasta_config.get_volumes())
    app_base_name = get_spark_app_name(
        args.cmd or instance_config.get_cmd(), args.enable_k8s_autogen
    )

    needs_docker_cfg = not args.build
    user_spark_opts = _parse_user_spark_args(args.spark_args, args.enable_k8s_autogen)
    paasta_instance = get_smart_paasta_instance_name(args)
    spark_conf = get_spark_conf(
        cluster_manager=args.cluster_manager,
        spark_app_base_name=app_base_name,
        docker_img=docker_image,
        user_spark_opts=user_spark_opts,
        paasta_cluster=args.cluster,
        paasta_pool=args.pool,
        paasta_service=args.service,
        paasta_instance=paasta_instance,
        extra_volumes=volumes,
        aws_creds=aws_creds,
        needs_docker_cfg=needs_docker_cfg,
    )
    return configure_and_run_docker_container(
        args,
        docker_img=docker_image,
        instance_config=instance_config,
        system_paasta_config=system_paasta_config,
        spark_conf=spark_conf,
        aws_creds=aws_creds,
        cluster_manager=args.cluster_manager,
    )
