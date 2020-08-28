import argparse
import json
import logging
import os
import re
import shlex
import socket
import sys
import time
from typing import Any
from typing import List
from typing import Mapping
from typing import Union

from boto3.exceptions import Boto3Error
from ruamel.yaml import YAML

from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import pick_random_port
from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.mesos_tools import find_mesos_leader
from paasta_tools.mesos_tools import MesosLeaderUnavailable
from paasta_tools.spark_tools import DEFAULT_SPARK_SERVICE
from paasta_tools.spark_tools import get_aws_credentials
from paasta_tools.spark_tools import get_default_event_log_dir
from paasta_tools.spark_tools import get_spark_resource_requirements
from paasta_tools.spark_tools import get_webui_url
from paasta_tools.spark_tools import inject_spark_conf_str
from paasta_tools.spark_tools import load_mesos_secret_for_spark
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
SENSITIVE_ENV = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
clusterman_metrics, CLUSTERMAN_YAML_FILE_PATH = get_clusterman_metrics()


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


def get_spark_env(args, spark_conf, spark_ui_port, access_key, secret_key):
    spark_env = {}

    if access_key is not None:
        spark_env["AWS_ACCESS_KEY_ID"] = access_key
        spark_env["AWS_SECRET_ACCESS_KEY"] = secret_key
        spark_env["AWS_DEFAULT_REGION"] = args.aws_region
    spark_env["PAASTA_LAUNCHED_BY"] = get_possible_launched_by_user_variable_from_env()
    spark_env["PAASTA_INSTANCE_TYPE"] = "spark"

    # Run spark (and mesos framework) as root.
    spark_env["SPARK_USER"] = "root"
    spark_env["SPARK_OPTS"] = spark_conf

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
        spark_env["SPARK_HISTORY_OPTS"] = "-D%s -Dspark.history.ui.port=%d" % (
            args.spark_args,
            spark_ui_port,
        )
        spark_env["SPARK_DAEMON_CLASSPATH"] = "/opt/spark/extra_jars/*"
        spark_env["SPARK_NO_DAEMONIZE"] = "true"

    return spark_env


def get_spark_config(
    args,
    spark_app_name,
    spark_ui_port,
    docker_img,
    system_paasta_config,
    volumes,
    access_key,
    secret_key,
    session_token,
):
    # User configurable Spark options
    user_args = {
        "spark.app.name": spark_app_name,
        "spark.cores.max": "4",
        "spark.executor.cores": "2",
        "spark.executor.memory": "4g",
        # Use \; for multiple constraints. e.g.
        # instance_type:m4.10xlarge\;pool:default
        "spark.mesos.constraints": "pool:%s" % args.pool,
        "spark.mesos.executor.docker.forcePullImage": "true",
        "spark.mesos.role": "spark",
    }

    default_event_log_dir = get_default_event_log_dir(
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
    )
    if default_event_log_dir is not None:
        user_args["spark.eventLog.enabled"] = "true"
        user_args["spark.eventLog.dir"] = default_event_log_dir

    try:
        mesos_address = find_mesos_leader(args.cluster)
    except MesosLeaderUnavailable as e:
        print(
            f"Couldn't reach the {args.cluster} Mesos leader from here. Please run this command from the environment that matches {args.cluster}.\nError: {e}",
            file=sys.stderr,
        )
        sys.exit(2)
    # Spark options managed by PaaSTA
    paasta_instance = get_smart_paasta_instance_name(args)
    non_user_args = {
        "spark.master": "mesos://%s" % mesos_address,
        "spark.ui.port": spark_ui_port,
        "spark.executorEnv.PAASTA_SERVICE": args.service,
        "spark.executorEnv.PAASTA_INSTANCE": paasta_instance,
        "spark.executorEnv.PAASTA_CLUSTER": args.cluster,
        "spark.executorEnv.PAASTA_INSTANCE_TYPE": "spark",
        "spark.mesos.executor.docker.parameters": f"label=paasta_service={args.service},label=paasta_instance={paasta_instance}",
        "spark.mesos.executor.docker.volumes": ",".join(volumes),
        "spark.mesos.executor.docker.image": docker_img,
        "spark.mesos.principal": "spark",
        "spark.mesos.secret": load_mesos_secret_for_spark(),
    }

    if not args.build and not args.image:
        non_user_args["spark.mesos.uris"] = "file:///root/.dockercfg"

    if args.spark_args:
        spark_args = args.spark_args.split()
        for spark_arg in spark_args:
            fields = spark_arg.split("=", 1)
            if len(fields) != 2:
                print(
                    PaastaColors.red(
                        "Spark option %s is not in format option=value." % spark_arg
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)

            if fields[0] in non_user_args:
                print(
                    PaastaColors.red(
                        "Spark option {} is set by PaaSTA with {}.".format(
                            fields[0], non_user_args[fields[0]]
                        )
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)
            # Update default configuration
            user_args[fields[0]] = fields[1]

    if "spark.sql.shuffle.partitions" not in user_args:
        num_partitions = str(2 * int(user_args["spark.cores.max"]))
        user_args["spark.sql.shuffle.partitions"] = num_partitions
        print(
            PaastaColors.yellow(
                f"Warning: spark.sql.shuffle.partitions has been set to"
                f" {num_partitions} to be equal to twice the number of "
                f"requested cores, but you should consider setting a "
                f"higher value if necessary."
            )
        )

    if int(user_args["spark.cores.max"]) < int(user_args["spark.executor.cores"]):
        print(
            PaastaColors.red(
                "Total number of cores {} is less than per-executor cores {}.".format(
                    user_args["spark.cores.max"], user_args["spark.executor.cores"]
                )
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    exec_mem = user_args["spark.executor.memory"]
    if exec_mem[-1] != "g" or not exec_mem[:-1].isdigit() or int(exec_mem[:-1]) > 32:
        print(
            PaastaColors.red(
                "Executor memory {} not in format dg (d<=32).".format(
                    user_args["spark.executor.memory"]
                )
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    # Limit a container's cpu usage
    non_user_args["spark.mesos.executor.docker.parameters"] += ",cpus={}".format(
        user_args["spark.executor.cores"]
    )

    return dict(non_user_args, **user_args)


def create_spark_config_str(spark_config_dict, is_mrjob):
    conf_option = "--jobconf" if is_mrjob else "--conf"
    spark_config_entries = list()

    if is_mrjob:
        spark_master = spark_config_dict.pop("spark.master")
        spark_config_entries.append(f"--spark-master={spark_master}")

    for opt, val in spark_config_dict.items():
        spark_config_entries.append(f"{conf_option} {opt}={val}")
    return " ".join(spark_config_entries)


def emit_resource_requirements(spark_config_dict, paasta_cluster, webui_url):
    print("Sending resource request metrics to Clusterman")

    desired_resources = get_spark_resource_requirements(spark_config_dict, webui_url)
    constraints = parse_constraints_string(spark_config_dict["spark.mesos.constraints"])
    pool = constraints["pool"]

    aws_region = get_aws_region_for_paasta_cluster(paasta_cluster)
    metrics_client = clusterman_metrics.ClustermanMetricsBotoClient(
        region_name=aws_region, app_identifier=pool
    )

    cpus = desired_resources["cpus"][1]
    mem = desired_resources["mem"][1]
    est_cost = clusterman_metrics.util.costs.estimate_cost_per_hour(
        cluster=paasta_cluster, pool=pool, cpus=cpus, mem=mem,
    )
    message = f"Resource request ({cpus} cpus and {mem} MB memory total) is estimated to cost ${est_cost} per hour"
    if clusterman_metrics.util.costs.should_warn(est_cost):
        message = "WARNING: " + message
        print(PaastaColors.red(message))
    else:
        print(message)

    with metrics_client.get_writer(
        clusterman_metrics.APP_METRICS, aggregate_meteorite_dims=True
    ) as writer:
        for _, (metric_key, desired_quantity) in desired_resources.items():
            writer.send((metric_key, int(time.time()), desired_quantity))


def get_aws_region_for_paasta_cluster(paasta_cluster: str) -> str:
    with open(CLUSTERMAN_YAML_FILE_PATH, "r") as clusterman_yaml_file:
        clusterman_yaml = YAML().load(clusterman_yaml_file.read())
        return clusterman_yaml["clusters"][paasta_cluster]["aws_region"]


def parse_constraints_string(constraints_string: str) -> Mapping[str, str]:
    constraints = {}
    for constraint in constraints_string.split(";"):
        if constraint[-1] == "\\":
            constraint = constraint[:-1]
        k, v = constraint.split(":")
        constraints[k] = v

    return constraints


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
    original_docker_cmd: Union[Any, str, List[str]], spark_ui_port: int
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

    spark_app_name += "_{}_{}".format(get_username(), spark_ui_port)
    return spark_app_name


def configure_and_run_docker_container(
    args: argparse.Namespace,
    docker_img: str,
    instance_config: InstanceConfig,
    system_paasta_config: SystemPaastaConfig,
) -> int:
    volumes = list()
    for volume in instance_config.get_volumes(system_paasta_config.get_volumes()):
        if os.path.exists(volume["hostPath"]):
            volumes.append(
                "{}:{}:{}".format(
                    volume["hostPath"], volume["containerPath"], volume["mode"].lower()
                )
            )
        else:
            print(
                PaastaColors.yellow(
                    "Warning: Path %s does not exist on this host. Skipping this binding."
                    % volume["hostPath"]
                ),
                file=sys.stderr,
            )

    original_docker_cmd = args.cmd or instance_config.get_cmd()
    spark_ui_port = pick_random_port(args.service + str(os.getpid()))
    spark_app_name = get_spark_app_name(original_docker_cmd, spark_ui_port)

    command = f"echo 'hola\n'"
    retcode, _ = _run(command, stream=True)
    # Uncomment this code to update the credentials file to define the session token/key/id inline
    # command = f"aws-okta -a Dev -r read-only -k --session-duration 900"
    # retcode, _ = _run(command, stream=True)

    access_key, secret_key, session_token = get_aws_credentials(
        service=args.service,
        no_aws_credentials=args.no_aws_credentials,
        aws_credentials_yaml=args.aws_credentials_yaml,
        profile_name=args.aws_profile,
    )
    # Debuging access key so I can see which profile is used
    print('>>>>>' + access_key + "<<<<<<\n")
    spark_config_dict = get_spark_config(
        args=args,
        spark_app_name=spark_app_name,
        spark_ui_port=spark_ui_port,
        docker_img=docker_img,
        system_paasta_config=system_paasta_config,
        volumes=volumes,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
    )
    spark_conf_str = create_spark_config_str(spark_config_dict, is_mrjob=args.mrjob)

    # Spark client specific volumes
    volumes.append("%s:rw" % args.work_dir)
    volumes.append("/etc/passwd:/etc/passwd:ro")
    volumes.append("/etc/group:/etc/group:ro")
    volumes.append("/nail/home:/nail/home:rw")

    environment = instance_config.get_env_dictionary()
    environment.update(
        get_spark_env(args, spark_conf_str, spark_ui_port, access_key, secret_key)
    )

    webui_url = get_webui_url(spark_ui_port)

    docker_cmd = get_docker_cmd(args, instance_config, spark_conf_str)
    if "history-server" in docker_cmd:
        print(f"\nSpark history server URL {webui_url}\n")
    elif any(c in docker_cmd for c in ["pyspark", "spark-shell", "spark-submit"]):
        print(f"\nSpark monitoring URL {webui_url}\n")

    if clusterman_metrics and _should_emit_resource_requirements(
        docker_cmd, args.mrjob
    ):
        try:
            emit_resource_requirements(spark_config_dict, args.cluster, webui_url)
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
        container_name=spark_app_name,
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

    if args.build:
        docker_url = build_and_push_docker_image(args)
        if docker_url is None:
            return 1
    elif args.image:
        docker_url = args.image
    else:
        if args.cmd == "jupyter-lab":
            print(
                PaastaColors.red(
                    "The jupyter-lab command requires a prebuilt image with -I or --image."
                ),
                file=sys.stderr,
            )
            return 1

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
            return 1
        print(
            "Please wait while the image (%s) is pulled (times out after 5m)..."
            % docker_url,
            file=sys.stderr,
        )
        retcode, _ = _run(
            "sudo -H docker pull %s" % docker_url, stream=True, timeout=300
        )
        if retcode != 0:
            print(
                "\nPull failed. Are you authorized to run docker commands?",
                file=sys.stderr,
            )
            return 1

    return configure_and_run_docker_container(
        args,
        docker_img=docker_url,
        instance_config=instance_config,
        system_paasta_config=system_paasta_config,
    )
