import argparse
import json
import os
import socket
import sys
import time

from boto3.exceptions import Boto3Error
from botocore.session import Session
from ruamel.yaml import YAML

from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import pick_random_port
from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.mesos_tools import find_mesos_leader
from paasta_tools.mesos_tools import MESOS_MASTER_PORT
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_username
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig

AWS_CREDENTIALS_DIR = '/etc/boto_cfg/'
DEFAULT_SERVICE = 'spark'
DEFAULT_SPARK_WORK_DIR = '/spark_driver'
DEFAULT_SPARK_DOCKER_IMAGE_PREFIX = 'paasta-spark-run'
DEFAULT_SPARK_DOCKER_REGISTRY = 'docker-dev.yelpcorp.com'
DEFAULT_SPARK_MESOS_SECRET_FILE = '/nail/etc/paasta_spark_secret'
SENSITIVE_ENV = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
clusterman_metrics, CLUSTERMAN_YAML_FILE_PATH = get_clusterman_metrics()


deprecated_opts = {
    'j': 'spark.jars',
    'jars': 'spark.jars',
    'max-cores': 'spark.cores.max',
    'executor-cores': 'spark.executor.cores',
    'executor-memory': 'spark.executor.memory',
    'driver-max-result-size': 'spark.driver.maxResultSize',
    'driver-cores': 'spark.driver.cores',
    'driver-memory': 'spark.driver.memory',
}


class DeprecatedAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        paasta_print(
            PaastaColors.red(
                "Use of {} is deprecated. Please use {}=value in --spark-args.".format(
                    option_string,
                    deprecated_opts[option_string.strip('-')],
                ),
            ),
        )
        sys.exit(1)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'spark-run',
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
        '-b', '--build',
        help="Build the docker image from scratch using the local Makefile's cook-image target.",
        action='store_true',
        default=False,
    )
    group.add_argument(
        '-I', '--image',
        help="Use the provided image to start the Spark driver and executors.",
    )

    list_parser.add_argument(
        '--docker-registry',
        help="Docker registry to push the Spark image built.",
        default=DEFAULT_SPARK_DOCKER_REGISTRY,
    )

    list_parser.add_argument(
        '-s', '--service',
        help="The name of the service from which the Spark image is built.",
        default=DEFAULT_SERVICE,
    ).completer = lazy_choices_completer(list_services)

    list_parser.add_argument(
        '-i', '--instance',
        help=("Start a docker run for a particular instance of the service."),
        default='client',
    ).completer = lazy_choices_completer(list_instances)

    # Restrict usage to norcal-devc and pnw-devc for now.
    list_parser.add_argument(
        '-c', '--cluster',
        help=(
            "The name of the cluster you wish to run Spark on."
        ),
        default='norcal-devc',
    )

    list_parser.add_argument(
        '-p', '--pool',
        help="Name of the resource pool to run the Spark job.",
        default='default',
    )

    list_parser.add_argument(
        '-w', '--work-dir',
        default='{}:{}'.format(os.getcwd(), DEFAULT_SPARK_WORK_DIR),
        help="The read-write volume to mount in format local_abs_dir:container_abs_dir",
    )

    list_parser.add_argument(
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        help='A directory from which yelpsoa-configs should be read from.',
        default=DEFAULT_SOA_DIR,
    )

    list_parser.add_argument(
        '-C', '--cmd',
        help="Run the spark-shell, pyspark, spark-submit, jupyter, or history-server command.",
        default='pyspark',
    )

    list_parser.add_argument(
        '-d', '--dry-run',
        help='Shows the arguments supplied to docker as json.',
        action='store_true',
        default=False,
    )

    list_parser.add_argument(
        '--mesos-principal',
        help="Mesos principal (username) to run a framework on Mesos.",
        default='spark',
    )

    list_parser.add_argument(
        '--mesos-secret',
        help="Mesos secret (password) to run a framework on Mesos.",
    )

    list_parser.add_argument(
        '--spark-args',
        help='Spark configurations documented in https://spark.apache.org/docs/latest/configuration.html. '
        'For example, --spark-args "spark.mesos.constraints=pool:default\;instance_type:m4.10xlarge '
        'spark.executor.cores=4".',
    )

    if clusterman_metrics:
        list_parser.add_argument(
            '--suppress-clusterman-metrics-errors',
            help='Continue even if sending resource requirements to Clusterman fails. This may result in the job '
            'failing to acquire resources.',
            action='store_true',
        )

    list_parser.add_argument(
        '-j', '--jars',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--executor-memory',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--executor-cores',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--max-cores',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--driver-max-result-size',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--driver-memory',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    list_parser.add_argument(
        '--driver-cores',
        help=argparse.SUPPRESS,
        action=DeprecatedAction,
    )

    aws_group = list_parser.add_argument_group(
        title='AWS credentials options',
        description='If --aws-credentials-yaml is specified, it overrides all '
        'other options. Otherwise, if -s/--service is specified, spark-run '
        'looks for service credentials in /etc/boto_cfg/[service].yaml. If '
        'it does not find the service credentials or no service is '
        'specified, spark-run falls back to the boto default behavior '
        '(checking ~/.aws/credentials, ~/.boto, etc).',
    )

    aws_group.add_argument(
        '--aws-credentials-yaml',
        help='Load aws keys from the provided yaml file. The yaml file must '
        'have keys for aws_access_key_id and aws_secret_access_key.',
    )

    aws_group.add_argument(
        '--aws-profile',
        help="Name of the AWS profile to load credentials from. Only used when "
        "--aws-credentials-yaml is not specified and --service is either "
        "not specified or the service does not have credentials in "
        "/etc/boto_cfg",
        default='default',
    )

    jupyter_group = list_parser.add_argument_group(
        title='Jupyter kernel culling options',
        description='Idle kernels will be culled by default. Idle '
        'kernels with connections can be overridden not to be culled.',
    )

    jupyter_group.add_argument(
        '--cull-idle-timeout',
        type=int,
        default=7200,
        help='Timeout (in seconds) after which a kernel is considered idle and '
        'ready to be culled.',
    )

    jupyter_group.add_argument(
        '--not-cull-connected',
        action='store_true',
        default=False,
        help='By default, connected idle kernels are culled after timeout. '
        'They can be skipped if not-cull-connected is specified.',
    )

    list_parser.set_defaults(command=paasta_spark_run)


def get_docker_run_cmd(
    container_name,
    volumes,
    env,
    docker_img,
    docker_cmd,
):
    cmd = ['paasta_docker_wrapper', 'run']
    cmd.append('--rm')
    cmd.append('--net=host')

    sensitive_env = {}

    non_interactive_cmd = ['spark-submit', 'jupyter', 'history-server']
    if not any(c in docker_cmd for c in non_interactive_cmd):
        cmd.append('--interactive=true')
        if sys.stdout.isatty():
            cmd.append('--tty=true')

    cmd.append('--user=%d:%d' % (os.geteuid(), os.getegid()))
    cmd.append('--name=%s' % container_name)
    for k, v in env.items():
        cmd.append('--env')
        if k in SENSITIVE_ENV:
            sensitive_env[k] = v
            cmd.append(k)
        else:
            cmd.append(f'{k}={v}')
    for volume in volumes:
        cmd.append('--volume=%s' % volume)
    cmd.append('%s' % docker_img)
    cmd.extend(('sh', '-c', docker_cmd))
    cmd.append(sensitive_env)

    return cmd


def get_spark_env(
    args,
    spark_conf,
    spark_ui_port,
):
    spark_env = {}

    access_key, secret_key = get_aws_credentials(args)
    spark_env['AWS_ACCESS_KEY_ID'] = access_key
    spark_env['AWS_SECRET_ACCESS_KEY'] = secret_key

    # Run spark (and mesos framework) as root.
    spark_env['SPARK_USER'] = 'root'
    spark_env['SPARK_OPTS'] = spark_conf

    # Default configs to start the jupyter notebook server
    if args.cmd == 'jupyter':
        dirs = args.work_dir.split(':')
        spark_env['JUPYTER_RUNTIME_DIR'] = dirs[1] + '/.jupyter'
        spark_env['JUPYTER_DATA_DIR'] = dirs[1] + '/.jupyter'
    elif args.cmd == 'history-server':
        dirs = args.work_dir.split(':')
        spark_env['SPARK_LOG_DIR'] = dirs[1]
        if not args.spark_args or not args.spark_args.startswith('spark.history.fs.logDirectory'):
            paasta_print(
                "history-server requires spark.history.fs.logDirectory in spark-args",
                file=sys.stderr,
            )
            sys.exit(1)
        spark_env['SPARK_HISTORY_OPTS'] = '-D%s -Dspark.history.ui.port=%d' % (
            args.spark_args,
            spark_ui_port,
        )
        spark_env['SPARK_NO_DAEMONIZE'] = 'true'

    return spark_env


def get_aws_credentials(args):
    if args.aws_credentials_yaml:
        return load_aws_credentials_from_yaml(args.aws_credentials_yaml)
    elif args.service != DEFAULT_SERVICE:
        service_credentials_path = get_service_aws_credentials_path(args.service)
        if os.path.exists(service_credentials_path):
            return load_aws_credentials_from_yaml(service_credentials_path)
        else:
            paasta_print(
                PaastaColors.yellow(
                    'Did not find service AWS credentials at %s.  Falling back to '
                    'user credentials.' % (service_credentials_path),
                ),
            )

    creds = Session(profile=args.aws_profile).get_credentials()
    return creds.access_key, creds.secret_key


def get_service_aws_credentials_path(service_name):
    service_yaml = '%s.yaml' % service_name
    return os.path.join(AWS_CREDENTIALS_DIR, service_yaml)


def load_aws_credentials_from_yaml(yaml_file_path):
    with open(yaml_file_path, 'r') as yaml_file:
        try:
            credentials_yaml = YAML().load(yaml_file.read())
        except Exception as e:
            paasta_print(
                PaastaColors.red(
                    'Encountered %s when trying to parse AWS credentials yaml %s. '
                    'Suppressing further output to avoid leaking credentials.' % (
                        type(e),
                        yaml_file_path,
                    ),
                ),
            )
            sys.exit(1)

        return (
            credentials_yaml['aws_access_key_id'],
            credentials_yaml['aws_secret_access_key'],
        )


def get_spark_config(
    args,
    container_name,
    spark_ui_port,
    docker_img,
    system_paasta_config,
    volumes,
):
    # User configurable Spark options
    user_args = {
        'spark.cores.max': '4',
        'spark.executor.cores': '2',
        'spark.executor.memory': '4g',
        # Use \; for multiple constraints. e.g.
        # instance_type:m4.10xlarge\;pool:default
        'spark.mesos.constraints': 'pool:%s' % args.pool,
        'spark.mesos.executor.docker.forcePullImage': 'true',
    }

    # Spark options managed by PaaSTA
    cluster_fqdn = system_paasta_config.get_cluster_fqdn_format().format(cluster=args.cluster)
    mesos_address = '{}:{}'.format(
        find_mesos_leader(cluster_fqdn),
        MESOS_MASTER_PORT,
    )
    non_user_args = {
        'spark.master': 'mesos://%s' % mesos_address,
        'spark.app.name': container_name,
        'spark.ui.port': spark_ui_port,
        'spark.executorEnv.PAASTA_SERVICE': args.service,
        'spark.executorEnv.PAASTA_INSTANCE': '{}_{}'.format(args.instance, get_username()),
        'spark.executorEnv.PAASTA_CLUSTER': args.cluster,
        'spark.mesos.executor.docker.parameters': 'label=paasta_service={},label=paasta_instance={}_{}'.format(
            args.service, args.instance, get_username(),
        ),
        'spark.mesos.executor.docker.volumes': ','.join(volumes),
        'spark.mesos.executor.docker.image': docker_img,
        'spark.mesos.principal': args.mesos_principal,
        'spark.mesos.secret': args.mesos_secret,
        # derby.system.home property defaulting to '.',
        # which requires directory permission changes.
        'spark.driver.extraJavaOptions': '-Dderby.system.home=/tmp/derby',
    }

    if not args.mesos_secret:
        try:
            with open(DEFAULT_SPARK_MESOS_SECRET_FILE, 'r') as f:
                mesos_secret = f.read()
                non_user_args['spark.mesos.secret'] = mesos_secret
        except IOError:
            paasta_print(
                'Cannot load mesos secret from %s' % DEFAULT_SPARK_MESOS_SECRET_FILE,
                file=sys.stderr,
            )
            sys.exit(1)

    if not args.build and not args.image:
        non_user_args['spark.mesos.uris'] = 'file:///root/.dockercfg'

    if args.spark_args:
        spark_args = args.spark_args.split()
        for spark_arg in spark_args:
            fields = spark_arg.split('=')
            if len(fields) != 2:
                paasta_print(
                    PaastaColors.red(
                        "Spark option %s is not in format option=value." % spark_arg,
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)

            if fields[0] in non_user_args:
                paasta_print(
                    PaastaColors.red(
                        "Spark option {} is set by PaaSTA with {}.".format(
                            fields[0],
                            non_user_args[fields[0]],
                        ),
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)
            # Update default configuration
            user_args[fields[0]] = fields[1]

    if int(user_args['spark.cores.max']) < int(user_args['spark.executor.cores']):
        paasta_print(
            PaastaColors.red(
                "Total number of cores {} is less than per-executor cores {}.".format(
                    user_args['spark.cores.max'],
                    user_args['spark.executor.cores'],
                ),
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    exec_mem = user_args['spark.executor.memory']
    if exec_mem[-1] != 'g' or not exec_mem[:-1].isdigit() or int(exec_mem[:-1]) > 32:
        paasta_print(
            PaastaColors.red(
                "Executor memory {} not in format dg (d<=32).".format(
                    user_args['spark.executor.memory'],
                ),
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    return dict(non_user_args, **user_args)


def create_spark_config_str(spark_config_dict):
    spark_config_entries = list()
    for opt, val in spark_config_dict.items():
        spark_config_entries.append(f'--conf {opt}={val}')
    return ' '.join(spark_config_entries)


def emit_resource_requirements(spark_config_dict, paasta_cluster, pool):
    num_executors = int(spark_config_dict['spark.cores.max']) / int(spark_config_dict['spark.executor.cores'])
    memory_per_executor = spark_memory_to_megabytes(spark_config_dict['spark.executor.memory'])

    desired_resources = {
        'cpus': int(spark_config_dict['spark.cores.max']),
        'mem': memory_per_executor * num_executors,
        'disk': memory_per_executor * num_executors,  # rough guess since spark does not collect this information
    }
    dimensions = {'framework_name': spark_config_dict['spark.app.name']}

    paasta_print('Sending resource request metrics to Clusterman')
    aws_region = get_aws_region_for_paasta_cluster(paasta_cluster)
    metrics_client = clusterman_metrics.ClustermanMetricsBotoClient(region_name=aws_region, app_identifier=pool)

    with metrics_client.get_writer(clusterman_metrics.APP_METRICS, aggregate_meteorite_dims=True) as writer:
        for resource, desired_quantity in desired_resources.items():
            metric_key = clusterman_metrics.generate_key_with_dimensions(f'requested_{resource}', dimensions)
            writer.send((metric_key, int(time.time()), desired_quantity))


def get_aws_region_for_paasta_cluster(paasta_cluster):
    with open(CLUSTERMAN_YAML_FILE_PATH, 'r') as clusterman_yaml_file:
        clusterman_yaml = YAML().load(clusterman_yaml_file.read())
        return clusterman_yaml['mesos_clusters'][paasta_cluster]['aws_region']


def spark_memory_to_megabytes(spark_memory_string):
    # expected to be in format "dg" where d is an integer
    return 1000 * int(spark_memory_string[:-1])


def run_docker_container(
    container_name,
    volumes,
    environment,
    docker_img,
    docker_cmd,
    dry_run,
):
    docker_run_args = dict(
        container_name=container_name,
        volumes=volumes,
        env=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
    )
    docker_run_cmd = get_docker_run_cmd(**docker_run_args)

    if dry_run:
        paasta_print(json.dumps(docker_run_cmd))
        return 0

    os.execlpe('paasta_docker_wrapper', *docker_run_cmd)
    return 0


def configure_and_run_docker_container(
        args,
        docker_img,
        instance_config,
        system_paasta_config,
):
    volumes = list()
    for volume in instance_config.get_volumes(system_paasta_config.get_volumes()):
        if os.path.exists(volume['hostPath']):
            volumes.append('{}:{}:{}'.format(volume['hostPath'], volume['containerPath'], volume['mode'].lower()))
        else:
            paasta_print(
                PaastaColors.yellow(
                    "Warning: Path %s does not exist on this host. Skipping this binding." % volume['hostPath'],
                ),
            )

    spark_ui_port = pick_random_port(args.service + str(os.getpid()))
    container_name = 'paasta_spark_run_{}_{}_{}'.format(get_username(), spark_ui_port, int(time.time()))

    spark_config_dict = get_spark_config(
        args=args,
        container_name=container_name,
        spark_ui_port=spark_ui_port,
        docker_img=docker_img,
        system_paasta_config=system_paasta_config,
        volumes=volumes,
    )
    spark_conf_str = create_spark_config_str(spark_config_dict)

    # Spark client specific volumes
    volumes.append('%s:rw' % args.work_dir)
    volumes.append('/etc/passwd:/etc/passwd:ro')
    volumes.append('/etc/group:/etc/group:ro')

    docker_cmd = get_docker_cmd(args, instance_config, spark_conf_str)
    if docker_cmd is None:
        paasta_print("A command is required, pyspark, spark-shell, spark-submit or jupyter", file=sys.stderr)
        return 1

    environment = instance_config.get_env_dictionary()
    environment.update(
        get_spark_env(
            args,
            spark_conf_str,
            spark_ui_port,
        ),
    )

    if 'history-server' in docker_cmd:
        paasta_print('\nSpark history server URL http://%s:%d\n' % (socket.getfqdn(), spark_ui_port))
    elif any(c in docker_cmd for c in ['pyspark', 'spark-shell', 'jupyter']):
        paasta_print('\nSpark monitoring URL http://%s:%d\n' % (socket.getfqdn(), spark_ui_port))

    if clusterman_metrics and _should_emit_resource_requirements(docker_cmd):
        try:
            emit_resource_requirements(spark_config_dict, args.cluster, args.pool)
        except Boto3Error as e:
            paasta_print(
                PaastaColors.red(f'Encountered {e} while attempting to send resource requirements to Clusterman.'),
            )
            if args.suppress_clusterman_metrics_errors:
                paasta_print('Continuing anyway since --suppress-clusterman-metrics-errors was passed')
            else:
                raise

    return run_docker_container(
        container_name=container_name,
        volumes=volumes,
        environment=environment,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        dry_run=args.dry_run,
    )


def _should_emit_resource_requirements(docker_cmd):
    return any(c in docker_cmd for c in ['pyspark', 'spark-shell', 'spark-submit'])


def get_docker_cmd(args, instance_config, spark_conf_str):
    original_docker_cmd = args.cmd or instance_config.get_cmd()
    if original_docker_cmd is None:
        return None

    # Default cli options to start the jupyter notebook server.
    if original_docker_cmd == 'jupyter':
        cull_opts = '--MappingKernelManager.cull_idle_timeout=%s ' % args.cull_idle_timeout
        if args.not_cull_connected is False:
            cull_opts += '--MappingKernelManager.cull_connected=True '

        return 'jupyter notebook -y --ip={} --notebook-dir={} {}'.format(
            socket.getfqdn(), args.work_dir.split(':')[1], cull_opts,
        )
    elif original_docker_cmd == 'history-server':
        return 'start-history-server.sh'
    # Spark options are passed as options to pyspark and spark-shell.
    # For jupyter, environment variable SPARK_OPTS is set instead.
    else:
        for base_cmd in ('pyspark', 'spark-shell', 'spark-submit'):
            if base_cmd in original_docker_cmd:
                return original_docker_cmd.replace(
                    base_cmd,
                    base_cmd + ' ' + spark_conf_str,
                    1,
                )
        return original_docker_cmd


def build_and_push_docker_image(args):
    """
    Build an image if the default Spark service image is not preferred.
    The image needs to be pushed to a registry for the Spark executors
    to pull.
    """
    if not makefile_responds_to('cook-image'):
        paasta_print(
            "A local Makefile with a 'cook-image' target is required for --build",
            file=sys.stderr,
        )
        return None

    default_tag = '{}-{}'.format(DEFAULT_SPARK_DOCKER_IMAGE_PREFIX, get_username())
    docker_tag = os.environ.get('DOCKER_TAG', default_tag)
    os.environ['DOCKER_TAG'] = docker_tag

    cook_return = paasta_cook_image(
        args=None,
        service=args.service,
        soa_dir=args.yelpsoa_config_root,
    )
    if cook_return is not 0:
        return None

    docker_url = f'{args.docker_registry}/{docker_tag}'
    command = f'docker tag {docker_tag} {docker_url}'
    paasta_print(PaastaColors.grey(command))
    retcode, _ = _run(command, stream=True)
    if retcode is not 0:
        return None

    if args.docker_registry != DEFAULT_SPARK_DOCKER_REGISTRY:
        command = 'sudo -H docker push %s' % docker_url
    else:
        command = 'docker push %s' % docker_url

    paasta_print(PaastaColors.grey(command))
    retcode, output = _run(command, stream=True)
    if retcode is not 0:
        return None

    return docker_url


def validate_work_dir(s):
    dirs = s.split(':')
    if len(dirs) != 2:
        paasta_print(
            "work-dir %s is not in format local_abs_dir:container_abs_dir" % s,
            file=sys.stderr,
        )
        sys.exit(1)

    for d in dirs:
        if not os.path.isabs(d):
            paasta_print("%s is not an absolute path" % d, file=sys.stderr)
            sys.exit(1)


def paasta_spark_run(args):
    # argparse does not work as expected with both default and
    # type=validate_work_dir.
    validate_work_dir(args.work_dir)

    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        paasta_print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and local-run may not behave"
                "the same way it would behave on a server configured for PaaSTA.",
            ),
            sep='\n',
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

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
        paasta_print(str(e), file=sys.stderr)
        return 1
    except NoDeploymentsAvailable:
        paasta_print(
            PaastaColors.red(
                "Error: No deployments.json found in %(soa_dir)s/%(service)s."
                "You can generate this by running:"
                "generate_deployments_for_service -d %(soa_dir)s -s %(service)s" % {
                    'soa_dir': args.yelpsoa_config_root,
                    'service': args.service,
                },
            ),
            sep='\n',
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
        try:
            docker_url = instance_config.get_docker_url()
        except NoDockerImageError:
            paasta_print(
                PaastaColors.red(
                    "Error: No sha has been marked for deployment for the %s deploy group.\n"
                    "Please ensure this service has either run through a jenkins pipeline "
                    "or paasta mark-for-deployment has been run for %s\n" % (
                        instance_config.get_deploy_group(), args.service,
                    ),
                ),
                sep='',
                file=sys.stderr,
            )
            return 1
        paasta_print(
            "Please wait while the image (%s) is pulled (times out after 5m)..." % docker_url,
            file=sys.stderr,
        )
        retcode, _ = _run('sudo -H docker pull %s' % docker_url, stream=True, timeout=300)
        if retcode != 0:
            paasta_print(
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
