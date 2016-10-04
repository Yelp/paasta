# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import fnmatch
import logging
import os
import pkgutil
import re
import sys
from socket import gaierror
from socket import gethostbyname_ex
from subprocess import CalledProcessError

from service_configuration_lib import read_services_configuration

from paasta_tools.api import client
from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.monitoring_tools import _load_sensu_team_data
from paasta_tools.utils import _run
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_default_cluster_for_service
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import validate_service_instance


log = logging.getLogger(__name__)


def load_method(module_name, method_name):
    """Return a function given a module and method name.

    :param module_name: a string
    :param method_name: a string
    :return: a function
    """
    module = __import__(module_name, fromlist=[method_name])
    method = getattr(module, method_name)
    return method


def modules_in_pkg(pkg):
    """Return the list of modules in a python package (a module with a
    __init__.py file.)

    :return: a list of strings such as `['list', 'check']` that correspond to
             the module names in the package.
    """
    for _, module_name, _ in pkgutil.walk_packages(pkg.__path__):
        yield module_name


def is_file_in_dir(file_name, path):
    """Recursively search path for file_name.

    :param file_name: a string of a file name to find
    :param path: a string path
    :param file_ext: a string of a file extension
    :return: a boolean
    """
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if fnmatch.fnmatch(filename, file_name):
                return os.path.join(root, filename)
    return False


def get_file_contents(path):
    """Open a file for reading

    :param path: path of file to read
    """
    with open(path) as p:
        return p.read()


def check_mark():
    """
    :return: string that can print a checkmark
    """
    return PaastaColors.green(u'\u2713'.encode('utf-8'))


def x_mark():
    """
    :return: string that can print an x-mark
    """
    return PaastaColors.red(u'\u2717'.encode('utf-8'))


def success(msg):
    """Format a paasta check success message.

    :param msg: a string
    :return: a beautiful string
    """
    return "%s %s" % (check_mark(), msg)


def failure(msg, link):
    """Format a paasta check failure message.

    :param msg: a string
    :return: a beautiful string
    """
    return "%s %s %s" % (x_mark(), msg, PaastaColors.blue(link))


class PaastaCheckMessages:

    """Collection of message printed out by 'paasta check'.
    Helpful as it avoids cumbersome maintenance of the unit tests.
    """

    DEPLOY_YAML_FOUND = success("deploy.yaml exists for a Jenkins pipeline")

    DEPLOY_YAML_MISSING = failure(
        "No deploy.yaml exists, so your service cannot be deployed.\n  "
        "Push a deploy.yaml and run `paasta generate-pipeline`.\n  "
        "More info:", "http://y/yelpsoa-configs")

    DEPLOY_SECURITY_FOUND = success("Found a security-check entry in your deploy pipeline")
    DEPLOY_SECURITY_MISSING = failure(
        "No 'security-check' entry was found in your deploy.yaml.\n"
        "Please add a security-check entry *AFTER* the itest entry in deploy.yaml\n"
        "so your docker image can be checked against known security vulnerabilities.\n"
        "More info:", "http://servicedocs.yelpcorp.com/docs/paasta_tools/cli/security_check.html")

    DEPLOY_PERFORMANCE_FOUND = success("Found a performance-check entry in your deploy pipeline")
    DEPLOY_PERFORMANCE_MISSING = failure(
        "No 'performance-check' entry was found in your deploy.yaml.\n"
        "Please add a performance-check entry *AFTER* the security-check entry in deploy.yaml\n"
        "so your docker image can be checked for performance regressions.\n"
        "More info:", "http://servicedocs.yelpcorp.com/docs/paasta_tools/cli/performance_check.html")

    DOCKERFILE_FOUND = success("Found Dockerfile")

    DOCKERFILE_MISSING = failure(
        "Dockerfile not found. Create a Dockerfile and try again.\n  "
        "More info:", "http://y/paasta-runbook-dockerfile")

    DOCKERFILE_YELPCORP = success(
        "Your Dockerfile pulls from the standard Yelp images.")

    DOCKERFILE_NOT_YELPCORP = failure(
        "Your Dockerfile does not use the standard Yelp images.\n  "
        "This is bad because your `docker pulls` will be slow and you won't be "
        "using the local mirrors.\n"
        "More info:", "http://y/base-docker-images")

    GIT_REPO_FOUND = success("Git repo found in the expected location.")

    MARATHON_YAML_FOUND = success("Found marathon.yaml file.")

    CHRONOS_YAML_FOUND = success("Found chronos.yaml file.")

    YAML_MISSING = failure(
        "No marathon.yaml or chronos.yaml exists, so your service cannot be deployed.\n  "
        "Push a marathon-[superregion].yaml or chronos-[superregion].yaml "
        "and run `paasta generate-pipeline`.\n  "
        "More info:", "http://y/yelpsoa-configs")

    MAKEFILE_FOUND = success("A Makefile is present")
    MAKEFILE_MISSING = failure(
        "No Makefile available. Please make a Makefile that responds\n"
        "to the proper targets. More info:", "http://paasta.readthedocs.io/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_BUILD_IMAGE = success("The Makefile responds to `make cook-image`")
    MAKEFILE_RESPONDS_BUILD_IMAGE_FAIL = failure(
        "The Makefile does not have a `make cook-image` target. local-run needs\n"
        "this and expects it to build your docker image. More info:",
        "http://paasta.readthedocs.io/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_ITEST = success("The Makefile responds to `make itest`")
    MAKEFILE_RESPONDS_ITEST_FAIL = failure(
        "The Makefile does not have a `make itest` target. Jenkins needs\n"
        "this and expects it to build and itest your docker image. More info:",
        "http://paasta.readthedocs.io/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_TEST = success("The Makefile responds to `make test`")
    MAKEFILE_RESPONDS_TEST_FAIL = failure(
        "The Makefile does not have a `make test` target. Jenkins needs\n"
        "this and expects it to run unit tests. More info:",
        "http://paasta.readthedocs.io/en/latest/about/contract.html"
    )
    MAKEFILE_HAS_A_TAB = success("The Makefile contains a tab character")
    MAKEFILE_HAS_NO_TABS = failure(
        "The Makefile contains no tab characters. Make sure you\n"
        "didn't accidentally paste spaces (which `make` does not respect)\n"
        "instead of a tab.",
        "http://paasta.readthedocs.io/en/latest/about/contract.html",
    )
    MAKEFILE_HAS_DOCKER_TAG = success("The Makefile contains a docker tag")
    MAKEFILE_HAS_NO_DOCKER_TAG = failure(
        "The Makefile contains no reference to DOCKER_TAG. Make sure you\n"
        "specify a DOCKER_TAG and that your itest tags your docker image with $DOCKER_TAG.",
        "http://paasta.readthedocs.io/en/latest/about/contract.html",
    )

    PIPELINE_FOUND = success("Jenkins build pipeline found")

    PIPELINE_MISSING = failure("Jenkins build pipeline missing. Please run "
                               "'paasta generate-pipeline'\n"
                               "  More info:", "http://y/paasta-deploy")

    SENSU_MONITORING_FOUND = success(
        "monitoring.yaml found for Sensu monitoring")

    SENSU_MONITORING_MISSING = failure(
        "Your service is not using Sensu (monitoring.yaml).\n  "
        "Please setup a monitoring.yaml so we know where to send alerts.\n  "
        "More info:", "http://y/monitoring-yaml")

    SENSU_TEAM_MISSING = failure(
        "Cannot get team name. Ensure 'team' field is set in monitoring.yaml.\n"
        "  More info:", "http://y/monitoring-yaml")

    SMARTSTACK_YAML_FOUND = success("Found smartstack.yaml file")

    SMARTSTACK_PORT_MISSING = failure(
        "Could not determine port. "
        "Ensure 'proxy_port' is set in smartstack.yaml.\n  "
        "More info:", "http://y/smartstack-cep323")

    @staticmethod
    def git_repo_missing(git_url):
        git_url = PaastaColors.cyan(git_url)
        return failure(
            "Could not find Git repo %s. "
            "Your service must be there.\n"
            "  More info:" % git_url,
            "http://y/yelpsoa-configs")

    @staticmethod
    def sensu_team_found(team_name):
        return success(
            "Your service uses Sensu and team '%s' will get alerts." % team_name)

    @staticmethod
    def smartstack_port_found(instance, port):
        return success(
            "Instance '%s' of your service is using smartstack port %d "
            "and will be automatically load balanced" % (instance, port))

    @staticmethod
    def service_dir_found(service, soa_dir):
        message = "yelpsoa-config directory for %s found in %s" \
                  % (PaastaColors.cyan(service), soa_dir)
        return success(message)

    @staticmethod
    def service_dir_missing(service, soa_dir):
        message = "Failed to locate yelpsoa-config directory for %s in %s.\n" \
                  "  Please follow the guide linked below to get boilerplate." \
                  % (service, soa_dir)
        return failure(message, "http://y/paasta-deploy")


class NoSuchService(Exception):

    """Exception to be raised in the event that the service
    name can not be guessed.
    """
    GUESS_ERROR_MSG = "Could not determine service name.\n" \
                      "Please run this from the root of a copy " \
                      "(git clone) of your service.\n" \
                      "Alternatively, supply the %s name you wish to " \
                      "inspect with the %s option." \
                      % (PaastaColors.cyan('SERVICE'), PaastaColors.cyan('-s'))

    CHECK_ERROR_MSG = "not found.  Please provide a valid service name.\n" \
                      "Ensure that a directory of the same name exists in %s."\
                      % PaastaColors.green('/nail/etc/services')

    def __init__(self, service):
        self.service = service

    def __str__(self):
        if self.service:
            return "SERVICE: %s %s" \
                   % (PaastaColors.cyan(self.service), self.CHECK_ERROR_MSG)
        else:
            return self.GUESS_ERROR_MSG


def guess_service_name():
    """Deduce the service name from the pwd
    :return : A string representing the service name
    """
    return os.path.basename(os.getcwd())


def guess_instance(service, cluster, args):
    """Returns instance from args if available, otherwise uses 'main' if it is a valid instance,
    otherwise takes a good guess and returns the first instance available"""
    if args.instance:
        instance = args.instance
    else:
        try:
            instances = list_all_instances_for_service(
                service=service, clusters=[cluster], instance_type=None, soa_dir=args.yelpsoa_config_root)
            if 'main' in instances:
                instance = 'main'
            else:
                instance = list(instances)[0]
        except NoConfigurationForServiceError:
            sys.stderr.write(PaastaColors.red(
                'Could not automatically detect instance to emulate. Please specify one with the --instance option.\n'))
            sys.exit(2)
        sys.stderr.write(PaastaColors.yellow(
            'Guessing instance configuration for %s. To override, use the --instance option.\n' % instance))
    return instance


def guess_cluster(service, args):
    """Returns the cluster from args if available, otherwise uses the "default" one"""
    if args.cluster:
        cluster = args.cluster
    else:
        try:
            cluster = get_default_cluster_for_service(service, soa_dir=args.yelpsoa_config_root)
        except NoConfigurationForServiceError:
            sys.stderr.write(PaastaColors.red(
                'Could not automatically detect cluster to emulate. Please specify one with the --cluster option.\n'))
            sys.exit(2)
        sys.stderr.write(PaastaColors.yellow(
            'Guessing cluster configuration for %s. To override, use the --cluster option.\n' % cluster))
    return cluster


def validate_service_name(service, soa_dir=DEFAULT_SOA_DIR):
    """Determine whether directory named service exists in the provided soa_dir
    :param service: a string of the name of the service you wish to check exists
    :param soa_dir: directory to look for service names
    :return : boolean True
    :raises: NoSuchService exception
    """
    if not service or not os.path.isdir(os.path.join(soa_dir, service)):
        raise NoSuchService(service)
    return True


def list_services(**kwargs):
    """Returns a sorted list of all services"""
    return sorted(read_services_configuration().keys())


def list_paasta_services():
    """Returns a sorted list of services that happen to have at
    least one service.instance (including Marathon and Chronos instances), which indicates it is on PaaSTA
    """
    the_list = []
    for service in list_services():
        if list_all_instances_for_service(service):
            the_list.append(service)
    return the_list


def list_service_instances():
    """Returns a sorted list of service<SPACER>instance names"""
    the_list = []
    for service in list_services():
        for instance in list_all_instances_for_service(service):
            the_list.append(compose_job_id(service, instance))
    return the_list


def list_instances(**kwargs):
    """Returns a sorted list of all possible instance names
    for tab completion. We try to guess what service you might be
    operating on, otherwise we just provide *all* of them
    """
    all_instances = set()
    service = guess_service_name()
    try:
        validate_service_name(service)
        all_instances = set(list_all_instances_for_service(service))
    except NoSuchService:
        for service in list_services():
            for instance in list_all_instances_for_service(service):
                all_instances.add(instance)
    return sorted(all_instances)


def list_teams(**kwargs):
    """Loads team data from the system. Returns a set of team names (or empty
    set).
    """
    team_data = _load_sensu_team_data()
    teams = set(team_data.get('team_data', {}).keys())
    return teams


def calculate_remote_masters(cluster, system_paasta_config):
    """Given a cluster, do a DNS lookup of that cluster (which
    happens to point, eventually, to the Mesos masters in that cluster).
    Return IPs of those Mesos masters.
    """

    cluster_fqdn = system_paasta_config.get_cluster_fqdn_format().format(cluster=cluster)
    try:
        _, _, ips = gethostbyname_ex(cluster_fqdn)
        output = None
    except gaierror as e:
        output = 'ERROR while doing DNS lookup of %s:\n%s\n ' % (cluster_fqdn, e.strerror)
        ips = []
    return (ips, output)


def find_connectable_master(masters):
    """For each host in the iterable 'masters', try various connectivity
    checks. For each master that fails, emit an error message about which check
    failed and move on to the next master.

    If a master passes all checks, return a tuple of the connectable master and
    None. If no masters pass all checks, return a tuple of None and the output
    from the DNS lookup.
    """
    timeout = 6.0  # seconds

    connectable_master = None
    for master in masters:
        rc, output = check_ssh_and_sudo_on_master(master, timeout=timeout)
        if rc is True:
            connectable_master = master
            output = None
            break
    return (connectable_master, output)


def check_ssh_and_sudo_on_master(master, timeout=10):
    """Given a master, attempt to ssh to the master and run a simple command
    with sudo to verify that ssh and sudo work properly. Return a tuple of the
    success status (True or False) and any output from attempting the check.
    """
    check_command = 'ssh -A -n %s sudo paasta_serviceinit -h' % master
    rc, output = _run(check_command, timeout=timeout)
    if rc == 0:
        return (True, None)
    if rc == 255:  # ssh error
        reason = 'Return code was %d which probably means an ssh failure.' % rc
        hint = 'HINT: Are you allowed to ssh to this machine %s?' % master
    if rc == 1:  # sudo error
        reason = 'Return code was %d which probably means a sudo failure.' % rc
        hint = 'HINT: Is your ssh agent forwarded? (ssh-add -l)'
    if rc == -9:  # timeout error
        reason = 'Return code was %d which probably means ssh took too long and timed out.' % rc
        hint = 'HINT: Is there network latency? Try running somewhere closer to the cluster.'
    else:  # unknown error
        reason = 'Return code was %d which is an unknown failure.' % rc
        hint = 'HINT: Talk to #operations and pastebin this output'
    output = ('ERROR cannot run check command %(check_command)s\n'
              '%(reason)s\n'
              '%(hint)s\n'
              'Output from check command: %(output)s' %
              {
                  'check_command': check_command,
                  'reason': reason,
                  'hint': hint,
                  'output': output,
              })
    return (False, output)


def run_paasta_serviceinit(subcommand, master, service, instances, cluster, stream, ssh_flags='', **kwargs):
    """Run 'paasta_serviceinit <subcommand>'. Return the output from running it."""
    if 'verbose' in kwargs and kwargs['verbose'] > 0:
        verbose_flag = ' '.join(['-v' for i in range(kwargs['verbose'])])
        timeout = 960 if subcommand == 'status' else 240
    else:
        verbose_flag = ''
        timeout = 240 if subcommand == 'status' else 60

    if 'app_id' in kwargs and kwargs['app_id']:
        app_id_flag = "--appid %s" % kwargs['app_id']
    else:
        app_id_flag = ''

    if 'delta' in kwargs and kwargs['delta']:
        delta_flag = "--delta %s" % kwargs['delta']
    else:
        delta_flag = ''

    ssh_flags += ' -t' if stream else ' -n'
    ssh_flags = ssh_flags.strip()

    command_parts = [
        "ssh -A %s %s sudo paasta_serviceinit" % (ssh_flags, master),
        "-s %s" % service,
        "-i %s" % instances,
        verbose_flag,
        app_id_flag,
        delta_flag,
        subcommand
    ]
    command_without_empty_strings = [part for part in command_parts if part != '']
    command = ' '.join(command_without_empty_strings)
    log.debug("Running Command: %s" % command)
    return_code, output = _run(command, timeout=timeout, stream=stream)
    if return_code != 0:
        raise CalledProcessError(return_code, command, output)
    return output


def execute_paasta_serviceinit_on_remote_master(subcommand, cluster, service, instances, system_paasta_config,
                                                stream=False, ignore_ssh_output=False, **kwargs):
    """Returns a string containing an error message if an error occurred.
    Otherwise returns the output of run_paasta_serviceinit_status().
    """
    masters, output = calculate_remote_masters(cluster, system_paasta_config)
    if masters == []:
        return 'ERROR: %s' % output
    master, output = find_connectable_master(masters)
    if not master:
        return (
            'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        )
    if ignore_ssh_output:
        return run_paasta_serviceinit(subcommand, master, service, instances, cluster, stream,
                                      ssh_flags='-o LogLevel=QUIET', **kwargs)
    else:
        return run_paasta_serviceinit(subcommand, master, service, instances, cluster, stream, **kwargs)


def run_paasta_metastatus(master, humanize, groupings, verbose=0):
    if verbose > 0:
        verbose_flag = "-%s" % ('v' * verbose)
        timeout = 120
    else:
        verbose_flag = ''
        timeout = 20
    humanize_flag = "-H" if humanize else ''
    groupings_flag = "-g %s" % " ".join(groupings) if groupings else ''
    cmd_args = " ".join(filter(None, [verbose_flag, humanize_flag, groupings_flag]))
    command = ('ssh -A -n %s sudo paasta_metastatus %s' % (
        master,
        cmd_args
    )).strip()
    _, output = _run(command, timeout=timeout)
    return output


def execute_paasta_metastatus_on_remote_master(cluster, system_paasta_config, humanize, groupings, verbose):
    """Returns a string containing an error message if an error occurred.
    Otherwise returns the output of run_paasta_metastatus().
    """
    masters, output = calculate_remote_masters(cluster, system_paasta_config)
    if masters == []:
        return 'ERROR: %s' % output
    master, output = find_connectable_master(masters)
    if not master:
        return (
            'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        )
    return run_paasta_metastatus(master, humanize, groupings, verbose)


def run_chronos_rerun(master, service, instancename, **kwargs):
    timeout = 60
    verbose_flags = '-v ' * kwargs['verbose']
    command = 'ssh -A -n %s \'sudo chronos_rerun %s"%s %s" "%s"\'' % (
        master,
        verbose_flags,
        service,
        instancename,
        kwargs['execution_date'],
    )
    return _run(command, timeout=timeout)


def execute_chronos_rerun_on_remote_master(service, instancename, cluster, system_paasta_config, **kwargs):
    """Returns a string containing an error message if an error occurred.
    Otherwise returns the output of run_chronos_rerun().
    """
    masters, output = calculate_remote_masters(cluster, system_paasta_config)
    if masters == []:
        return (-1, 'ERROR: %s' % output)
    master, output = find_connectable_master(masters)
    if not master:
        return (
            -1,
            'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        )
    return run_chronos_rerun(master, service, instancename, **kwargs)


def lazy_choices_completer(list_func):
    def inner(prefix, **kwargs):
        options = list_func(**kwargs)
        return [o for o in options if o.startswith(prefix)]
    return inner


def figure_out_service_name(args, soa_dir=DEFAULT_SOA_DIR):
    """Figures out and validates the input service name"""
    service = args.service or guess_service_name()
    try:
        validate_service_name(service, soa_dir=soa_dir)
    except NoSuchService as service_not_found:
        print service_not_found
        exit(1)
    return service


def get_jenkins_build_output_url():
    """Returns the URL for Jenkins job's output.
    Returns None if it's not available.
    """
    build_output = os.environ.get('BUILD_URL')
    if build_output:
        build_output = build_output + 'console'
    return build_output


def get_instance_config(service, instance, cluster, soa_dir, load_deployments=False):
    """ Returns the InstanceConfig object for whatever type of instance
    it is. (chronos or marathon) """
    instance_type = validate_service_instance(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
    )
    if instance_type == 'marathon':
        instance_config_load_function = load_marathon_service_config
    elif instance_type == 'chronos':
        instance_config_load_function = load_chronos_job_config
    else:
        raise NotImplementedError(
            "instance is %s of type %s which is not supported by paasta"
            % (instance, instance_type)
        )
    return instance_config_load_function(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir
    )


def extract_tags(paasta_tag):
    """Returns a dictionary containing information from a git tag"""
    regex = r'^refs/tags/(?:paasta-){1,2}(?P<deploy_group>.*?)-(?P<tstamp>\d{8}T\d{6})-(?P<tag>.*?)$'
    regex_match = re.match(regex, paasta_tag)
    return regex_match.groupdict() if regex_match else {}


def validate_given_deploy_groups(service_deploy_groups, args_deploy_groups):
    """Given two lists of deploy groups, return the intersection and difference between them.

    :param service_deploy_groups: instances actually belonging to a service
    :param args_deploy_groups: the desired instances
    :returns: a tuple with (common, difference) indicating deploy groups common in both
        lists and those only in args_deploy_groups
    """
    if len(args_deploy_groups) is 0:
        valid_deploy_groups = set(service_deploy_groups)
        invalid_deploy_groups = set([])
    else:
        valid_deploy_groups = set(args_deploy_groups).intersection(service_deploy_groups)
        invalid_deploy_groups = set(args_deploy_groups).difference(service_deploy_groups)

    return valid_deploy_groups, invalid_deploy_groups


def get_subparser(subparsers, function, command, help_text, description):
    new_parser = subparsers.add_parser(
        command,
        help=help_text,
        description=(description),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "nodes."
        ),
    )
    new_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
        required=True
    ).completer = lazy_choices_completer(list_services)
    new_parser.add_argument(
        '-c', '--cluster',
        help="Cluster on which the service is running"
             "For example: --cluster norcal-prod",
        required=True
    ).completer = lazy_choices_completer(list_clusters)
    new_parser.add_argument(
        '-i', '--instance',
        help="The instance that you wish to inspect"
             "For example: --instance main",
        required=True,
        default='main'
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    new_parser.add_argument(
        '-H', '--host',
        dest="host",
        default=None,
        help="Specify a specific host on which to run. Defaults to"
             " one that is running the service chosen at random"
    )
    new_parser.add_argument(
        '-m', '--mesos-id',
        dest="mesos_id",
        default=None,
        help="A specific mesos task ID, must match a task "
             "running on the specified host. If not specified we "
             "will pick a task at random"
    )
    new_parser.set_defaults(command=function)
    return new_parser


def get_status_for_instance(cluster, service, instance):
    api = client.get_paasta_api_client(cluster=cluster)
    if not api:
        sys.exit(1)
    status = api.service.status_instance(service=service, instance=instance).result()
    if not status.marathon:
        log.error("Not a marathon service, exiting")
        sys.exit(1)
    return status


def pick_slave_from_status(status, host=None):
    if host:
        return host
    else:
        slaves = status.marathon.slaves
        return slaves[0]
