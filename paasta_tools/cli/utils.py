# Copyright 2015 Yelp Inc.
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
import pkgutil
import os
import sys
from socket import gaierror
from socket import gethostbyname_ex

from service_configuration_lib import read_services_configuration
from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.monitoring_tools import _load_sensu_team_data
from paasta_tools.utils import _run
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import get_default_cluster_for_service
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError


log = logging.getLogger('__main__')
logging.basicConfig()


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

    MARATHON_YAML_MISSING = failure(
        "No marathon.yaml exists, so your service cannot be deployed.\n  "
        "Push a marathon-[superregion].yaml and run `paasta generate-pipeline`.\n  "
        "More info:", "http://y/yelpsoa-configs")

    MAKEFILE_FOUND = success("A Makefile is present")
    MAKEFILE_MISSING = failure(
        "No Makefile available. Please make a Makefile that responds\n"
        "to the proper targets. More info:", "http://paasta.readthedocs.org/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_BUILD_IMAGE = success("The Makefile responds to `make cook-image`")
    MAKEFILE_RESPONDS_BUILD_IMAGE_FAIL = failure(
        "The Makefile does not have a `make cook-image` target. local-run needs\n"
        "this and expects it to build your docker image. More info:",
        "http://paasta.readthedocs.org/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_ITEST = success("The Makefile responds to `make itest`")
    MAKEFILE_RESPONDS_ITEST_FAIL = failure(
        "The Makefile does not have a `make itest` target. Jenkins needs\n"
        "this and expects it to build and itest your docker image. More info:",
        "http://paasta.readthedocs.org/en/latest/about/contract.html"
    )
    MAKEFILE_RESPONDS_TEST = success("The Makefile responds to `make test`")
    MAKEFILE_RESPONDS_TEST_FAIL = failure(
        "The Makefile does not have a `make test` target. Jenkins needs\n"
        "this and expects it to run unit tests. More info:",
        "http://paasta.readthedocs.org/en/latest/about/contract.html"
    )
    MAKEFILE_HAS_A_TAB = success("The Makefile contains a tab character")
    MAKEFILE_HAS_NO_TABS = failure(
        "The Makefile contains no tab characters. Make sure you\n"
        "didn't accidentally paste spaces (which `make` does not respect)\n"
        "instead of a tab.",
        "http://paasta.readthedocs.org/en/latest/about/contract.html",
    )
    MAKEFILE_HAS_DOCKER_TAG = success("The Makefile contains a docker tag")
    MAKEFILE_HAS_NO_DOCKER_TAG = failure(
        "The Makefile contains no reference to DOCKER_TAG. Make sure you\n"
        "specify a DOCKER_TAG and that your itest tags your docker image with $DOCKER_TAG.",
        "http://paasta.readthedocs.org/en/latest/about/contract.html",
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
    def service_dir_found(service):
        message = "yelpsoa-config directory for %s found in /nail/etc/services" \
                  % PaastaColors.cyan(service)
        return success(message)

    @staticmethod
    def service_dir_missing(service):
        message = "Failed to locate yelpsoa-config directory for %s.\n" \
                  "  Please follow the guide linked below to get boilerplate." \
                  % service
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
            sys.stdout.write(PaastaColors.red(
                'Could not automatically detect instance to emulate. Please specify one with the --instance option.\n'))
            sys.exit(2)
        sys.stdout.write(PaastaColors.yellow(
            'Guessing instance configuration for %s. To override, use the --instance option.\n' % instance))
    return instance


def guess_cluster(service, args):
    """Returns the cluster from args if available, otherwise uses the "default" one"""
    if args.cluster:
        cluster = args.cluster
    else:
        try:
            cluster = get_default_cluster_for_service(service)
        except NoConfigurationForServiceError:
            sys.stdout.write(PaastaColors.red(
                'Could not automatically detect cluster to emulate. Please specify one with the --cluster option.\n'))
            sys.exit(2)
        sys.stdout.write(PaastaColors.yellow(
            'Guesing cluster configuration for %s. To override, use the --cluster option.\n' % cluster))
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


def list_services():
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


def list_instances():
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


def list_teams():
    """Loads team data from the system. Returns a set of team names (or empty
    set).
    """
    team_data = _load_sensu_team_data()
    teams = set(team_data.get('team_data', {}).keys())
    return teams


def calculate_remote_masters(cluster):
    """Given a cluster, do a DNS lookup of that cluster (which
    happens to point, eventually, to the Mesos masters in that cluster).
    Return IPs of those Mesos masters.
    """
    cluster_fqdn = "paasta-%s.yelp" % cluster
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


def run_paasta_serviceinit(subcommand, master, service, instancename, cluster, **kwargs):
    """Run 'paasta_serviceinit <subcommand>'. Return the output from running it."""
    if 'verbose' in kwargs and kwargs['verbose']:
        verbose_flag = "-v "
        timeout = 240
    else:
        verbose_flag = ''
        timeout = 60
    if 'app_id' in kwargs and kwargs['app_id']:
        app_id_flag = "--appid %s " % kwargs['app_id']
    else:
        app_id_flag = ''
    if 'delta' in kwargs and kwargs['delta']:
        delta = "--delta %s" % kwargs['delta']
    else:
        delta = ''
    command = 'ssh -A -n %s sudo paasta_serviceinit %s%s%s %s %s' % (
        master,
        verbose_flag,
        app_id_flag,
        compose_job_id(service, instancename),
        subcommand,
        delta
    )
    log.debug("Running Command: %s" % command)
    _, output = _run(command, timeout=timeout)
    return output


def execute_paasta_serviceinit_on_remote_master(subcommand, cluster, service, instancename, **kwargs):
    """Returns a string containing an error message if an error occurred.
    Otherwise returns the output of run_paasta_serviceinit_status().
    """
    masters, output = calculate_remote_masters(cluster)
    if masters == []:
        return 'ERROR: %s' % output
    master, output = find_connectable_master(masters)
    if not master:
        return (
            'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        )
    return run_paasta_serviceinit(subcommand, master, service, instancename, cluster, **kwargs)


def run_paasta_metastatus(master, verbose=0):
    if verbose > 0:
        verbose_flag = " -%s" % 'v'*verbose
        timeout = 120
    else:
        verbose_flag = ''
        timeout = 20
    command = 'ssh -A -n %s sudo paasta_metastatus%s' % (
        master,
        verbose_flag,
    )
    _, output = _run(command, timeout=timeout)
    return output


def execute_paasta_metastatus_on_remote_master(cluster, verbose=0):
    """Returns a string containing an error message if an error occurred.
    Otherwise returns the output of run_paasta_metastatus().
    """
    masters, output = calculate_remote_masters(cluster)
    if masters == []:
        return 'ERROR: %s' % output
    master, output = find_connectable_master(masters)
    if not master:
        return (
            'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        )
    return run_paasta_metastatus(master, verbose)


def lazy_choices_completer(list_func):
    def inner(prefix, **kwargs):
        options = list_func()
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


def figure_out_cluster(args):
    """Figures out and validates the input cluster name"""
    try:
        cluster = args.cluster or load_system_paasta_config().get_cluster()
    except IOError:
        # TODO: Read the new global paasta.json
        print "Sorry, could not detect the PaaSTA cluster. Please provide one"
        exit(1)
    return cluster


def get_pipeline_url(service):
    return PaastaColors.cyan(
        'https://jenkins.yelpcorp.com/view/services-%s' % service)


def get_jenkins_build_output_url():
    """Returns the URL for Jenkins job's output.
    Returns None if it's not available.
    """
    build_output = os.environ.get('BUILD_URL')
    if build_output:
        build_output = build_output + 'console'
    return build_output
