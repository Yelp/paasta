#!/usr/bin/env python
"""Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready."""
import os
import re
import subprocess
import urllib2

from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_service_configuration
from paasta_tools.marathon_tools import list_clusters
from paasta_tools.marathon_tools import get_service_instance_list
from paasta_tools.monitoring_tools import get_team
from paasta_tools.paasta_cli.utils import guess_service_name
from paasta_tools.paasta_cli.utils import is_file_in_dir
from paasta_tools.paasta_cli.utils import NoSuchService
from paasta_tools.utils import DEPLOY_PIPELINE_NON_DEPLOY_STEPS
from paasta_tools.paasta_cli.utils import PaastaCheckMessages
from paasta_tools.paasta_cli.utils import PaastaColors
from paasta_tools.paasta_cli.utils import success
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.paasta_cli.utils import x_mark


def get_pipeline_config(service_name):
    service_configuration = read_service_configuration(service_name)
    return service_configuration.get('deploy', {}).get('pipeline', [])


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Execute 'paasta check' from service repo root",
        help="Determine whether service in pwd is paasta ready")
    check_parser.set_defaults(command=paasta_check)


def deploy_check(service_path):
    """Check whether deploy.yaml exists in service directory. Prints success or
    error message.

    :param service_path: path to a directory containing deploy.yaml"""
    if is_file_in_dir('deploy.yaml', service_path):
        print PaastaCheckMessages.DEPLOY_YAML_FOUND
    else:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING


def deploy_has_security_check(service_name):
    pipeline = get_pipeline_config(service_name)
    steps = [step['instancename'] for step in pipeline]
    if 'security-check' in steps:
        print PaastaCheckMessages.DEPLOY_SECURITY_FOUND
        return True
    else:
        print PaastaCheckMessages.DEPLOY_SECURITY_MISSING
        return False


def deploy_has_performance_check(service_name):
    pipeline = get_pipeline_config(service_name)
    steps = [step['instancename'] for step in pipeline]
    if 'performance-check' in steps:
        print PaastaCheckMessages.DEPLOY_PERFORMANCE_FOUND
        return True
    else:
        print PaastaCheckMessages.DEPLOY_PERFORMANCE_MISSING
        return False


def expose_8888_in_dockerfile(path):
    """Ensure Dockerfile contains line 'EXPOSE 8888'.

    :param path : path to a Dockerfile
    :return : A boolean that is True if the Dockerfile contains 'EXPOSE 8888'
    """
    pattern = re.compile('EXPOSE\s+8888.*')
    with open(path, 'r') as dockerfile:
        for line in dockerfile.readlines():
            if pattern.match(line):
                return True
        return False


def docker_file_reads_from_yelpcorp(path):
    """Ensure Dockerfile is valid.

    :param path : path to a Dockerfile
    :return : A boolean that is True if the Dockerfile reads from yelpcorp"""

    with open(path, 'r') as dockerfile:
        first_line = dockerfile.readline()
        if first_line.startswith("FROM docker-dev.yelpcorp.com"):
            return True
        else:
            return False


def docker_check():
    """Check whether Dockerfile exists in service directory, and is valid.
    Prints suitable message depending on outcome"""
    docker_file_path = is_file_in_dir('Dockerfile', os.getcwd())
    if docker_file_path:
        print PaastaCheckMessages.DOCKERFILE_FOUND

        if docker_file_reads_from_yelpcorp(docker_file_path):
            print PaastaCheckMessages.DOCKERFILE_YELPCORP
        else:
            print PaastaCheckMessages.DOCKERFILE_NOT_YELPCORP

        if expose_8888_in_dockerfile(docker_file_path):
            print PaastaCheckMessages.DOCKERFILE_EXPOSES_8888
        else:
            print PaastaCheckMessages.DOCKERFILE_DOESNT_EXPOSE_8888
    else:
        print PaastaCheckMessages.DOCKERFILE_MISSING


def makefile_responds_to_itest():
    """Runs `make -q itest` to detect if a makefile responds to an itest
    target."""
    args = ['make', '-q', 'itest']
    with open(os.devnull, 'w') as devnull:
        # Per the docs
        # http://www.gnu.org/software/make/manual/make.html#index-exit-status-of-make
        # 0 and 1 are ok. 2 Means Error
        # In question mode:
        # http://linux.die.net/man/1/make (see Exit Status)
        # 0 - Nothing to do
        # 1 - Things to do
        # 2 - Don't know what you are talking about
        rc = subprocess.call(args, stdout=devnull, stderr=devnull)
        return rc in [0, 1]


def makefile_check():
    """Detects if you have a makefile and runs some sanity tests against
    it to ensure it is paasta-ready"""
    makefile_path = is_file_in_dir('Makefile', os.getcwd())
    if makefile_path:
        print PaastaCheckMessages.MAKEFILE_FOUND
        if makefile_responds_to_itest():
            print PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST
        else:
            print PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST_FAIL
    else:
        print PaastaCheckMessages.MAKEFILE_MISSING


def git_repo_check(service_name):
    devnull = open(os.devnull, 'w')
    cmd = 'git'
    args = [cmd, 'ls-remote', 'git@git.yelpcorp.com:services/%s' % service_name]
    if subprocess.call(args, stdout=devnull, stderr=devnull) == 0:
        print PaastaCheckMessages.GIT_REPO_FOUND
    else:
        print PaastaCheckMessages.git_repo_missing(service_name)


def marathon_check(service_path):
    """Check whether a marathon yaml file exists in service directory, and
    print success/failure message.

    :param service_path: path to a directory containing the marathon yaml
    files"""
    if is_file_in_dir('marathon*.yaml', service_path):
        print PaastaCheckMessages.MARATHON_YAML_FOUND
    else:
        print PaastaCheckMessages.MARATHON_YAML_MISSING


def get_marathon_steps(service_name):
    """This is a kind of funny function that gets all the marathon instances
    for a service and massages it into a form that matches up with what
    deploy.yaml's steps look like. This is only so we can compare it 1-1
    with what deploy.yaml has for linting."""
    steps = []
    for cluster in list_clusters(service_name):
        for instance in get_service_instance_list(service_name, cluster):
            steps.append("%s.%s" % (cluster, instance[1]))
    return steps


def marathon_deployments_check(service_name):
    """Checks for consistency between deploy.yaml and the marathon yamls"""
    the_return = True
    pipeline_deployments = get_pipeline_config(service_name)
    pipeline_steps = [step['instancename'] for step in pipeline_deployments]
    pipeline_steps = [step for step in pipeline_steps if step not in DEPLOY_PIPELINE_NON_DEPLOY_STEPS]
    marathon_steps = get_marathon_steps(service_name)
    in_marathon_not_deploy = set(marathon_steps) - set(pipeline_steps)
    if len(in_marathon_not_deploy) > 0:
        print "%s There are some instance(s) you have asked to run in marathon that" % x_mark()
        print "  do not have a corresponding entry in deploy.yaml:"
        print "  %s" % PaastaColors.bold(", ".join(in_marathon_not_deploy))
        print "  You should probably add entries to deploy.yaml for them so they"
        print "  are deployed to those clusters."
        the_return = False
    in_deploy_not_marathon = set(pipeline_steps) - set(marathon_steps)
    if len(in_deploy_not_marathon) > 0:
        print "%s There are some instance(s) in deploy.yaml that are not referenced" % x_mark()
        print "  by any marathon instance:"
        print "  %s" % PaastaColors.bold((", ".join(in_deploy_not_marathon)))
        print "  You should probably delete these deploy.yaml entries if they are unused."
        the_return = False
    if the_return is True:
        print success("All entries in deploy.yaml correspond to a marathon entry")
        print success("All marathon instances have a corresponding deploy.yaml entry")
    return the_return


def pipeline_check(service_name):
    url = "https://jenkins.yelpcorp.com/view/services-%s/api/xml" % service_name
    try:
        req_status = urllib2.urlopen(url).getcode()
        if req_status == 200:
            print PaastaCheckMessages.PIPELINE_FOUND
        else:
            print PaastaCheckMessages.PIPELINE_MISSING
    except urllib2.HTTPError:
        print PaastaCheckMessages.PIPELINE_MISSING


def sensu_check(service_name, service_path):
    """Check whether monitoring.yaml exists in service directory,
    and that the team name is declared.

    :param service_name: name of service currently being examined
    :param service_path: path to loction of monitoring.yaml file"""
    if is_file_in_dir('monitoring.yaml', service_path):
        print PaastaCheckMessages.SENSU_MONITORING_FOUND
        team = get_team(None, service_name)
        if team is None:
            print PaastaCheckMessages.SENSU_TEAM_MISSING
        else:
            print PaastaCheckMessages.sensu_team_found(team)
    else:
        print PaastaCheckMessages.SENSU_MONITORING_MISSING


def service_dir_check(service_name):
    """Check whether directory service_name exists in /nail/etc/services
    :param service_name: string of service name we wish to inspect
    """
    try:
        validate_service_name(service_name)
        print PaastaCheckMessages.service_dir_found(service_name)
    except NoSuchService:
        print PaastaCheckMessages.service_dir_missing(service_name)


def smartstack_check(service_name, service_path):
    """Check whether smartstack.yaml exists in service directory, and the proxy
    ports are declared.  Print appropriate message depending on outcome.

    :param service_name: name of service currently being examined
    :param service_path: path to loction of smartstack.yaml file"""
    if is_file_in_dir('smartstack.yaml', service_path):
        print PaastaCheckMessages.SMARTSTACK_YAML_FOUND
        smartstack_dict = read_extra_service_information(
            service_name, 'smartstack')
        instances = smartstack_dict.keys()
        if instances:
            no_ports_found = True
            for instance in instances:
                if 'proxy_port' in smartstack_dict[instance]:
                    no_ports_found = False
                    print PaastaCheckMessages.smartstack_port_found(
                        instance, smartstack_dict[instance]['proxy_port'])
            if no_ports_found:
                print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
        else:
            print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
    else:
        print PaastaCheckMessages.SMARTSTACK_YAML_MISSING


def paasta_check(args):
    """Analyze the service in the PWD to determine if it is paasta ready
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = guess_service_name()
    service_path = os.path.join('/nail/etc/services', service_name)

    service_dir_check(service_name)
    deploy_check(service_path)
    deploy_has_security_check(service_name)
    deploy_has_performance_check(service_name)
    pipeline_check(service_name)
    git_repo_check(service_name)
    docker_check()
    makefile_check()
    marathon_check(service_path)
    marathon_deployments_check(service_path)
    sensu_check(service_name, service_path)
    smartstack_check(service_name, service_path)
