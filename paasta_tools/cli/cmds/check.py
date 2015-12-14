#!/usr/bin/env python
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

"""Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready."""
import os
import re
import urllib2

from paasta_tools.marathon_tools import get_all_namespaces_for_service
from paasta_tools.monitoring_tools import get_team
from paasta_tools.cli.utils import get_file_contents
from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import is_file_in_dir
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.cli.utils import success
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import x_mark
from paasta_tools.utils import DEPLOY_PIPELINE_NON_DEPLOY_STEPS
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_clusters
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import _run
from service_configuration_lib import read_service_configuration


def get_pipeline_config(service):
    service_configuration = read_service_configuration(service)
    return service_configuration.get('deploy', {}).get('pipeline', [])


def add_subparser(subparsers):
    help_text = (
        "Determine whether service in pwd is 'paasta ready', checking for common "
        "mistakes in the soa-configs directory and the local service directory. This "
        "command is designed to be run from the 'root' of a service directory."
    )
    check_parser = subparsers.add_parser(
        'check',
        description=help_text,
        help=help_text,
    )
    check_parser.set_defaults(command=paasta_check)


def deploy_check(service_path):
    """Check whether deploy.yaml exists in service directory. Prints success or
    error message.

    :param service_path: path to a directory containing deploy.yaml"""
    if is_file_in_dir('deploy.yaml', service_path):
        print PaastaCheckMessages.DEPLOY_YAML_FOUND
    else:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING


def deploy_has_security_check(service):
    pipeline = get_pipeline_config(service)
    steps = [step['instancename'] for step in pipeline]
    if 'security-check' in steps:
        print PaastaCheckMessages.DEPLOY_SECURITY_FOUND
        return True
    else:
        print PaastaCheckMessages.DEPLOY_SECURITY_MISSING
        return False


def deploy_has_performance_check(service):
    pipeline = get_pipeline_config(service)
    steps = [step['instancename'] for step in pipeline]
    if 'performance-check' in steps:
        print PaastaCheckMessages.DEPLOY_PERFORMANCE_FOUND
        return True
    else:
        print PaastaCheckMessages.DEPLOY_PERFORMANCE_MISSING
        return False


def docker_check():
    """Check whether Dockerfile exists in service directory, and is valid.
    Prints suitable message depending on outcome"""
    docker_file_path = is_file_in_dir('Dockerfile', os.getcwd())
    if docker_file_path:
        print PaastaCheckMessages.DOCKERFILE_FOUND
    else:
        print PaastaCheckMessages.DOCKERFILE_MISSING


def makefile_responds_to(target):
    """Runs `make -q <target>` to detect if a makefile responds to the
    specified target."""
    cmd = 'make -q %s' % target
    # Per the docs
    # http://www.gnu.org/software/make/manual/make.html#index-exit-status-of-make
    # 0 and 1 are ok. 2 Means Error
    # In question mode:
    # http://linux.die.net/man/1/make (see Exit Status)
    # 0 - Nothing to do
    # 1 - Things to do
    # 2 - Don't know what you are talking about
    returncode, _ = _run(cmd, timeout=5)
    return returncode in [0, 1]


def makefile_has_a_tab(makefile_path):
    contents = get_file_contents(makefile_path)
    return '\t' in contents


def makefile_has_docker_tag(makefile_path):
    contents = get_file_contents(makefile_path)
    return re.search(r"^DOCKER_TAG\s*\?=", contents, re.MULTILINE) is not None


def makefile_check():
    """Detects if you have a makefile and runs some sanity tests against
    it to ensure it is paasta-ready"""
    makefile_path = is_file_in_dir('Makefile', os.getcwd())
    if makefile_path:
        print PaastaCheckMessages.MAKEFILE_FOUND

        if makefile_has_a_tab(makefile_path):
            print PaastaCheckMessages.MAKEFILE_HAS_A_TAB
        else:
            print PaastaCheckMessages.MAKEFILE_HAS_NO_TABS

        if makefile_has_docker_tag(makefile_path):
            print PaastaCheckMessages.MAKEFILE_HAS_DOCKER_TAG
        else:
            print PaastaCheckMessages.MAKEFILE_HAS_NO_DOCKER_TAG

        if makefile_responds_to('cook-image'):
            print PaastaCheckMessages.MAKEFILE_RESPONDS_BUILD_IMAGE
        else:
            print PaastaCheckMessages.MAKEFILE_RESPONDS_BUILD_IMAGE_FAIL

        if makefile_responds_to('itest'):
            print PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST
        else:
            print PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST_FAIL

        if makefile_responds_to('test'):
            print PaastaCheckMessages.MAKEFILE_RESPONDS_TEST
        else:
            print PaastaCheckMessages.MAKEFILE_RESPONDS_TEST_FAIL
    else:
        print PaastaCheckMessages.MAKEFILE_MISSING


def git_repo_check(service):
    git_url = get_git_url(service)
    cmd = 'git ls-remote %s' % git_url
    returncode, _ = _run(cmd, timeout=5)
    if returncode == 0:
        print PaastaCheckMessages.GIT_REPO_FOUND
    else:
        print PaastaCheckMessages.git_repo_missing(git_url)


def marathon_check(service_path):
    """Check whether a marathon yaml file exists in service directory, and
    print success/failure message.

    :param service_path: path to a directory containing the marathon yaml
                         files
    """
    if is_file_in_dir('marathon*.yaml', service_path):
        print PaastaCheckMessages.MARATHON_YAML_FOUND
    else:
        print PaastaCheckMessages.MARATHON_YAML_MISSING


def get_marathon_steps(service):
    """This is a kind of funny function that gets all the marathon instances
    for a service and massages it into a form that matches up with what
    deploy.yaml's steps look like. This is only so we can compare it 1-1
    with what deploy.yaml has for linting."""
    steps = []
    for cluster in list_clusters(service):
        for instance in get_service_instance_list(service, cluster=cluster, instance_type='marathon'):
            steps.append("%s.%s" % (cluster, instance[1]))
    return steps


def marathon_deployments_check(service):
    """Checks for consistency between deploy.yaml and the marathon yamls"""
    the_return = True
    pipeline_deployments = get_pipeline_config(service)
    pipeline_steps = [step['instancename'] for step in pipeline_deployments]
    pipeline_steps = [step for step in pipeline_steps if step not in DEPLOY_PIPELINE_NON_DEPLOY_STEPS]
    marathon_steps = get_marathon_steps(service)
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


def pipeline_check(service):
    url = "https://jenkins.yelpcorp.com/view/services-%s/api/xml" % service
    try:
        req_status = urllib2.urlopen(url).getcode()
        if req_status == 200:
            print PaastaCheckMessages.PIPELINE_FOUND
        else:
            print PaastaCheckMessages.PIPELINE_MISSING
    except urllib2.HTTPError:
        print PaastaCheckMessages.PIPELINE_MISSING


def sensu_check(service, service_path):
    """Check whether monitoring.yaml exists in service directory,
    and that the team name is declared.

    :param service: name of service currently being examined
    :param service_path: path to loction of monitoring.yaml file"""
    if is_file_in_dir('monitoring.yaml', service_path):
        print PaastaCheckMessages.SENSU_MONITORING_FOUND
        team = get_team(service=service, overrides={})
        if team is None:
            print PaastaCheckMessages.SENSU_TEAM_MISSING
        else:
            print PaastaCheckMessages.sensu_team_found(team)
    else:
        print PaastaCheckMessages.SENSU_MONITORING_MISSING


def service_dir_check(service):
    """Check whether directory service exists in /nail/etc/services
    :param service: string of service name we wish to inspect
    """
    try:
        validate_service_name(service)
        print PaastaCheckMessages.service_dir_found(service)
    except NoSuchService:
        print PaastaCheckMessages.service_dir_missing(service)


def smartstack_check(service, service_path):
    """Check whether smartstack.yaml exists in service directory, and the proxy
    ports are declared.  Print appropriate message depending on outcome.

    :param service: name of service currently being examined
    :param service_path: path to loction of smartstack.yaml file"""
    if is_file_in_dir('smartstack.yaml', service_path):
        print PaastaCheckMessages.SMARTSTACK_YAML_FOUND
        instances = get_all_namespaces_for_service(service)
        if len(instances) > 0:
            for namespace, config in get_all_namespaces_for_service(service, full_name=False):
                if 'proxy_port' in config:
                    print PaastaCheckMessages.smartstack_port_found(
                        namespace, config.get('proxy_port'))
                else:
                    print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
        else:
            print PaastaCheckMessages.SMARTSTACK_PORT_MISSING


def paasta_check(args):
    """Analyze the service in the PWD to determine if it is paasta ready
    :param args: argparse.Namespace obj created from sys.args by cli"""
    service = guess_service_name()
    service_path = os.path.join('/nail/etc/services', service)

    service_dir_check(service)
    deploy_check(service_path)
    deploy_has_security_check(service)
    deploy_has_performance_check(service)
    pipeline_check(service)
    git_repo_check(service)
    docker_check()
    makefile_check()
    marathon_check(service_path)
    marathon_deployments_check(service_path)
    sensu_check(service, service_path)
    smartstack_check(service, service_path)


def read_dockerfile_lines(path):
    with open(path, 'r') as dockerfile:
        return dockerfile.readlines()
