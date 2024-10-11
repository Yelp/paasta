#!/usr/bin/env python
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
"""Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready."""
import os
import re

from paasta_tools.cli.cmds.validate import paasta_validate_soa_configs
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_file_contents
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import is_file_in_dir
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.cli.utils import success
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import x_mark
from paasta_tools.long_running_service_tools import get_all_namespaces_for_service
from paasta_tools.monitoring_tools import get_team
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_pipeline_config
from paasta_tools.utils import get_pipeline_deploy_groups
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import INSTANCE_TYPES
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    help_text = (
        "Determine whether service in pwd is 'paasta ready', checking for common "
        "mistakes in the soa-configs directory and the local service directory. This "
        "command is designed to be run from the 'root' of a service directory."
    )
    check_parser = subparsers.add_parser("check", description=help_text, help=help_text)
    check_parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Defaults to autodetect.",
    ).completer = lazy_choices_completer(list_services)
    check_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    check_parser.set_defaults(command=paasta_check)


def deploy_check(service_path):
    """Check whether deploy.yaml exists in service directory. Prints success or
    error message.

    :param service_path: path to a directory containing deploy.yaml"""
    if is_file_in_dir("deploy.yaml", service_path):
        print(PaastaCheckMessages.DEPLOY_YAML_FOUND)
    else:
        print(PaastaCheckMessages.DEPLOY_YAML_MISSING)


def deploy_has_security_check(service, soa_dir):
    pipeline = get_pipeline_config(service=service, soa_dir=soa_dir)
    steps = [step["step"] for step in pipeline if not step.get("parallel")]
    steps += [
        substep["step"]
        for step in pipeline
        if step.get("parallel")
        for substep in step.get("parallel")
    ]
    if "security-check" in steps:
        print(PaastaCheckMessages.DEPLOY_SECURITY_FOUND)
        return True
    else:
        print(PaastaCheckMessages.DEPLOY_SECURITY_MISSING)
        return False


def docker_check():
    """Check whether Dockerfile exists in service directory, and is valid.
    Prints suitable message depending on outcome"""
    docker_file_path = is_file_in_dir("Dockerfile", os.getcwd())
    if docker_file_path:
        print(PaastaCheckMessages.DOCKERFILE_FOUND)
    else:
        print(PaastaCheckMessages.DOCKERFILE_MISSING)


def makefile_responds_to(target):
    """Runs `make --question <target>` to detect if a makefile responds to the
    specified target."""
    # According to http://www.gnu.org/software/make/manual/make.html#index-exit-status-of-make,
    # 0 means OK, 1 means the target is not up to date, and 2 means error
    returncode, _ = _run(["make", "--question", target], timeout=5)
    return returncode != 2


def makefile_has_a_tab(makefile_path):
    contents = get_file_contents(makefile_path)
    return "\t" in contents


def makefile_has_docker_tag(makefile_path):
    contents = get_file_contents(makefile_path)
    return re.search(r"DOCKER_TAG\s*\?=", contents, re.MULTILINE) is not None


def makefile_check():
    """Detects if you have a makefile and runs some sanity tests against
    it to ensure it is paasta-ready"""
    makefile_path = is_file_in_dir("Makefile", os.getcwd())
    if makefile_path:
        print(PaastaCheckMessages.MAKEFILE_FOUND)

        if makefile_has_a_tab(makefile_path):
            print(PaastaCheckMessages.MAKEFILE_HAS_A_TAB)
        else:
            print(PaastaCheckMessages.MAKEFILE_HAS_NO_TABS)

        if makefile_has_docker_tag(makefile_path):
            print(PaastaCheckMessages.MAKEFILE_HAS_DOCKER_TAG)
        else:
            print(PaastaCheckMessages.MAKEFILE_HAS_NO_DOCKER_TAG)

        if makefile_responds_to("cook-image"):
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_BUILD_IMAGE)
        else:
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_BUILD_IMAGE_FAIL)

        if makefile_responds_to("itest"):
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST)
        else:
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_ITEST_FAIL)

        if makefile_responds_to("test"):
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_TEST)
        else:
            print(PaastaCheckMessages.MAKEFILE_RESPONDS_TEST_FAIL)
    else:
        print(PaastaCheckMessages.MAKEFILE_MISSING)


def git_repo_check(service, soa_dir):
    git_url = get_git_url(service, soa_dir)
    cmd = "git ls-remote %s" % git_url
    returncode, _ = _run(cmd, timeout=5)
    if returncode == 0:
        print(PaastaCheckMessages.GIT_REPO_FOUND)
    else:
        print(PaastaCheckMessages.git_repo_missing(git_url))


def get_deploy_groups_used_by_framework(instance_type, service, soa_dir):
    """This is a kind of funny function that gets all the instances for specified
    service and framework, and massages it into a form that matches up with what
    deploy.yaml's steps look like. This is only so we can compare it 1-1
    with what deploy.yaml has for linting.

    :param instance_type: one of the entries in utils.INSTANCE_TYPES
    :param service: the service name
    :param soa_dir: The SOA configuration directory to read from

    :returns: a list of deploy group names used by the service.
    """

    deploy_groups = []
    for cluster in list_clusters(service, soa_dir):
        for _, instance in get_service_instance_list(
            service=service,
            cluster=cluster,
            instance_type=instance_type,
            soa_dir=soa_dir,
        ):
            try:
                config = get_instance_config(
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    soa_dir=soa_dir,
                    load_deployments=False,
                    instance_type=instance_type,
                )
                deploy_groups.append(config.get_deploy_group())
            except NotImplementedError:
                pass
    return set(filter(None, deploy_groups))


def deployments_check(service, soa_dir):
    """Checks for consistency between deploy.yaml and the kubernetes/etc yamls"""
    the_return = True
    pipeline_deploy_groups = get_pipeline_deploy_groups(
        service=service, soa_dir=soa_dir
    )

    framework_deploy_groups = {}
    in_deploy_not_frameworks = set(pipeline_deploy_groups)
    for it in INSTANCE_TYPES:
        framework_deploy_groups[it] = get_deploy_groups_used_by_framework(
            it, service, soa_dir
        )
        in_framework_not_deploy = set(framework_deploy_groups[it]) - set(
            pipeline_deploy_groups
        )
        in_deploy_not_frameworks -= set(framework_deploy_groups[it])
        if len(in_framework_not_deploy) > 0:
            print(
                "{} There are some instance(s) you have asked to run in {} that".format(
                    x_mark(), it
                )
            )
            print("  do not have a corresponding entry in deploy.yaml:")
            print("  %s" % PaastaColors.bold(", ".join(in_framework_not_deploy)))
            print("  You should probably configure these to use a 'deploy_group' or")
            print(
                "  add entries to deploy.yaml for them so they are deployed to those clusters."
            )
            the_return = False

    if len(in_deploy_not_frameworks) > 0:
        print(
            "%s There are some instance(s) in deploy.yaml that are not referenced"
            % x_mark()
        )
        print("  by any instance:")
        print("  %s" % PaastaColors.bold((", ".join(in_deploy_not_frameworks))))
        print(
            "  You should probably delete these deploy.yaml entries if they are unused."
        )
        the_return = False

    if the_return is True:
        print(success("All entries in deploy.yaml correspond to a paasta instance"))
        for it in INSTANCE_TYPES:
            if len(framework_deploy_groups[it]) > 0:
                print(
                    success(
                        "All %s instances have a corresponding deploy.yaml entry" % it
                    )
                )
    return the_return


def sensu_check(service, service_path, soa_dir):
    """Check whether monitoring.yaml exists in service directory,
    and that the team name is declared.

    :param service: name of service currently being examined
    :param service_path: path to location of monitoring.yaml file"""
    if is_file_in_dir("monitoring.yaml", service_path):
        print(PaastaCheckMessages.SENSU_MONITORING_FOUND)
        team = get_team(service=service, overrides={}, soa_dir=soa_dir)
        if team is None:
            print(PaastaCheckMessages.SENSU_TEAM_MISSING)
        else:
            print(PaastaCheckMessages.sensu_team_found(team))
    else:
        print(PaastaCheckMessages.SENSU_MONITORING_MISSING)


def service_dir_check(service, soa_dir):
    """Check whether directory service exists in /nail/etc/services
    :param service: string of service name we wish to inspect
    """
    try:
        validate_service_name(service, soa_dir)
        print(PaastaCheckMessages.service_dir_found(service, soa_dir))
    except NoSuchService:
        print(PaastaCheckMessages.service_dir_missing(service, soa_dir))


def smartstack_check(service, service_path, soa_dir):
    """Check whether smartstack.yaml exists in service directory, and the proxy
    ports are declared.  Print appropriate message depending on outcome.

    :param service: name of service currently being examined
    :param service_path: path to location of smartstack.yaml file"""
    if is_file_in_dir("smartstack.yaml", service_path):
        print(PaastaCheckMessages.SMARTSTACK_YAML_FOUND)
        instances = get_all_namespaces_for_service(service=service, soa_dir=soa_dir)
        if len(instances) > 0:
            for namespace, config in get_all_namespaces_for_service(
                service=service, soa_dir=soa_dir, full_name=False
            ):
                if "proxy_port" in config:
                    print(
                        PaastaCheckMessages.smartstack_port_found(
                            namespace, config.get("proxy_port")
                        )
                    )
                else:
                    print(PaastaCheckMessages.SMARTSTACK_PORT_MISSING)
        else:
            print(PaastaCheckMessages.SMARTSTACK_PORT_MISSING)


def paasta_check(args):
    """Analyze the service in the PWD to determine if it is paasta ready
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.yelpsoa_config_root
    service = figure_out_service_name(args, soa_dir)
    service_path = os.path.join(soa_dir, service)

    service_dir_check(service, soa_dir)
    deploy_check(service_path)
    deploy_has_security_check(service, soa_dir)
    git_repo_check(service, soa_dir)
    docker_check()
    makefile_check()
    deployments_check(service, soa_dir)
    sensu_check(service, service_path, soa_dir)
    smartstack_check(service, service_path, soa_dir)
    paasta_validate_soa_configs(service, service_path)


def read_dockerfile_lines(path):
    with open(path, "r") as dockerfile:
        return dockerfile.readlines()
