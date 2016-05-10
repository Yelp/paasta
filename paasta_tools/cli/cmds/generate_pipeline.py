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
"""Contains methods used by the paasta client to generate a Jenkins build
pipeline."""
import re

from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.monitoring_tools import get_team
from paasta_tools.monitoring_tools import get_team_email_address
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'generate-pipeline',
        help="Configures a Yelp-specific Jenkins build pipeline to match the 'deploy.yaml'",
        description=(
            "'paasta generate-pipeline' is a Yelp-specific tool to interact with Jenkins "
            "to build a build pipeline that matches what is declared in the 'deploy.yaml' "
            "for a service."
        ),
        epilog="Warning: Due to the Yelpisms in this tool, it is not currently useful to other organizations."
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to generate a Jenkins pipeline',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_generate_pipeline)


def paasta_generate_pipeline(args):
    """Generate a Jenkins build pipeline.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    service = args.service or guess_service_name()
    soa_dir = args.soa_dir
    try:
        validate_service_name(service, soa_dir=soa_dir)
    except NoSuchService as service_not_found:
        print service_not_found
        return 1

    generate_pipeline(service=service, soa_dir=soa_dir)


def validate_git_url_for_fab_repo(git_url):
    """fab_repo can only accept certain git urls, so this function will raise an
    exception if the git_url is not something fab_repo can handle."""
    if not git_url.startswith('git@git.yelpcorp.com:'):
        raise NotImplementedError(
            "fab_repo cannot currently handle git urls that look like: '%s'.\n"
            "They must start with 'git@git.yelpcorp.com:'" % git_url
        )
    return True


def get_git_repo_for_fab_repo(service, soa_dir):
    """Returns the 'repo' in fab_repo terms. fab_repo just wants the trailing
    section of the git_url, after the colon.
    """
    git_url = get_git_url(service, soa_dir=soa_dir)
    validate_git_url_for_fab_repo(git_url)
    repo = git_url.split(':')[1]
    return repo


def print_warning():
    print "Warning: paasta generate-pipeline is a Yelp-specific tool and will"
    print "not work outside Yelp."
    print ""
    print "paasta generate-pipeline is designed only to give a starting Jenkins"
    print "configuration that can be customized later"
    print ""
    print "Warning: running this tool on an existing pipeline will remove any"
    print "hand-made customizations and will leave behind orphaned Jenkins jobs"
    print "that need to be manually cleaned up."
    print ""
    raw_input("Press any key to continue or ctrl-c to cancel")


def generate_pipeline(service, soa_dir):
    email_address = get_team_email_address(service=service, soa_dir=soa_dir)
    repo = get_git_repo_for_fab_repo(service, soa_dir)
    if not email_address:
        owner = get_team(overrides={}, service=service, soa_dir=soa_dir)
    else:
        # fab_repo tacks on the domain, so we only want the first
        # part of the email.
        owner = re.sub('@.*', '', email_address)
    cmds = [
        'fab_repo setup_jenkins:services/%s,'
        'profile=paasta,job_disabled=False,owner=%s,repo=%s' % (service, owner, repo),
        'fab_repo setup_jenkins:services/%s,'
        'profile=paasta_boilerplate,owner=%s,repo=%s' % (service, owner, repo),
    ]
    print_warning()
    for cmd in cmds:
        print "INFO: Executing %s" % cmd
        returncode, output = _run(cmd, timeout=90)
        if returncode != 0:
            print "ERROR: Failed to generate Jenkins pipeline"
            print output
            return returncode
