#!/usr/bin/env python
from argcomplete.completers import ChoicesCompleter
import datetime
import dulwich.client
import fnmatch
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.generate_deployments_json import get_branches_for_service
from paasta_tools import utils
import re
import sys

SOA_DIR = '/nail/etc/services'


def completer_branches(prefix, parsed_args, **kwargs):
    branches = get_branches_for_service(SOA_DIR, parsed_args.service)
    return (b for b in branches if b.startswith(prefix))


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'start',
        description="Starts a PaaSTA service",
        help="Restarts a PaaSTA service by asking Marathon to suspend/resume.")
    status_parser.add_argument(
        '-s', '--service',
        help='Service that you want to start. Like example_service.',
        required=True,
    ).completer = ChoicesCompleter(list_services())
    status_parser.add_argument(
        '-b', '--branch',
        help="""Branch of the service that you want to restart. Like
                "norcal-prod.main" or "pnw-stagea.canary". This can be a
                glob-style pattern to match multiple branches.""",
        action='append',
        required=True,
    ).completer = completer_branches
    status_parser.set_defaults(command=paasta_start)


class NoBranchesMatchException(Exception):
    def __init__(self, unmatched_pattern):
        self.unmatched_pattern = unmatched_pattern


def match_branches(all_branches, branch_patterns):
    matched = set()
    for pattern in branch_patterns:
        regex = fnmatch.translate(pattern)
        found_match = False
        for branch in all_branches:
            if re.match(regex, branch):
                matched.add(branch)
                found_match = True
        if not found_match:
            raise NoBranchesMatchException(pattern)

    return matched


def format_timestamp(dt=None):
    if dt is None:
        dt = datetime.datetime.utcnow()
    return dt.strftime('%Y%m%dT%H%M%S')


def format_tag(branch, force_bounce, desired_state):
    return 'refs/tags/paasta-%s-%s-%s' % (branch, force_bounce, desired_state)


def make_determine_wants_func(branches, force_bounce, desired_state):
    def determine_wants(refs):
        for branch in branches:
            refs[format_tag(branch, force_bounce, desired_state)] = \
                refs['refs/heads/%s' % branch]
        return refs

    return determine_wants


def issue_start_for_branches(service, branches, force_bounce):
    client, path = dulwich.client.get_transport_and_path(
        utils.get_git_url(service)
    )

    client.send_pack(
        path,
        make_determine_wants_func(
            branches=branches,
            force_bounce=force_bounce,
            desired_state='start'
        ),
        lambda have, want: []  # We know we don't need to push any objects.
    )


def paasta_start(args):
    """Issues a start for given branches of a service."""
    service = args.service
    branch_patterns = args.branch  # this is a list because of action='append'
    all_branches = get_branches_for_service(SOA_DIR, service)
    try:
        branches = match_branches(all_branches, branch_patterns)
    except NoBranchesMatchException as e:
        print "No branches found for %s that match %s" % \
            (service, e.unmatched_pattern)
        sys.exit(1)

    force_bounce = format_timestamp(datetime.datetime.utcnow())
    issue_start_for_branches(service, branches, force_bounce)
