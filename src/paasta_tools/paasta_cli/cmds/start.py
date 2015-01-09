#!/usr/bin/env python
from argcomplete.completers import ChoicesCompleter
import contextlib
import datetime
import dulwich.client
import fnmatch
import git
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.generate_deployments_json import (
    get_remote_branches_for_service,
    get_branches_for_service
)
from paasta_tools import utils
import re
import shutil
import sys
import tempfile

SOA_DIR = '/nail/etc/services'


@contextlib.contextmanager
def why_isnt_mkdtemp_already_a_contextmanager():
    # seriously though this is so easy. Why is this not in the stdlib?
    d = tempfile.mkdtemp()
    try:
        yield d
    finally:
        shutil.rmtree(d)


def get_branches(service):
    paasta_branches = set(get_branches_for_service(SOA_DIR, service))
    with why_isnt_mkdtemp_already_a_contextmanager() as temp_dir:
        mygit = git.Git(temp_dir)
        for sha, branch in get_remote_branches_for_service(mygit, service):
            if branch in paasta_branches:
                yield branch


def completer_branches(prefix, parsed_args, **kwargs):
    try:
        branches = get_branches(parsed_args.service)
        return (b for b in branches if b.startswith(prefix))
    except Exception as e:
        from argcomplete import warn
        warn(e)


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
    """Create a function that will inform send_pack that we want to create tags
    corresponding to the set of branches passed, with the given force_bounce
    and desired_state parameters. These tags will point at the current tip of
    the branch they associate with.

    dulwich's send_pack wants a function that takes a dictionary of ref name
    to sha and returns a modified version of that dictionary. send_pack will
    then diff what is returned versus what was passed in, and inform the remote
    git repo of our desires."""
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
    all_branches = get_branches(service)
    try:
        branches = match_branches(all_branches, branch_patterns)
    except NoBranchesMatchException as e:
        print "No branches found for %s that match %s" % \
            (service, e.unmatched_pattern)
        sys.exit(1)

    force_bounce = format_timestamp(datetime.datetime.utcnow())
    issue_start_for_branches(service, branches, force_bounce)
