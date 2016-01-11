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

"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""

import sys

from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import get_paasta_branch
from paasta_tools import remote_git


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'mark-for-deployment',
        help='Mark a docker image for deployment in git',
        description=(
            "'paasta mark-for-deployment' uses Git as the control-plane, to "
            "signal to other PaaSTA components that a particular docker image "
            "is ready to be deployed."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service are required "
            "for this command to work."
        )
    )
    list_parser.add_argument(
        '-u', '--git-url',
        help='Git url for service -- where magic mark-for-deployment branches are pushed',
        required=True,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha to mark for deployment',
        required=True,
    )
    list_parser.add_argument(
        '-l', '--clusterinstance',
        help='Mark the service ready for deployment in this clusterinstance (e.g. '
             'cluster1.canary, cluster2.main)',
        required=True,
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to mark for deployment. Leading '
        '"services-" will be stripped.',
        required=True,
    )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(git_url, cluster, instance, service, commit):
    """Mark a docker image for deployment"""
    remote_branch = get_paasta_branch(cluster=cluster, instance=instance)
    ref_mutator = remote_git.make_force_push_mutate_refs_func(
        target_branches=[remote_branch],
        sha=commit,
    )
    try:
        remote_git.create_remote_refs(git_url=git_url, ref_mutator=ref_mutator, force=True)
    except Exception as e:
        loglines = ["Failed to mark %s in for deployment on %s in the %s cluster!" % (commit, instance, cluster)]
        for line in str(e).split('\n'):
            loglines.append(line)
        return_code = 1
    else:
        loglines = ["Marked %s in for deployment on %s in the %s cluster" % (commit, instance, cluster)]
        return_code = 0

    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance,
        )
    return return_code


def paasta_mark_for_deployment(args):
    """Wrapping mark_for_deployment"""
    cluster, instance = args.clusterinstance.split('.')
    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service)
    returncode = mark_for_deployment(
        git_url=args.git_url,
        cluster=cluster,
        instance=instance,
        service=service,
        commit=args.commit
    )
    sys.exit(returncode)
