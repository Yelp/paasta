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

from paasta_tools.paasta_cli.utils import get_jenkins_build_output_url
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import get_paasta_branch
from paasta_tools import remote_git


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'mark-for-deployment',
        description='Mark a docker image for deployment',
        help='Mark a docker image for deployment')

    list_parser.add_argument('-u', '--git-url',
                             help='Git url for service -- where magic mark-for-deployment branches are pushed',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha to mark for deployment',
                             required=True,
                             )
    list_parser.add_argument('-l', '--clusterinstance',
                             help='Mark the service ready for deployment in this clusterinstance (e.g. '
                                  'cluster1.canary, cluster2.main)',
                             required=True,
                             )
    list_parser.add_argument('-s', '--service',
                             help='Name of the service which you wish to mark for deployment. Leading '
                             '"services-" will be stripped.',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def build_command(
    upstream_git_url,
    upstream_git_commit,
    cluster,
    instance,
):
    """upstream_git_url is the Git URL where the service lives (e.g.
    git@git.yelpcorp.com:services/foo)

    instancename is where you want to deploy. E.g. cluster1.canary indicates
    a Mesos cluster (cluster1) and an instance within that cluster (canary)
    """
    cmd = 'git push -f %s %s:refs/heads/paasta-%s.%s' % (
        upstream_git_url,
        upstream_git_commit,
        cluster,
        instance,
    )
    return cmd


def get_loglines(returncode, cmd, output, commit, cluster, instance):
    loglines = []
    if returncode != 0:
        loglines.append('ERROR: Failed to mark %s for deployment in %s.%s.' % (commit, cluster, instance))
        loglines.append("Ran: '%s'" % cmd)
        loglines.append("Output: %s" % output)
        output_url = get_jenkins_build_output_url()
        if output_url:
            loglines.append('See Jenkins output at %s' % output)
    else:
        loglines.append('Marked %s in %s.%s for deployment.' % (commit, cluster, instance))
    return loglines


def mark_for_deployment(git_url, cluster, instance, service, commit):
    """Mark a docker image for deployment"""
    cmd = build_command(git_url, commit, cluster=cluster, instance=instance)
    # Clusterinstance should be in cluster.instance format
    returncode, output = _run(
        cmd,
        timeout=30,
    )
    loglines = get_loglines(
        returncode=returncode,
        cmd=cmd,
        output=output,
        commit=commit,
        cluster=cluster,
        instance=instance
    )
    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance,
        )
    return returncode


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
