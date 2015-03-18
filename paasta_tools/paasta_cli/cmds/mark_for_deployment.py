#!/usr/bin/env python
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""

import sys

from paasta_tools.paasta_cli.utils import get_jenkins_build_output_url
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run


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

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def build_command(
    upstream_git_url,
    upstream_git_commit,
    clusterinstance,
):
    """upstream_git_url is the Git URL where the service lives (e.g.
    git@git.yelpcorp.com:services/foo)

    instancename is where you want to deploy. E.g. cluster1.canary indicates
    a Mesos cluster (cluster1) and an instance within that cluster (canary)
    """
    cmd = 'git push -f %s %s:refs/heads/paasta-%s' % (
        upstream_git_url,
        upstream_git_commit,
        clusterinstance,
    )
    return cmd


def paasta_mark_for_deployment(args):
    """Mark a docker image for deployment"""
    cmd = build_command(args.git_url, args.commit, args.clusterinstance)
    # Git repo URL's basename should be the service_name
    service_name = args.git_url.split('/')[-1]
    validate_service_name(service_name)
    # Clusterinstance should be in cluster.instance format
    cluster, instance = args.clusterinstance.split('.')
    loglines = []
    returncode, output = _run(
        cmd,
        timeout=30,
        log=True,
        component='deploy',
        loglevel='debug',
        service_name=service_name,
        cluster=cluster,
        instance=instance
    )
    if returncode != 0:
        loglines.append('ERROR: Failed to mark %s for deployment in %s.' % (args.commit, args.clusterinstance))
        output = get_jenkins_build_output_url()
        if output:
            loglines.append('See output: %s' % output)
    else:
        loglines.append('Marked %s in %s for deployment.' % (args.commit, args.clusterinstance))
    for logline in loglines:
        _log(
            service_name=service_name,
            line=logline,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance,
        )
    sys.exit(returncode)
