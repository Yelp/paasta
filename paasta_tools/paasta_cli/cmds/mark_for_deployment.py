#!/usr/bin/env python
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""

import sys

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
    print "INFO: Executing command '%s'" % cmd
    returncode, output = _run(cmd)
    if returncode != 0:
        print 'ERROR: Failed to mark image for deployment. Output:\n%sReturn code was: %d' % (output, returncode)
        sys.exit(returncode)
