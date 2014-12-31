#!/usr/bin/env python
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""

import shlex
import subprocess
import sys


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
    list_parser.add_argument('-l', '--clustername',
                             help='Mark the service ready for deployment in this clustername',
                             required=True,
                             )
    list_parser.add_argument('-i', '--instancename',
                             help='Mark the service ready for deployment in this instancename',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def build_command(
    upstream_git_url,
    upstream_git_commit,
    clustername,
    instancename,
):
    cmd = 'git push %s %s:refs/heads/paasta-%s.%s' % (
        upstream_git_url,
        upstream_git_commit,
        clustername,
        instancename,
    )
    return shlex.split(cmd)


def paasta_mark_for_deployment(args):
    """Mark a docker image for deployment"""
    cmd = build_command(args.git_url, args.commit, args.clustername, args.instancename)
    print "INFO: Executing command '%s'" % cmd
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print 'ERROR: Failed to mark image for deployment. Output:\n%sReturn code was: %d' % (exc.output, exc.returncode)
        sys.exit(exc.returncode)
