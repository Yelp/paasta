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
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""
from paasta_tools import remote_git
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_paasta_tag_from_deploy_group


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
        help='Git url for service -- where magic mark-for-deployment tags are pushed',
        required=True,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha to mark for deployment',
        required=True,
    )
    list_parser.add_argument(
        '-l', '--deploy-group', '--clusterinstance',
        help='Mark the service ready for deployment in this deploy group (e.g. '
             'cluster1.canary, cluster2.main). --clusterinstance is depricated and '
             'should be replaced with --deploy-group',
        required=True,
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to mark for deployment. Leading '
        '"services-" will be stripped.',
        required=True,
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(git_url, deploy_group, service, commit):
    """Mark a docker image for deployment"""
    tag = get_paasta_tag_from_deploy_group(identifier=deploy_group, desired_state='deploy')
    remote_tag = format_tag(tag)
    ref_mutator = remote_git.make_force_push_mutate_refs_func(
        targets=[remote_tag],
        sha=commit,
    )
    try:
        remote_git.create_remote_refs(git_url=git_url, ref_mutator=ref_mutator, force=True)
    except Exception as e:
        loglines = ["Failed to mark %s in for deployment in deploy group %s!" % (commit, deploy_group)]
        for line in str(e).split('\n'):
            loglines.append(line)
        return_code = 1
    else:
        loglines = ["Marked %s in for deployment in deploy group %s" % (commit, deploy_group)]
        return_code = 0

    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='deploy',
            level='event',
        )
    return return_code


def paasta_mark_for_deployment(args):
    """Wrapping mark_for_deployment"""
    deploy_group = args.deploy_group
    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, soa_dir=args.soa_dir)
    return mark_for_deployment(
        git_url=args.git_url,
        deploy_group=deploy_group,
        service=service,
        commit=args.commit,
    )
