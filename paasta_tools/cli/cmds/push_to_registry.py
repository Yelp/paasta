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
"""Contains methods used by the paasta client to upload a docker
image to a registry.
"""
from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import DEFAULT_SOA_DIR


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'push-to-registry',
        help='Uploads a docker image to a registry',
        description=(
            "'paasta push-to-registry' is a tool to upload a local docker image "
            "to the configured PaaSTA docker registry with a predictable and "
            "well-constructed image name. The image name must be predictable because "
            "the other PaaSTA components are expecting a particular format for the docker "
            "image name."
        ),
        epilog=(
            "Note: Uploading to a docker registry often requires access to the local "
            "docker socket as well as credentials to the remote registry"
        ),
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to upload a docker image. Leading "services-", '
             'as included in a Jenkins job name, will be stripped.',
        required=True,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha after which to name the remote image',
        required=True,
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_push_to_registry)


def build_command(upstream_job_name, upstream_git_commit):
    # This is kinda dumb since we just cleaned the 'services-' off of the
    # service so we could validate it, but the Docker image will have the full
    # name with 'services-' so add it back.
    tag = build_docker_tag(upstream_job_name, upstream_git_commit)
    cmd = 'docker push %s' % (
        tag,
    )
    return cmd


def paasta_push_to_registry(args):
    """Upload a docker image to a registry"""
    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, args.soa_dir)

    cmd = build_command(service, args.commit)
    loglines = []
    returncode, output = _run(
        cmd,
        timeout=3600,
        log=True,
        component='build',
        service=service,
        loglevel='debug'
    )
    if returncode != 0:
        loglines.append('ERROR: Failed to promote image for %s.' % args.commit)
        output = get_jenkins_build_output_url()
        if output:
            loglines.append('See output: %s' % output)
    else:
        loglines.append('Successfully pushed image for %s to registry' % args.commit)
    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='build',
            level='event',
        )
    return returncode
