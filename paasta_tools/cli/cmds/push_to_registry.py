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
from __future__ import absolute_import
from __future__ import unicode_literals

import base64
import os

import requests
import simplejson as json
from requests.exceptions import RequestException

from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.generate_deployments_for_service import build_docker_image_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_service_docker_registry
from paasta_tools.utils import paasta_print


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
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        '-f', '--force',
        help=('Do not check if the image is already in the PaaSTA docker registry. '
              'Push it anyway.'),
        action='store_true',
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

    if not args.force:
        try:
            if is_docker_image_already_in_registry(service, args.soa_dir, args.commit):
                paasta_print("The docker image is already in the PaaSTA docker registry. "
                             "I'm NOT overriding the existing image. "
                             "Add --force to override the image in the registry if you are sure what you are doing.")
                return 0
        except RequestException as e:
            registry_uri = get_service_docker_registry(service, args.soa_dir)
            paasta_print("Can not connect to the PaaSTA docker registry '%s' to verify if this image exists.\n"
                         "%s" % (registry_uri, str(e)))
            return 1

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


def read_docker_registy_creds(registry_uri):
    dockercfg_path = os.path.expanduser('~/.dockercfg')
    try:
        with open(dockercfg_path) as f:
            dockercfg = json.load(f)
            auth = base64.b64decode(dockercfg[registry_uri]['auth'])
            first_colon = auth.find(':')
            if first_colon != -1:
                return (auth[:first_colon], auth[first_colon + 1:-2])
    except IOError:  # Can't open ~/.dockercfg
        pass
    except json.scanner.JSONDecodeError:  # JSON decoder error
        pass
    except TypeError:  # base64 decode error
        pass
    return (None, None)


def is_docker_image_already_in_registry(service, soa_dir, sha):
    """Verifies that docker image exists in the paasta registry.

    :param service: name of the service
    :param sha: git sha
    :returns: True, False or raises requests.exceptions.RequestException
    """
    registry_uri = get_service_docker_registry(service, soa_dir)
    repository, tag = build_docker_image_name(service, sha).split(':')
    url = 'https://%s/v2/%s/tags/list' % (registry_uri, repository)

    creds = read_docker_registy_creds(registry_uri)

    with requests.Session() as s:
        r = s.get(url, timeout=30) if creds[0] is None else s.get(url, auth=creds, timeout=30)
        if r.status_code == 200:
            tags_resp = r.json()
            if tags_resp['tags']:
                return tag in tags_resp['tags']
            else:
                return False
        elif r.status_code == 404:
            return False  # No Such Repository Error
        r.raise_for_status()
