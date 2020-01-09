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
import base64
import binascii
import json
import os

import requests
from requests.exceptions import RequestException

from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.generate_deployments_for_service import build_docker_image_name
from paasta_tools.utils import _log
from paasta_tools.utils import _log_audit
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tags
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_service_push_docker_registries
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "push-to-registry",
        help="Uploads a docker image to a registry",
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
        "-s",
        "--service",
        help='Name of service for which you wish to upload a docker image. Leading "services-", '
        "as included in a Jenkins job name, will be stripped.",
        required=True,
    )
    list_parser.add_argument(
        "-c",
        "--commit",
        help="Git sha after which to name the remote image",
        required=True,
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        "-f",
        "--force",
        help=(
            "Do not check if the image is already in the PaaSTA docker registries. "
            "Push it anyway."
        ),
        action="store_true",
    )
    list_parser.add_argument(
        "-r",
        "--registries",
        nargs="*",
        help=(
            "Docker Registry URLs, such as docker-paasta-norcal.yelpcorp.com:443, separated by semicolons."
            "Restricts the Docker Registry URLs to only a specified list."
        ),
    )
    list_parser.set_defaults(command=paasta_push_to_registry)


def build_commands(upstream_job_name, upstream_git_commit, registries=None):
    # This is kinda dumb since we just cleaned the 'services-' off of the
    # service so we could validate it, but the Docker image will have the full
    # name with 'services-' so add it back.
    tags = build_docker_tags(upstream_job_name, upstream_git_commit, registries)
    cmds = [f"docker push {tag}" for tag in tags]
    return cmds


def paasta_push_to_registry(args):
    """Upload a docker image to registries"""
    service = args.service
    if service and service.startswith("services-"):
        service = service.split("services-", 1)[1]
    validate_service_name(service, args.soa_dir)

    if not args.force:
        try:
            image_in_registry_or_not = where_does_docker_image_exist_and_does_not(
                service, args.soa_dir, args.commit, args.registries
            )
        except Exception as e:
            paasta_print(
                "Cannot find or connect to the PaaSTA docker registries to verify if the image exists.\n"
                "%s" % (str(e))
            )
            return 1

    cmds = build_commands(service, args.commit, args.registries)

    if args.registries:
        registries = args.registries
    else:
        registries = get_service_push_docker_registries(service, args.soa_dir)

    resultingcode = 0

    for cmd, registry in zip(cmds, registries):
        if not args.force and image_in_registry_or_not.get(registry, False):
            paasta_print(
                f"The docker image is already in the PaaSTA docker registry {registry}. "
                "I'm NOT overriding the existing image. "
                "Add --force and --registries to override the image in the registry if you are sure what you are doing."
            )
            continue

        loglines = []

        returncode, output = _run(
            cmd,
            timeout=3600,
            log=True,
            component="build",
            service=service,
            loglevel="debug",
        )

        if returncode != 0:
            resultingcode = 1
            loglines.append(
                f"ERROR: Failed to promote image for {args.commit} (command: {cmd})."
            )
            output = get_jenkins_build_output_url()
            if output:
                loglines.append("See output: %s" % output)
        else:
            loglines.append(
                f"Successfully pushed image for {args.commit} to registry (command: {cmd})"
            )

        for logline in loglines:
            _log(service=service, line=logline, component="build", level="event")

    if resultingcode == 0:
        _log_audit(
            action="push-to-registry",
            action_details={"commit": args.commit},
            service=service,
        )

    return resultingcode


def read_docker_registry_creds(registry_uri):
    dockercfg_path = os.path.expanduser("~/.dockercfg")
    try:
        with open(dockercfg_path) as f:
            dockercfg = json.load(f)
            auth = base64.b64decode(dockercfg[registry_uri]["auth"]).decode("utf-8")
            first_colon = auth.find(":")
            if first_colon != -1:
                return (auth[:first_colon], auth[first_colon + 1 : -2])
    except IOError:  # Can't open ~/.dockercfg
        pass
    except json.scanner.JSONDecodeError:  # JSON decoder error
        pass
    except binascii.Error:  # base64 decode error
        pass
    return (None, None)


def where_does_docker_image_exist_and_does_not(service, soa_dir, sha, registries=None):
    if registries:
        registry_uris = registries
    else:
        registry_uris = get_service_push_docker_registries(service, soa_dir)

    if not registry_uris:
        raise Exception("Unable to check image existence - no registry provided")

    return {
        registry_uri: is_docker_image_already_in_registry(service, sha, registry_uri)
        for registry_uri in registry_uris
    }


def is_docker_image_already_in_registry(service, sha, registry_uri):
    repository, tag = build_docker_image_name(service, sha).split(":")

    creds = read_docker_registry_creds(registry_uri)
    uri = f"{registry_uri}/v2/{repository}/manifests/paasta-{sha}"

    with requests.Session() as s:
        try:
            url = "https://" + uri
            r = (
                s.head(url, timeout=30)
                if creds[0] is None
                else s.head(url, auth=creds, timeout=30)
            )
        except RequestException:
            # If no auth creds, fallback to trying http
            if creds[0] is not None:
                raise
            url = "http://" + uri
            r = s.head(url, timeout=30)

        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False  # No Such Repository Error

        r.raise_for_status()


def is_docker_image_already_in_registries(service, soa_dir, sha):
    """Verifies that docker image exists in all the paasta registries.

    :param service: name of the service
    :param sha: git sha
    :returns: True, False or raises requests.exceptions.RequestException
    """
    return all(
        where_does_docker_image_exist_and_does_not(service, soa_dir, sha).values()
    )
