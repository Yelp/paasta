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
import argparse
import base64
import binascii
import json
import os
from typing import Optional
from typing import Tuple

import requests
from requests.exceptions import RequestException
from requests.exceptions import SSLError

from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.generate_deployments_for_service import build_docker_image_name
from paasta_tools.utils import _log
from paasta_tools.utils import _log_audit
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import build_image_identifier
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_service_docker_registry


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
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
        "--image-version",
        type=str,
        required=False,
        default=None,
        help="Extra version metadata to use when naming the remote image",
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
            "Do not check if the image is already in the PaaSTA docker registry. "
            "Push it anyway."
        ),
        action="store_true",
    )
    list_parser.set_defaults(command=paasta_push_to_registry)


def build_command(
    upstream_job_name: str,
    upstream_git_commit: str,
    image_version: Optional[str] = None,
) -> str:
    # This is kinda dumb since we just cleaned the 'services-' off of the
    # service so we could validate it, but the Docker image will have the full
    # name with 'services-' so add it back.
    tag = build_docker_tag(upstream_job_name, upstream_git_commit, image_version)
    cmd = f"docker push {tag}"
    return cmd


def paasta_push_to_registry(args: argparse.Namespace) -> int:
    """Upload a docker image to a registry"""
    service = args.service
    if service and service.startswith("services-"):
        service = service.split("services-", 1)[1]
    validate_service_name(service, args.soa_dir)
    image_identifier = build_image_identifier(args.commit, None, args.image_version)

    if not args.force:
        try:
            if is_docker_image_already_in_registry(
                service, args.soa_dir, args.commit, args.image_version
            ):
                print(
                    "The docker image is already in the PaaSTA docker registry. "
                    "I'm NOT overriding the existing image. "
                    "Add --force to override the image in the registry if you are sure what you are doing."
                )
                return 0
        except RequestException as e:
            registry_uri = get_service_docker_registry(service, args.soa_dir)
            print(
                "Can not connect to the PaaSTA docker registry '%s' to verify if this image exists.\n"
                "%s" % (registry_uri, str(e))
            )
            return 1

    cmd = build_command(service, args.commit, args.image_version)
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
        loglines.append("ERROR: Failed to promote image for %s." % image_identifier)
        output = get_jenkins_build_output_url()
        if output:
            loglines.append("See output: %s" % output)
    else:
        loglines.append(
            "Successfully pushed image for %s to registry" % image_identifier
        )
        _log_audit(
            action="push-to-registry",
            action_details={"commit": args.commit},
            service=service,
        )
    for logline in loglines:
        _log(service=service, line=logline, component="build", level="event")
    return returncode


def read_docker_registry_creds(
    registry_uri: str,
) -> Tuple[Optional[str], Optional[str]]:
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
    except json.JSONDecodeError:  # JSON decoder error
        pass
    except binascii.Error:  # base64 decode error
        pass
    return (None, None)


def is_docker_image_already_in_registry(service: str, soa_dir: str, sha: str, image_version: Optional[str] = None) -> bool:  # type: ignore
    """Verifies that docker image exists in the paasta registry.

    :param service: name of the service
    :param sha: git sha
    :returns: True, False or raises requests.exceptions.RequestException
    """
    registry_uri = get_service_docker_registry(service, soa_dir)
    repository, tag = build_docker_image_name(service, sha, image_version).split(":", 1)

    creds = read_docker_registry_creds(registry_uri)
    uri = f"{registry_uri}/v2/{repository}/manifests/{tag}"

    with requests.Session() as s:
        try:
            url = "https://" + uri
            r = (
                s.head(url, timeout=30)
                if creds[0] is None
                else s.head(url, auth=creds, timeout=30)
            )
        except SSLError:
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
