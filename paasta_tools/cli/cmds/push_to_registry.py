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
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import _log
from paasta_tools.utils import _log_audit
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import build_image_identifier
from paasta_tools.utils import get_service_docker_registry

# Unless we show that we accept these, they will not be sent to us
DOCKER_REGISTRY_MANIFEST_ACCEPT_HEADER = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)


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
        # strip any potential trailing / for folks tab-completing directories
        type=lambda x: x.rstrip("/"),
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
        help=(
            "Extra version metadata to use when naming the remote image."
            " When set, both versioned and non-versioned Docker tags are"
            " pushed to the registry. The image with a versioned tag is"
            " expected to have been build locally."
        ),
    )
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    arch_group = list_parser.add_mutually_exclusive_group()
    arch_group.add_argument(
        "--architecture",
        type=str,
        default=None,
        help=(
            "Push the locally built image under an architecture-specific tag"
            " (e.g. paasta-<sha>-arm64) instead of the normal tag. Used when"
            " building multi-arch images: each architecture is pushed separately"
            " and then combined with --combine-architectures."
        ),
    )
    arch_group.add_argument(
        "--combine-architectures",
        type=lambda x: [a.strip() for a in x.split(",") if a.strip()],
        default=None,
        metavar="ARCH[,ARCH...]",
        help=(
            "Instead of pushing a local image, create a multi-arch manifest list"
            " under the normal tag from architecture-specific tags previously"
            " pushed with --architecture."
        ),
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


def paasta_push_to_registry_impl(
    args: argparse.Namespace, service: str, image_version: Optional[str]
) -> int:
    """Upload a docker image to a registry"""
    image_identifier = build_image_identifier(args.commit, None, image_version)

    if not args.force:
        try:
            if is_docker_image_already_in_registry(
                service, args.soa_dir, args.commit, image_version
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

    cmd = build_command(service, args.commit, image_version)
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


def arch_image_version(image_version: str | None, architecture: str) -> str:
    """Image version used for the architecture-specific tag of a multi-arch image,
    i.e. paasta-<sha>[-<image_version>]-<architecture>"""
    if image_version:
        return f"{image_version}-{architecture}"
    return architecture


def paasta_push_architecture(
    args: argparse.Namespace,
    service: str,
    commit: str,
    image_version: str | None,
    architecture: str,
) -> int:
    """Push the locally built image under its architecture-specific tag"""
    arch_version = arch_image_version(image_version, architecture)
    returncode, _ = _run(
        [
            "docker",
            "tag",
            # NOTE: this doesn't actually get pushed (just the arch_version tag below)
            # ...but it's nice to keep this around for interoperability with other stuff
            # that's still looking for the standard DOCKER_TAG :)
            # and also, there's no way for collisions since atm, we only support doing
            # native builds - i.e,. there's no way for this tag to be on the same host
            # and overwrite the tag for another arch-image
            build_docker_tag(service, commit, image_version),
            # for reference: paasta-<sha>[-<image_version>]-<arch>
            build_docker_tag(service, commit, arch_version),
        ],
        timeout=30,
        log=True,
        component="build",
        service=service,
        loglevel="debug",
    )
    if returncode != 0:
        return returncode
    return paasta_push_to_registry_impl(args, service, arch_version)


def paasta_combine_architectures(
    service: str,
    commit: str,
    image_version: str | None,
    architectures: list[str],
    soa_dir: str,
    force: bool,
) -> int:
    """Create multi-arch manifest list(s) under the normal tag(s) from the
    architecture-specific images already in the registry"""
    # i.e., default to just pushing the standard paasta-<sha> tag
    target_versions: list[str | None] = [None]
    # but also handle paasta-<sha>-<image version> for the services
    # using that feature
    if image_version:
        target_versions.append(image_version)

    targets = []
    for target_version in target_versions:
        if not force:
            try:
                if is_docker_image_already_in_registry(
                    service=service,
                    soa_dir=soa_dir,
                    sha=commit,
                    image_version=target_version,
                ):
                    print(
                        f"{build_docker_tag(service, commit, target_version)} is already "
                        "in the PaaSTA docker registry, not overriding it."
                    )
                    continue
            except RequestException as e:
                print(
                    f"Can not check the PaaSTA docker registry for existing images: {e}"
                )
                return 1
        targets.append(build_docker_tag(service, commit, target_version))

    if not targets:
        return 0

    cmd = ["docker", "buildx", "imagetools", "create"]
    # i.e., the "destination" tag(s)
    for target in targets:
        cmd += ["--tag", target]
    # and the "source" tags to combine into the "destination" tag(s)
    cmd += [
        build_docker_tag(service, commit, arch_image_version(image_version, arch))
        for arch in architectures
    ]
    returncode, _ = _run(
        cmd,
        timeout=3600,
        log=True,
        component="build",
        service=service,
        loglevel="debug",
    )
    image_identifier = build_image_identifier(commit, None, image_version)
    if returncode != 0:
        line = f"ERROR: Failed to create multi-arch image for {image_identifier}."
        output = get_jenkins_build_output_url()
        if output:
            line += f" See output: {output}"
    else:
        line = f"Successfully pushed {'/'.join(architectures)} image for {image_identifier} to registry"
        _log_audit(
            action="push-to-registry",
            action_details={"commit": commit, "architectures": architectures},
            service=service,
        )
    _log(service=service, line=line, component="build", level="event")
    return returncode


def paasta_push_to_registry(args: argparse.Namespace) -> int:
    """Upload a docker images to a registry"""
    service = args.service
    if service and service.startswith("services-"):
        service = service.split("services-", 1)[1]
    validate_service_name(service, args.soa_dir)

    if args.architecture:
        return paasta_push_architecture(
            args, service, args.commit, args.image_version, args.architecture
        )
    if args.combine_architectures:
        return paasta_combine_architectures(
            service=service,
            commit=args.commit,
            image_version=args.image_version,
            architectures=args.combine_architectures,
            soa_dir=args.soa_dir,
            force=args.force,
        )

    returncode = 0
    image_versions = [None]
    if args.image_version:
        # re-tag with legacy convention as well
        returncode = retag_versioned_image(service, args.commit, args.image_version)
        if returncode != 0:
            _log(
                service=service,
                line=f"ERROR: failed re-tagging image. See output at {get_jenkins_build_output_url()}",
                component="build",
                level="event",
            )
            return returncode
        image_versions.append(args.image_version)

    for image_version in image_versions:
        returncode = paasta_push_to_registry_impl(args, service, image_version)
        if returncode != 0:
            break

    return returncode


def retag_versioned_image(service: str, commit: str, image_version: str) -> int:
    """Create new local tag without additional version information

    :param str service: service name
    :param str commit: commit sha
    :param str image_version: additional image version metadata
    :return: docker command return code
    """
    unversioned_tag = build_docker_tag(service, commit, None)
    image_version_tag = build_docker_tag(service, commit, image_version)
    returncode, _ = _run(
        ["docker", "tag", image_version_tag, unversioned_tag],
        timeout=30,
        log=True,
        component="build",
        service=service,
        loglevel="debug",
    )
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
        s.headers["Accept"] = DOCKER_REGISTRY_MANIFEST_ACCEPT_HEADER
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
