import json
import os.path

import kopf

from paasta_tools.generate_deployments_for_service import build_docker_image_name
from paasta_tools.utils import get_paasta_branch


def incrementally_update_deploy_group_json(service, deploy_group, git_sha, soa_dir):
    filename = os.path.join(soa_dir, service, "fake_deployments.json")
    try:
        with open(filename) as f:
            contents = json.load(f)
    except FileNotFoundError:
        contents = {}

    # v1 = contents.setdefault('v1', {})
    v2 = contents.setdefault("v2", {})
    deployments = v2.setdefault("deployments", {})
    deployments[deploy_group] = {
        "docker_image": build_docker_image_name(service, git_sha),
        "git_sha": git_sha,
    }
    with open(filename, "w+") as f:
        json.dump(contents, f)


def incrementally_update_start_stop_control(
    service, instance, desired_state, force_bounce, soa_dir
):
    filename = os.path.join(soa_dir, service, "fake_deployments.json")
    try:
        with open(filename) as f:
            contents = json.load(f)
    except FileNotFoundError:
        contents = {}

    # v1 = contents.setdefault('v1', {})

    control_branch = get_paasta_branch(os.environ["PAASTA_CLUSTER"], instance)
    control_branch_alias_v2 = f"{service}:{control_branch}"

    v2 = contents.setdefault("v2", {})
    controls = v2.setdefault("controls", {})
    controls[control_branch_alias_v2] = {
        "desired_state": desired_state,
        "force_bounce": force_bounce,
    }
    with open(filename, "w+") as f:
        json.dump(contents, f)


@kopf.on.create("deploygroup")
@kopf.on.update("deploygroup")
def update_dg(body, **kwargs):
    incrementally_update_deploy_group_json(
        body["service"],
        body["deploy_group"],
        body["git_sha"],
        "/nail/home/krall/pg/yelpsoa-configs",
    )


@kopf.on.create("startstopcontrol")
@kopf.on.update("startstopcontrol")
def update_start_stop_control(body, **kwargs):
    incrementally_update_start_stop_control(
        body["service"],
        body["instance"],
        body["desired_state"],
        body.get("force_bounce", None),
        "/nail/home/krall/pg/yelpsoa-configs",
    )
