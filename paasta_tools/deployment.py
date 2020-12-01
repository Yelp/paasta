import json
import os
from typing import Any
from typing import Collection
from typing import Dict
from typing import Optional

from mypy_extensions import TypedDict

from paasta_tools.util.const import DEFAULT_SOA_DIR


class NoDeploymentsAvailable(Exception):
    pass


class BranchDictV1(TypedDict, total=False):
    docker_image: str
    desired_state: str
    force_bounce: Optional[str]


class BranchDictV2(TypedDict):
    git_sha: str
    docker_image: str
    desired_state: str
    force_bounce: Optional[str]


DeploymentsJsonV1Dict = Dict[str, BranchDictV1]

DeployGroup = str
BranchName = str


class _DeploymentsJsonV2ControlsDict(TypedDict, total=False):
    force_bounce: Optional[str]
    desired_state: str


class _DeploymentsJsonV2DeploymentsDict(TypedDict):
    docker_image: str
    git_sha: str


class DeploymentsJsonV2Dict(TypedDict):
    deployments: Dict[DeployGroup, _DeploymentsJsonV2DeploymentsDict]
    controls: Dict[BranchName, _DeploymentsJsonV2ControlsDict]


class DeploymentsJsonDict(TypedDict):
    v1: DeploymentsJsonV1Dict
    v2: DeploymentsJsonV2Dict


class DeploymentsJsonV1:
    def __init__(self, config_dict: DeploymentsJsonV1Dict) -> None:
        self.config_dict = config_dict

    def get_branch_dict(self, service: str, branch: str) -> BranchDictV1:
        full_branch = f"{service}:paasta-{branch}"
        return self.config_dict.get(full_branch, {})

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, DeploymentsJsonV1)
            and other.config_dict == self.config_dict
        )


class DeploymentsJsonV2:
    def __init__(self, service: str, config_dict: DeploymentsJsonV2Dict) -> None:
        self.config_dict = config_dict
        self.service = service

    def get_branch_dict(
        self, service: str, branch: str, deploy_group: str
    ) -> BranchDictV2:
        full_branch = f"{service}:{branch}"
        branch_dict: BranchDictV2 = {
            "docker_image": self.get_docker_image_for_deploy_group(deploy_group),
            "git_sha": self.get_git_sha_for_deploy_group(deploy_group),
            "desired_state": self.get_desired_state_for_branch(full_branch),
            "force_bounce": self.get_force_bounce_for_branch(full_branch),
        }
        return branch_dict

    def get_deploy_groups(self) -> Collection[str]:
        return self.config_dict["deployments"].keys()

    def get_docker_image_for_deploy_group(self, deploy_group: str) -> str:
        try:
            return self.config_dict["deployments"][deploy_group]["docker_image"]
        except KeyError:
            e = f"{self.service} not deployed to {deploy_group}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)

    def get_git_sha_for_deploy_group(self, deploy_group: str) -> str:
        try:
            return self.config_dict["deployments"][deploy_group]["git_sha"]
        except KeyError:
            e = f"{self.service} not deployed to {deploy_group}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)

    def get_desired_state_for_branch(self, control_branch: str) -> str:
        try:
            return self.config_dict["controls"][control_branch].get(
                "desired_state", "start"
            )
        except KeyError:
            e = f"{self.service} not configured for {control_branch}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)

    def get_force_bounce_for_branch(self, control_branch: str) -> str:
        try:
            return self.config_dict["controls"][control_branch].get(
                "force_bounce", None
            )
        except KeyError:
            e = f"{self.service} not configured for {control_branch}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)


def load_deployments_json(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> Any:
    deployment_file = os.path.join(soa_dir, service, "deployments.json")
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            config_dict = json.load(f)
            return (
                DeploymentsJsonV1(config_dict["v1"])
                if "v1" in config_dict
                else DeploymentsJsonV2(service=service, config_dict=config_dict["v2"])
            )
    else:
        e = f"{deployment_file} was not found. 'generate_deployments_for_service --service {service}' must be run first"
        raise NoDeploymentsAvailable(e)


def load_v2_deployments_json(
    service: str, soa_dir: str = DEFAULT_SOA_DIR
) -> DeploymentsJsonV2:
    deployment_file = os.path.join(soa_dir, service, "deployments.json")
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJsonV2(service=service, config_dict=json.load(f)["v2"])
    else:
        e = f"{deployment_file} was not found. 'generate_deployments_for_service --service {service}' must be run first"
        raise NoDeploymentsAvailable(e)


def get_currently_deployed_sha(service, deploy_group, soa_dir=DEFAULT_SOA_DIR):
    """Tries to determine the currently deployed sha for a service and deploy_group,
    returns None if there isn't one ready yet"""
    try:
        deployments = load_v2_deployments_json(service=service, soa_dir=soa_dir)
        return deployments.get_git_sha_for_deploy_group(deploy_group=deploy_group)
    except NoDeploymentsAvailable:
        return None
