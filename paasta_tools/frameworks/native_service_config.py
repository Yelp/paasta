#!/usr/bin/env python
import copy
from typing import Any
from typing import cast
from typing import List
from typing import Optional

from mypy_extensions import TypedDict

from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import Constraint  # noqa, imported for typing.
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DockerParameter
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import SystemPaastaConfig


MESOS_TASK_SPACER = "."


VolumeInfo = TypedDict(
    "VolumeInfo", {"container_path": str, "host_path": str, "mode": str}
)

_Docker_PortMapping = TypedDict(
    "_Docker_PortMapping", {"host_port": int, "container_port": int, "protocol": str}
)

DockerInfo = TypedDict(
    "DockerInfo",
    {
        "image": str,
        "network": str,
        "port_mappings": List[_Docker_PortMapping],
        "parameters": List[DockerParameter],
    },
)

ContainerInfo = TypedDict(
    "ContainerInfo", {"type": str, "docker": DockerInfo, "volumes": List[VolumeInfo]}
)


_CommandInfo_URI = TypedDict(
    "_CommandInfo_URI",
    {
        "value": str,
        "executable": bool,
        "extract": bool,
        "cache": bool,
        "output_file": str,
    },
    total=False,
)

CommandInfo = TypedDict(
    "CommandInfo",
    {
        "uris": List[_CommandInfo_URI],
        "environment": Any,
        "shell": bool,
        "value": str,
        "arguments": List[str],
        "user": str,
    },
    total=False,
)

Value_Scalar = TypedDict("Value_Scalar", {"value": float})

Value_Range = TypedDict("Value_Range", {"begin": int, "end": int})

Value_Ranges = TypedDict("Value_Ranges", {"range": List[Value_Range]})

Resource = TypedDict(
    "Resource",
    {"name": str, "type": str, "scalar": Value_Scalar, "ranges": Value_Ranges},
    total=False,
)


TaskID = TypedDict("TaskID", {"value": str})

SlaveID = TypedDict("SlaveID", {"value": str})


class TaskInfoBase(TypedDict):
    name: str
    task_id: TaskID
    agent_id: SlaveID
    resources: List[Resource]


class TaskInfo(TaskInfoBase):
    container: ContainerInfo
    command: CommandInfo


class NativeServiceConfigDict(LongRunningServiceConfigDict):
    pass


class NativeServiceConfig(LongRunningServiceConfig):
    config_dict: NativeServiceConfigDict
    config_filename_prefix = "paasta_native"

    def __init__(
        self,
        service: str,
        instance: str,
        cluster: str,
        config_dict: NativeServiceConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str,
        service_namespace_config: Optional[ServiceNamespaceConfig] = None,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )
        # service_namespace_config may be omitted/set to None at first, then set
        # after initializing. e.g. we do this in load_paasta_native_job_config
        # so we can call get_nerve_namespace() to figure out what SNC to read.
        # It may also be set to None if this service is not in nerve.
        if service_namespace_config is not None:
            self.service_namespace_config = service_namespace_config
        else:
            self.service_namespace_config = ServiceNamespaceConfig()

    def task_name(self, base_task: TaskInfo) -> str:
        code_sha = get_code_sha_from_dockerurl(
            base_task["container"]["docker"]["image"]
        )

        filled_in_task = copy.deepcopy(base_task)
        filled_in_task["name"] = ""
        filled_in_task["task_id"] = {"value": ""}
        filled_in_task["agent_id"] = {"value": ""}

        config_hash = get_config_hash(
            filled_in_task, force_bounce=self.get_force_bounce()
        )

        return compose_job_id(
            self.service,
            self.instance,
            git_hash=code_sha,
            config_hash=config_hash,
            spacer=MESOS_TASK_SPACER,
        )

    def base_task(
        self, system_paasta_config: SystemPaastaConfig, portMappings=True
    ) -> TaskInfo:
        """Return a TaskInfo Dict with all the fields corresponding to the
        configuration filled in.

        Does not include task.agent_id or a task.id; those need to be
        computed separately.
        """
        docker_volumes = self.get_volumes(
            system_volumes=system_paasta_config.get_volumes(),
            uses_bulkdata_default=system_paasta_config.get_uses_bulkdata_default(),
        )
        task: TaskInfo = {
            "name": "",
            "task_id": {"value": ""},
            "agent_id": {"value": ""},
            "container": {
                "type": "DOCKER",
                "docker": {
                    "image": self.get_docker_url(),
                    "parameters": [
                        {"key": param["key"], "value": param["value"]}
                        for param in self.format_docker_parameters()
                    ],
                    "network": self.get_mesos_network_mode(),
                    "port_mappings": [],
                },
                "volumes": [
                    {
                        "container_path": volume["containerPath"],
                        "host_path": volume["hostPath"],
                        "mode": volume["mode"].upper(),
                    }
                    for volume in docker_volumes
                ],
            },
            "command": {
                "value": str(self.get_cmd()),
                "uris": [
                    {
                        "value": system_paasta_config.get_dockercfg_location(),
                        "extract": False,
                    }
                ],
            },
            "resources": [
                {
                    "name": "cpus",
                    "type": "SCALAR",
                    "scalar": {"value": self.get_cpus()},
                },
                {"name": "mem", "type": "SCALAR", "scalar": {"value": self.get_mem()}},
            ],
        }

        if portMappings:
            task["container"]["docker"]["port_mappings"] = [
                {
                    "container_port": self.get_container_port(),
                    # filled by tasks_and_state_for_offer()
                    "host_port": 0,
                    "protocol": "tcp",
                }
            ]

            task["resources"].append(
                {
                    "name": "ports",
                    "type": "RANGES",
                    "ranges": {
                        # filled by tasks_and_state_for_offer
                        "range": [{"begin": 0, "end": 0}]
                    },
                }
            )

        task["name"] = self.task_name(task)

        return task

    def get_mesos_network_mode(self) -> str:
        return self.get_net().upper()


def load_paasta_native_job_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
    instance_type: str = "paasta_native",
    config_overrides: Optional[NativeServiceConfigDict] = None,
) -> NativeServiceConfig:
    instance_config_dict = cast(
        NativeServiceConfigDict,
        load_service_instance_config(
            service=service,
            instance=instance,
            instance_type=instance_type,
            cluster=cluster,
            soa_dir=soa_dir,
        ),
    )
    branch_dict: Optional[BranchDictV2] = None
    instance_config_dict.update(config_overrides or {})
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = NativeServiceConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=instance_config_dict,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    service_config = NativeServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=instance_config_dict,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    service_namespace_config = load_service_namespace_config(
        service=service, namespace=service_config.get_nerve_namespace(), soa_dir=soa_dir
    )
    service_config.service_namespace_config = service_namespace_config

    return service_config


class UnknownNativeServiceError(Exception):
    pass
