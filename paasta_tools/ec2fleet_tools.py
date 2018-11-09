from typing import List
from typing import Optional
from typing import Dict
from typing import Any
from typing import Collection
from typing import Sequence
from typing import NewType
import warnings

import boto3
import botocore.exceptions
from mypy_extensions import TypedDict
import service_configuration_lib

# from paasta_tools.long_running_service_tools import BounceMethodConfigDict
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import load_v2_deployments_json


Ec2Client = Any  # TODO


class EC2FleetServiceLaunchTemplateConfigDict(TypedDict):
    instance_type: str
    max_price: float
    weighted_capacity: float


class EC2FleetServiceConfigDict(LongRunningServiceConfigDict, total=False):
    ami_id: str
    allocation_strategy: str
    instance_pools_to_use_count: int
    launch_template_configs: List[EC2FleetServiceLaunchTemplateConfigDict]
    bounce_method: str
    bounce_health_params: Dict[str, Any]
    bounce_margin_factor: int
    target_capacity: int
    on_demand_target_capacity: int
    spot_target_capacity: int
    default_target_capacity_type: str


class SpotOptionsDict(TypedDict):
    AllocationStrategy: str
    InstancePoolsToUseCount: int


class LaunchTemplateSpecificationDict(TypedDict):
    # LaunchTemplateId: str  # commented-out because we're just gonna use the name instead.
    LaunchTemplateName: str
    Version: str


class OverrideDict(TypedDict):
    InstanceType: str
    # MaxPrice: str
    # SubnetId: str
    # AvailabilityZone: str
    WeightedCapacity: Optional[float]  # I hope this is float. The docs don't really say.
    # Priority: Optional[int]


class EC2Fleet_LaunchTemplateConfigDict(TypedDict):
    """Named like this to distinguish it from Paasta launch template configs."""
    LaunchTemplateSpecification: LaunchTemplateSpecificationDict
    Overrides: List[OverrideDict]
    # Version: str  # this seems to be a string that is empty or holds an int, e.g. "" or "2"


class TargetCapacitySpecificationDict(TypedDict):
    TotalTargetCapacity: int
    OnDemandTargetCapacity: int
    SpotTargetCapacity: int
    DefaultTargetCapacityType: str


class TagDict(TypedDict):
    Key: str
    Value: str


class TagSpecificationDict(TypedDict):
    ResourceType: str
    Tags: List[TagDict]


class FormattedEC2FleetDict(TypedDict):  # should make this subclass BounceMethodConfigDict:
    SpotOptions: SpotOptionsDict
    # TODO: OnDemandOptions
    ExcessCapacityTerminationPolicy: str
    LaunchTemplateConfigs: List[EC2Fleet_LaunchTemplateConfigDict]
    TargetCapacitySpecification: TargetCapacitySpecificationDict
    TagSpecifications: List[TagSpecificationDict]


class EC2FleetNode:
    def __init__(
        self,
        host: str,
        weight: float,
    ) -> None:
        self.host = host
        self.ports: Sequence[int] = [8888]
        self.weight = weight


EC2FleetAwsId = NewType("EC2FleetAwsId", str)
EC2FleetPaastaId = NewType("EC2FleetPaastaId", str)


class EC2Fleet:
    def __init__(
        self,
        nodes: Sequence[EC2FleetNode],
        aws_id: str,
        paasta_id: str,
        target_capacity: int,
    ) -> None:
        self.nodes = nodes
        self.aws_id = EC2FleetAwsId(aws_id)
        self.paasta_id = EC2FleetPaastaId(paasta_id)
        self.target_capacity = target_capacity


class EC2FleetServiceConfig(LongRunningServiceConfig):
    config_dict: EC2FleetServiceConfigDict
    config_filename_prefix = 'ec2fleet'

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: EC2FleetServiceConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str=DEFAULT_SOA_DIR,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def format_ec2_fleet_dict(self) -> FormattedEC2FleetDict:
        config: FormattedEC2FleetDict = {
            "SpotOptions": {
                "AllocationStrategy": self.get_allocation_strategy(),
                "InstancePoolsToUseCount": self.get_instance_pools_to_use_count(),
            },
            "ExcessCapacityTerminationPolicy": "no-termination",  # we will terminate.
            "LaunchTemplateConfigs": self.format_launch_template_config_dicts(),
            "TargetCapacitySpecification": self.get_target_capacity_specification(),
            "TagSpecifications": [
                {
                    "ResourceType": "fleet",
                    "Tags": [
                        {
                            "Key": "PaastaFleetId",
                            "Value": "",  # To be filled in.
                        },
                    ],
                },
            ],
        }

        config['TagSpecifications'][0]['Tags'][0]["Value"] = format_job_id(
            service=self.service,
            instance=self.instance,
            git_hash=get_code_sha_from_dockerurl(self.get_docker_url()),
            config_hash=get_config_hash(config, force_bounce=self.get_force_bounce()),
        )

        return config

    def get_target_capacity_specification(self) -> TargetCapacitySpecificationDict:
        return {
            "TotalTargetCapacity": self.config_dict.get('target_capacity', 1),
            "OnDemandTargetCapacity": self.config_dict.get('on_demand_target_capacity', 0),
            "SpotTargetCapacity": self.config_dict.get('spot_target_capacity', 0),
            "DefaultTargetCapacityType": self.config_dict.get('default_target_capacity_type', 'spot'),
        }

    def get_allocation_strategy(self) -> str:
        return self.config_dict.get("allocation_strategy", "lowestPrice")

    def get_instance_pools_to_use_count(self) -> int:
        return self.config_dict.get("instance_pools_to_use_count", 1)

    def format_launch_template_config_dicts(self) -> List[EC2Fleet_LaunchTemplateConfigDict]:
        return [
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": self.get_launch_template_config_name(self.format_launch_template_config(ltc)),
                    "Version": "1",  # TODO: should this be specified as version 1 or something?
                },
                "Overrides": [{
                    "InstanceType": ltc.get("instance_type"),
                    "WeightedCapacity": self.get_weighted_capacity(ltc),
                }],
            }
            for ltc in self.config_dict.get("launch_template_configs", [])
        ]

    def format_launch_template_config(self, launch_template_config: EC2FleetServiceLaunchTemplateConfigDict):
        return {
            "EbsOptimized": True,
            "ImageId": self.get_ami_id(),
        }

    def get_ami_id(self) -> str:
        return self.config_dict['ami_id']

    def get_max_price(self, launch_template_config: EC2FleetServiceLaunchTemplateConfigDict) -> str:
        return str(launch_template_config.get('max_price', ''))

    def get_weighted_capacity(self, launch_template_config: EC2FleetServiceLaunchTemplateConfigDict) -> float:
        return launch_template_config['weighted_capacity']

    def get_launch_template_config_name(self, complete_ltc: Dict[str, Any]) -> str:
        docker_url = self.get_docker_url()
        code_sha = get_code_sha_from_dockerurl(docker_url)

        config_hash = get_config_hash(complete_ltc, force_bounce=self.get_force_bounce(),)
        job_id = format_job_id(self.service, self.instance, code_sha, config_hash)
        return f"ltc/{job_id}"

    def make_launch_config_template(
        self,
        ec2_client: Ec2Client,
        launch_template_config: EC2FleetServiceLaunchTemplateConfigDict,
    ) -> str:
        """Create a Launch Template in AWS, and return its name."""
        complete_ltc = self.format_launch_template_config(launch_template_config)
        name = self.get_launch_template_config_name(complete_ltc)
        response = ec2_client.create_launch_template(
            LaunchTemplateName=name,
            LaunchTemplateData=complete_ltc,
        )

        return name

    def create(
        self,
        ec2_client,
    ):
        for launch_template_config in self.config_dict.get("launch_template_configs", []):
            try:
                self.make_launch_config_template(ec2_client, launch_template_config)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "InvalidLaunchTemplateName.AlreadyExistsException":
                    pass
                else:
                    raise

        complete_config = self.format_ec2_fleet_dict()
        import json; print(json.dumps(complete_config, indent=4))
        resp = ec2_client.create_fleet(**complete_config)

    def get_bounce_method(self) -> str:
            """Get the bounce method specified in the service's marathon configuration.

            :param service_config: The service instance's configuration dictionary
            :returns: The bounce method specified in the config, or 'crossover' if not specified"""
            return self.config_dict.get('bounce_method', 'crossover')

    def get_bounce_health_params(self, service_namespace_config: ServiceNamespaceConfig) -> Dict[str, Any]:
        default: Dict[str, Any] = {}
        if service_namespace_config.is_in_smartstack():
            default = {'check_haproxy': True}
        return self.config_dict.get('bounce_health_params', default)

    def get_bounce_margin_factor(self) -> float:
        return self.config_dict.get('bounce_margin_factor', 1.0)


def format_job_id(service: str, instance: str, git_hash: Optional[str]=None, config_hash: Optional[str]=None) -> str:
    """
    :param service: The name of the service
    :param instance: The instance of the service
    :param git_hash: The git_hash portion of the job_id. If git_hash is set,
                     config_hash must also be set.
    :param config_hash: The config_hash portion of the job_id. If config_hash
                        is set, git_hash must also be set.
    :returns: a composed app id

    """
    return compose_job_id(service, instance, git_hash, config_hash)


def get_paasta_fleet_id_from_tags(tags) -> EC2FleetPaastaId:
    return EC2FleetPaastaId({tag['Key']: tag['Value'] for tag in tags}["PaastaFleetId"])


def kill_given_nodes(client: Ec2Client, node_ids: Collection[str]) -> None:
    print(f"Would have killed node_ids: {node_ids}")


def fetch_nodes_for_fleet(
    client: Ec2Client,
    fleet_aws_id: EC2FleetAwsId,
    type_to_weight_map: Dict[str, float],
) -> List[EC2FleetNode]:
    describe_fleet_instances_resp = client.describe_fleet_instances(FleetId=fleet_aws_id)
    print("describe_fleet_instances_resp")
    import pprint; pprint.pprint(describe_fleet_instances_resp)

    instance_ids = [i['InstanceId'] for i in describe_fleet_instances_resp['ActiveInstances']]
    # ugh, describe_instances gives you every instance if you pass an empty InstanceIds list.
    if not instance_ids:
        return []

    describe_instances_resp = client.describe_instances(InstanceIds=instance_ids)
    print("describe_instances_resp")
    import pprint; pprint.pprint(describe_instances_resp)

    nodes: List[EC2FleetNode] = []
    for reservation in describe_instances_resp['Reservations']:
        for instance in reservation['Instances']:
            nodes.append(EC2FleetNode(
                host=instance['PrivateIpAddress'],
                weight=type_to_weight_map[instance['InstanceType']],
            ))

    return nodes


def fetch_matching_fleets(client: Ec2Client, service: str, instance: str) -> List[EC2Fleet]:
    describe_fleet_resp = client.describe_fleets()
    print("describe_fleet_resp")
    import pprint; pprint.pprint(describe_fleet_resp)
    fleets: List[EC2Fleet] = []
    for fleet in describe_fleet_resp['Fleets']:
        if get_paasta_fleet_id_from_tags(fleet['Tags']).startswith(compose_job_id(service, instance)):
            type_to_weight_map = {
                o["InstanceType"]: o["WeightedCapacity"]
                for ltc in fleet['LaunchTemplateConfigs']
                for o in ltc['Overrides']
            }

            fleets.append(EC2Fleet(
                nodes=fetch_nodes_for_fleet(
                    client=client,
                    fleet_aws_id=fleet['FleetId'],
                    type_to_weight_map=type_to_weight_map,
                ),
                aws_id=fleet['FleetId'],
                paasta_id=get_paasta_fleet_id_from_tags(fleet['Tags']),
                target_capacity=fleet['TargetCapacitySpecification']['TotalTargetCapacity'],
            ))

    return fleets


def load_ec2fleet_service_config(
    service: str,
    instance: str,
    cluster: str,
    soa_dir: str,
    load_deployments: bool = False,
) -> EC2FleetServiceConfig:
    """Read a service instance's configuration for marathon.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir,
    )
    marathon_conf_file = "ec2fleet-%s" % cluster
    instance_configs = service_configuration_lib.read_extra_service_information(
        service,
        marathon_conf_file,
        soa_dir=soa_dir,
    )

    if instance.startswith('_'):
        raise InvalidJobNameError(
            f"Unable to load ec2fleet job config for {service}.{instance} as instance name starts with '_'",
        )
    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            f"{instance} not found in config file {soa_dir}/{service}/{marathon_conf_file}.yaml.",
        )

    general_config = deep_merge_dictionaries(overrides=instance_configs[instance], defaults=general_config)

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = EC2FleetServiceConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return EC2FleetServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


class EC2FleetAlreadyExistsError(RuntimeError):
    pass


def take_up_slack(fleet: EC2Fleet, ec2_client: Ec2Client) -> None:
    warnings.warn("take_up_slack not implemented")


def get_amount_of_at_risk_weight(fleet: EC2Fleet, draining_hosts: Collection[str]):
    return sum(
        [
            node.weight
            for node in fleet.nodes
            if node.host in draining_hosts
        ],
        0.0,
    )


def scale_fleet(
    aws_id: EC2FleetAwsId,
    new_target_capacity: int,
):
    raise NotImplementedError()


def get_ec2_client(
    system_paasta_config: SystemPaastaConfig,
) -> Ec2Client:
    return boto3.client('ec2', region_name=system_paasta_config.get_aws_region())


def cleanup_fleet(
    ec2_client: Ec2Client,
    aws_id: EC2FleetAwsId,
) -> None:
    ec2_client.delete_fleets(
        FleetIds=[aws_id],
        TerminateInstances=True,  # TODO: reconsider this.
    )


