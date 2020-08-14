# Copyright 2019 Yelp Inc.
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
import behave
import mock
import simplejson as json
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS
from moto import mock_autoscaling
from moto import mock_ec2
from moto import mock_sqs

from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.config import CREDENTIALS_NAMESPACE
from clusterman.monitoring_lib import yelp_meteorite

_ttl_patch = mock.patch('clusterman.aws.CACHE_TTL_SECONDS', -1)
_ttl_patch.__enter__()
behave.use_step_matcher('re')
BEHAVE_DEBUG_ON_ERROR = False


@behave.fixture
def patch_meteorite(context):
    if yelp_meteorite:
        with yelp_meteorite.testcase():
            yield
    else:
        yield


@behave.fixture
def setup_configurations(context):
    boto_config = {
        'accessKeyId': 'foo',
        'secretAccessKey': 'bar',
    }

    main_clusterman_config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
            'signals_bucket': 'the_bucket',
        },
        'autoscaling': {
            'setpoint': 0.7,
            'target_capacity_margin': 0.1,
            'default_signal_role': 'foo',
        },
        'batches': {
            'spot_prices': {
                'run_interval_seconds': 120,
                'dedupe_interval_seconds': 60,
            },
            'cluster_metrics': {
                'run_interval_seconds': 120,
            },
        },
        'clusters': {
            'mesos-test': {
                'mesos_master_fqdn': 'the.mesos.leader',
                'aws_region': 'us-west-2',
            },
            'kube-test': {
                'aws_region': 'us-west-2',
                'kubeconfig_path': '/foo/bar/admin.conf',
            }
        },
        'sensu_config': [
            {
                'team': 'my_team',
                'runbook': 'y/my-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'FooSignal',
            'period_minutes': 10,
        }
    }

    mesos_pool_config = {
        'resource_groups': [
            {
                'sfr': {
                    's3': {
                        'bucket': 'fake-bucket',
                        'prefix': 'none',
                    }
                },
            },
            {'asg': {'tag': 'puppet:role::paasta'}},
        ],
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 100,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
        'sensu_config': [
            {
                'team': 'other-team',
                'runbook': 'y/their-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'BarSignal3',
            'branch_or_tag': 'v42',
            'period_minutes': 7,
            'required_metrics': [
                {'name': 'cpus_allocated', 'type': SYSTEM_METRICS, 'minute_range': 10},
                {'name': 'cost', 'type': APP_METRICS, 'minute_range': 30},
            ],
        },
    }
    kube_pool_config = {
        'resource_groups': [
            {'sfr': {'tag': 'puppet:role::paasta'}},
            {'asg': {'tag': 'puppet:role::paasta'}},
        ],
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 100,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
        'sensu_config': [
            {
                'team': 'other-team',
                'runbook': 'y/their-runbook',
            }
        ],
        'autoscale_signal': {
            'internal': True,
            'period_minutes': 7,
        }
    }
    with staticconf.testing.MockConfiguration(boto_config, namespace=CREDENTIALS_NAMESPACE), \
            staticconf.testing.MockConfiguration(main_clusterman_config), \
            staticconf.testing.MockConfiguration(mesos_pool_config, namespace='bar.mesos_config'), \
            staticconf.testing.MockConfiguration(kube_pool_config, namespace='bar.kubernetes_config'):
        yield


def make_asg(asg_name, subnet_id):
    if len(ec2.describe_launch_templates()['LaunchTemplates']) == 0:
        ec2.create_launch_template(
            LaunchTemplateName='fake_launch_template',
            LaunchTemplateData={
                'ImageId': 'ami-785db401',  # this AMI is hard-coded into moto, represents ubuntu xenial
                'InstanceType': 't2.2xlarge',
            },
        )
    return autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchTemplate={
            'LaunchTemplateName': 'fake_launch_template',
            'Version': '1',
        },
        MinSize=1,
        MaxSize=30,
        DesiredCapacity=1,
        AvailabilityZones=['us-west-2a'],
        VPCZoneIdentifier=subnet_id,
        NewInstancesProtectedFromScaleIn=False,
        Tags=[
            {
                'Key': 'puppet:role::paasta',
                'Value': json.dumps({
                    'paasta_cluster': 'mesos-test',
                    'pool': 'bar',
                }),
            }, {
                'Key': 'fake_tag_key',
                'Value': 'fake_tag_value',
            },
        ],
    )


def make_fleet(subnet_id):
    ec2.create_launch_template(
        LaunchTemplateName='mock_launch_template',
        LaunchTemplateData={
            'InstanceType': 'c3.4xlarge',
            'NetworkInterfaces': [{'SubnetId': subnet_id}],
        },
    )
    return ec2.create_fleet(
        ExcessCapacityTerminationPolicy='no-termination',
        LaunchTemplateConfigs={'LaunchTemplateSpecification': {'LaunchTemplateName': 'mock_launch_template'}},
        TargetCapacitySpecification={'TotalTargetCapacity': 1},
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{
                    'Key': 'puppet:role::paasta',
                    'Value': json.dumps({
                        'paasta_cluster': 'mesos-test',
                        'pool': 'bar',
                    }),
                }, {
                    'Key': 'fake_fleet_key',
                    'Value': 'fake_fleet_value',
                }],
            },
        ],
    )


def make_sfr(subnet_id):
    return ec2.request_spot_fleet(
        SpotFleetRequestConfig={
            'AllocationStrategy': 'diversified',
            'SpotPrice': '2.0',
            'TargetCapacity': 1,
            'LaunchSpecifications': [
                {
                    'ImageId': 'ami-foo',
                    'SubnetId': subnet_id,
                    'WeightedCapacity': 1,
                    'InstanceType': 'c3.8xlarge',
                    'EbsOptimized': False,
                    'TagSpecifications': [{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'puppet:role::paasta',
                            'Value': json.dumps({
                                'paasta_cluster': 'mesos-test',
                                'pool': 'bar',
                            }),
                        }],
                    }],
                },
            ],
            'IamFleetRole': 'foo',
        },
    )


@behave.fixture
def boto_patches(context):
    mock_sqs_obj = mock_sqs()
    mock_sqs_obj.start()
    mock_ec2_obj = mock_ec2()
    mock_ec2_obj.start()
    mock_autoscaling_obj = mock_autoscaling()
    mock_autoscaling_obj.start()
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    subnet_response = ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
    )
    context.subnet_id = subnet_response['Subnet']['SubnetId']
    yield
    mock_sqs_obj.stop()
    mock_ec2_obj.stop()
    mock_autoscaling_obj.stop()


def before_all(context):
    global BEHAVE_DEBUG_ON_ERROR
    BEHAVE_DEBUG_ON_ERROR = context.config.userdata.getbool('BEHAVE_DEBUG_ON_ERROR')
    behave.use_fixture(setup_configurations, context)
    behave.use_fixture(patch_meteorite, context)


def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == 'failed':
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
