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
from hamcrest import assert_that
from hamcrest import equal_to
from hamcrest import has_key
from hamcrest import not_

from clusterman.aws.client import ec2
from clusterman.aws.client import sqs
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.draining.queue import DrainingClient
from itests.environment import boto_patches
from itests.environment import make_sfr


@behave.given('a draining client')
def draining_client(context):
    behave.use_fixture(boto_patches, context)
    response = make_sfr(context.subnet_id)
    context.sfr_id = response['SpotFleetRequestId']
    response = ec2.describe_spot_fleet_instances(SpotFleetRequestId=context.sfr_id)
    context.instance_id = response['ActiveInstances'][0]['InstanceId']
    ec2.create_tags(
        Resources=[context.instance_id],
        Tags=[{'Key': 'aws:ec2spot:fleet-request-id', 'Value': context.sfr_id}],
    )
    context.drain_url = sqs.create_queue(QueueName='draining_queue')['QueueUrl']
    context.termination_url = sqs.create_queue(QueueName='termination_queue')['QueueUrl']
    context.warning_url = sqs.create_queue(QueueName='warning_queue')['QueueUrl']
    with staticconf.testing.PatchConfiguration({
        'clusters': {
            'mesos-test': {
                'drain_queue_url': context.drain_url,
                'termination_queue_url': context.termination_url,
                'warning_queue_url': context.warning_url,
            }
        }
    }):
        context.draining_client = DrainingClient('mesos-test')


@behave.given('a message in the (?P<queue_name>draining|termination) queue')
def queue_setup(context, queue_name):
    url = context.drain_url if queue_name == 'draining' else context.termination_url
    sqs.send_message(
        QueueUrl=url,
        MessageAttributes={
            'Sender': {
                'DataType': 'String',
                'StringValue': 'sfr',
            },
        },
        MessageBody=json.dumps({
            'instance_id': context.instance_id,
            'ip': '1.2.3.4',
            'hostname': 'the-host',
            'group_id': context.sfr_id,
        }),
    )


@behave.given('a message in the warning queue')
def warning_queue_setup(context):
    sqs.send_message(
        QueueUrl=context.warning_url,
        MessageAttributes={
            'Sender': {
                'DataType': 'String',
                'StringValue': 'aws',
            },
        },
        MessageBody=json.dumps({'detail': {'instance-id': context.instance_id}}),
    )


@behave.when('the draining queue is processed')
def drain_queue_process(context):
    with mock.patch(
        'clusterman.draining.queue.drain',
    ), staticconf.testing.PatchConfiguration(
        {'drain_termination_timeout_seconds': {'sfr': 0}},
    ):
        context.draining_client.process_drain_queue(mock.Mock())


@behave.when('the termination queue is processed')
def termination_queue_process(context):
    context.draining_client.process_termination_queue(mock.Mock())


@behave.when('the warning queue is processed')
def warning_queue_process(context):
    with mock.patch(
        'clusterman.draining.queue.socket.gethostbyaddr',
        return_value=('the-host', '', ''),
    ), mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup.load',
        return_value={context.sfr_id: SpotFleetResourceGroup(context.sfr_id)},
    ), mock.patch(
        'clusterman.aws.spot_fleet_resource_group.load_spot_fleets_from_s3',
    ), mock.patch(
        'clusterman.draining.queue.get_pool_name_list',
        return_value=['bar'],
    ):
        context.draining_client.process_warning_queue()


@behave.then('the host should be terminated')
def check_host_terminated(context):
    response = ec2.describe_spot_fleet_instances(SpotFleetRequestId=context.sfr_id)
    assert_that(response['ActiveInstances'], equal_to([]))


@behave.then('the host should be submitted for (?P<queue_name>draining|termination)')
def check_host_queue(context, queue_name):
    url = context.drain_url if queue_name == 'draining' else context.termination_url
    message_str = sqs.receive_message(QueueUrl=url)['Messages'][0]
    message = json.loads(message_str['Body'])
    assert_that(message['hostname'], equal_to('the-host'))
    assert_that(message['instance_id'], equal_to(context.instance_id))
    assert_that(message['group_id'], equal_to(context.sfr_id))


@behave.then('all queues are empty')
def check_queues_empty(context):
    drain_message_response = sqs.receive_message(QueueUrl=context.drain_url)
    termination_message_response = sqs.receive_message(QueueUrl=context.termination_url)
    warning_message_response = sqs.receive_message(QueueUrl=context.warning_url)
    assert_that(drain_message_response, not_(has_key('Messages')))
    assert_that(termination_message_response, not_(has_key('Messages')))
    assert_that(warning_message_response, not_(has_key('Messages')))
