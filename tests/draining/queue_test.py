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
import socket

import arrow
import mock
import pytest
import staticconf.testing

from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.draining.queue import DrainingClient
from clusterman.draining.queue import Host
from clusterman.draining.queue import host_from_instance_id
from clusterman.draining.queue import main
from clusterman.draining.queue import process_queues
from clusterman.draining.queue import terminate_host


@pytest.fixture
def mock_draining_client():
    with mock.patch(
        'clusterman.draining.queue.sqs', autospec=True
    ) as mock_sqs:
        mock_sqs.send_message = mock.Mock()
        mock_sqs.receive_message = mock.Mock()
        mock_sqs.delete_message = mock.Mock()
        return DrainingClient('mesos-test')


def test_submit_instance_for_draining(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_instance = mock.Mock(
            group_id='sfr123',
            hostname='host123',
            instance_id='i123',
            ip_address='10.1.1.1',
        )
        assert mock_draining_client.submit_instance_for_draining(
            mock_instance,
            sender=SpotFleetResourceGroup,
            scheduler='mesos',
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
                'scheduler': 'mesos',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'sfr',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_submit_host_for_draining(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_host = mock.Mock(
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
            sender='aws_2_min_warning',
            scheduler='kubernetes',
        )
        assert mock_draining_client.submit_host_for_draining(
            mock_host,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
                'scheduler': 'kubernetes',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'aws_2_min_warning',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_get_warned_host(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.host_from_instance_id', autospec=True,
    ) as mock_host_from_instance_id:
        mock_draining_client.client.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'rcpt',
                'Body': '{"detail": {"instance-id": "i-123"}}',
            }]
        }
        assert mock_draining_client.get_warned_host() is mock_host_from_instance_id.return_value
        mock_host_from_instance_id.assert_called_with(
            sender='sfr',
            receipt_handle='rcpt',
            instance_id='i-123',
        )
        assert not mock_draining_client.client.delete_message.called

        mock_host_from_instance_id.return_value = None
        assert mock_draining_client.get_warned_host() is None
        assert mock_draining_client.client.delete_message.called


def test_get_warned_host_no_warning_queue_url(mock_draining_client):
    mock_draining_client.warning_queue_url = None
    host = mock_draining_client.get_warned_host()
    assert host is None
    assert mock_draining_client.client.receive_message.call_count == 0


def test_submit_host_for_termination(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_host = mock.Mock(
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
            sender='clusterman',
            scheduler='kubernetes',
        )
        assert mock_draining_client.submit_host_for_termination(
            mock_host,
            delay=0,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
                'scheduler': 'kubernetes',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )

        assert mock_draining_client.submit_host_for_termination(
            mock_host,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
                'scheduler': 'kubernetes',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            DelaySeconds=90,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_get_host_to_drain(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_draining_client.client.receive_message.return_value = {'Messages': []}
        assert mock_draining_client.get_host_to_drain() is None
        mock_draining_client.client.receive_message.return_value = {'Messages': [{
            'MessageAttributes': {'Sender': {'StringValue': 'clusterman'}},
            'ReceiptHandle': 'receipt_id',
            'Body': 'Helloworld',
        }]}
        mock_json.loads.return_value = {
            'instance_id': 'i123',
            'ip': '10.1.1.1',
            'hostname': 'host123',
            'group_id': 'sfr123',
        }

        assert mock_draining_client.get_host_to_drain() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_draining_client.client.receive_message.assert_called_with(
            QueueUrl=mock_draining_client.drain_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_get_host_to_terminate(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_draining_client.client.receive_message.return_value = {'Messages': []}
        assert mock_draining_client.get_host_to_terminate() is None
        mock_draining_client.client.receive_message.return_value = {'Messages': [{
            'MessageAttributes': {'Sender': {'StringValue': 'clusterman'}},
            'ReceiptHandle': 'receipt_id',
            'Body': 'Helloworld',
        }]}
        mock_json.loads.return_value = {
            'instance_id': 'i123',
            'ip': '10.1.1.1',
            'hostname': 'host123',
            'group_id': 'sfr123',
        }

        assert mock_draining_client.get_host_to_terminate() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_draining_client.client.receive_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_delete_drain_message(mock_draining_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_draining_client.delete_drain_messages(mock_hosts)
    mock_draining_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_draining_client.drain_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_draining_client.drain_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_delete_warning_message(mock_draining_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_draining_client.delete_warning_messages(mock_hosts)
    mock_draining_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_draining_client.warning_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_draining_client.warning_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_delete_warning_message_no_warning_queue_url(mock_draining_client):
    mock_draining_client.warning_queue_url = None
    mock_draining_client.delete_warning_messages(['host'])
    assert mock_draining_client.client.delete_message.call_count == 0


def test_delete_terminate_message(mock_draining_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_draining_client.delete_terminate_messages(mock_hosts)
    mock_draining_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_draining_client.termination_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_draining_client.termination_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_process_termination_queue(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.terminate_host', autospec=True,
    ) as mock_terminate, mock.patch(
        'clusterman.draining.queue.down', autospec=True,
    ) as mock_down, mock.patch(
        'clusterman.draining.queue.up', autospec=True,
    ) as mock_up, mock.patch(
        'clusterman.draining.queue.DrainingClient.get_host_to_terminate', autospec=True,
    ) as mock_get_host_to_terminate, mock.patch(
        'clusterman.draining.queue.DrainingClient.delete_terminate_messages', autospec=True,
    ) as mock_delete_terminate_messages:
        mock_mesos_client = mock.Mock()
        mock_get_host_to_terminate.return_value = None
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        assert not mock_terminate.called
        assert not mock_delete_terminate_messages.called

        mock_host = mock.Mock(hostname='', instance_id='i123')
        mock_draining_client.draining_host_ttl_cache[mock_host.instance_id] = arrow.now()
        mock_get_host_to_terminate.return_value = mock_host
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        assert not mock_down.called
        assert not mock_up.called
        mock_delete_terminate_messages.assert_called_with(mock_draining_client, [mock_host])

        mock_host = mock.Mock(hostname='host1', ip='10.1.1.1', instance_id='i123')
        mock_draining_client.draining_host_ttl_cache[mock_host.instance_id] = arrow.now()
        mock_get_host_to_terminate.return_value = mock_host
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        mock_down.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_up.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_delete_terminate_messages.assert_called_with(mock_draining_client, [mock_host])


def test_process_drain_queue(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.drain', autospec=True,
    ) as mock_drain, mock.patch(
        'clusterman.draining.queue.DrainingClient.get_host_to_drain', autospec=True,
    ) as mock_get_host_to_drain, mock.patch(
        'clusterman.draining.queue.DrainingClient.delete_drain_messages', autospec=True,
    ) as mock_delete_drain_messages, mock.patch(
        'clusterman.draining.queue.DrainingClient.submit_host_for_termination', autospec=True,
    ) as mock_submit_host_for_termination, mock.patch(
        'clusterman.draining.queue.arrow', autospec=False,
    ) as mock_arrow:
        mock_arrow.now = mock.Mock(return_value=mock.Mock(timestamp=1))
        mock_mesos_client = mock.Mock()
        mock_get_host_to_drain.return_value = None
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        assert not mock_drain.called
        assert not mock_submit_host_for_termination.called

        mock_host = mock.Mock(hostname='')
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        mock_submit_host_for_termination.assert_called_with(mock_draining_client, mock_host, delay=0)
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])
        assert not mock_drain.called

        mock_host = Host(
            hostname='host1',
            ip='10.1.1.1',
            group_id='sfr1',
            instance_id='i123',
            sender='mmb',
            receipt_handle='aaaaa',
        )
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        mock_drain.assert_called_with(
            mock_mesos_client,
            ['host1|10.1.1.1'],
            1000000000,
            1000000000,
        )
        mock_submit_host_for_termination.assert_called_with(mock_draining_client, mock_host)
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])

        # test we can't submit same host twice
        mock_host = Host(
            hostname='host1',
            ip='10.1.1.1',
            group_id='sfr1',
            instance_id='i123',
            sender='mmb',
            receipt_handle='bbb',
        )
        mock_drain.reset_mock()
        mock_submit_host_for_termination.reset_mock()
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        assert not mock_drain.called
        assert not mock_submit_host_for_termination.called
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])


def test_clean_processing_hosts_cache(mock_draining_client):
    mock_draining_client.draining_host_ttl_cache['i123'] = arrow.get('2018-12-17T16:01:59')
    mock_draining_client.draining_host_ttl_cache['i456'] = arrow.get('2018-12-17T16:02:00')
    with mock.patch(
        'clusterman.draining.queue.arrow', autospec=False
    ) as mock_arrow, mock.patch(
        'clusterman.draining.queue.DRAIN_CACHE_SECONDS', 60
    ):
        mock_arrow.now = mock.Mock(return_value=arrow.get('2018-12-17T16:02:00'))
        mock_draining_client.clean_processing_hosts_cache()
        assert 'i123' not in mock_draining_client.draining_host_ttl_cache
        assert 'i456' in mock_draining_client.draining_host_ttl_cache


def test_process_warning_queue(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.SpotFleetResourceGroup.load',
    ) as mock_load_spot, mock.patch(
        'clusterman.draining.queue.DrainingClient.submit_host_for_draining', autospec=True,
    ) as mock_submit_host_for_draining, mock.patch(
        'clusterman.draining.queue.DrainingClient.delete_warning_messages', autospec=True,
    ) as mock_delete_warning_messages, mock.patch(
        'clusterman.draining.queue.get_pool_name_list', autospec=True,
    ) as mock_get_pools:
        mock_load_spot.return_value = {}
        mock_get_pools.return_value = ['bar']
        mock_host = mock.Mock(group_id='sfr-123')
        mock_draining_client.get_warned_host = mock.Mock(return_value=mock_host)
        mock_draining_client.process_warning_queue()
        assert not mock_submit_host_for_draining.called
        mock_delete_warning_messages.assert_called_with(mock_draining_client, [mock_host])

        mock_load_spot.return_value = {'sfr-123': {}}
        mock_host = mock.Mock(group_id='sfr-123')
        mock_draining_client.get_warned_host = mock.Mock(return_value=mock_host)
        mock_draining_client.process_warning_queue()
        mock_submit_host_for_draining.assert_called_with(mock_draining_client, mock_host)
        mock_delete_warning_messages.assert_called_with(mock_draining_client, [mock_host])


def test_process_queues():
    with mock.patch(
        'clusterman.draining.queue.DrainingClient', autospec=True,
    ) as mock_draining_client, staticconf.testing.PatchConfiguration(
        {'clusters': {'westeros-prod': {'mesos_master_fqdn': 'westeros-prod'}}},
    ), mock.patch(
        'clusterman.draining.queue.time.sleep', autospec=True, side_effect=LoopBreak
    ):
        with pytest.raises(LoopBreak):
            process_queues('westeros-prod')
        assert mock_draining_client.return_value.process_termination_queue.called
        assert mock_draining_client.return_value.process_drain_queue.called
        assert mock_draining_client.return_value.clean_processing_hosts_cache.called
        assert mock_draining_client.return_value.process_warning_queue.called


def test_terminate_host():
    mock_host = mock.Mock(instance_id='i123', sender='sfr', group_id='sfr123')
    mock_sfr = mock.Mock()
    with mock.patch.dict(
        'clusterman.draining.queue.RESOURCE_GROUPS', {'sfr': mock_sfr}, clear=True
    ):
        terminate_host(mock_host)
        mock_sfr.assert_called_with('sfr123')
        mock_sfr.return_value.terminate_instances_by_id.assert_called_with(['i123'])


def test_host_from_instance_id():
    with mock.patch(
        'clusterman.draining.queue.ec2_describe_instances', autospec=True,
    ) as mock_ec2_describe, mock.patch(
        'socket.gethostbyaddr', autospec=True,
    ) as mock_gethostbyaddr:
        mock_ec2_describe.return_value = []
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) is None

        mock_ec2_describe.return_value = [
            {
                'Tags': [
                    {'Key': 'thing', 'Value': 'bar'}
                ]
            }
        ]
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) is None

        mock_ec2_describe.return_value = [
            {
                'Tags': [
                    {'Key': 'aws:ec2spot:fleet-request-id', 'Value': 'sfr-123'}
                ]
            }
        ]
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) is None

        mock_ec2_describe.return_value = [
            {
                'PrivateIpAddress': '10.1.1.1',
                'Tags': [
                    {'Key': 'aws:ec2spot:fleet-request-id', 'Value': 'sfr-123'}
                ]
            }
        ]
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) == Host(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
            hostname=mock_gethostbyaddr.return_value[0],
            group_id='sfr-123',
            ip='10.1.1.1',
        )

        mock_gethostbyaddr.side_effect = socket.error
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) is None

        # instance has no tags, probably because it is new and tags have not
        # yet propagated
        mock_ec2_describe.return_value = [{'InstanceId': 'i-123'}]
        assert host_from_instance_id(
            sender='aws',
            receipt_handle='rcpt',
            instance_id='i-123',
        ) is None


def test_main():
    with mock.patch(
        'clusterman.draining.queue.setup_config', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.load_cluster_pool_config', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.get_pool_name_list', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.process_queues', autospec=True,
    ) as mock_process_queues:
        main(mock.Mock())
        assert mock_process_queues.called


class LoopBreak(Exception):
    pass
