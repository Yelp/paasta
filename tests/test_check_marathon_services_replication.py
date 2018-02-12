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
from datetime import datetime
from datetime import timedelta

import mock
import pysensu_yelp
import pytest

from paasta_tools import check_marathon_services_replication
from paasta_tools.utils import compose_job_id

check_marathon_services_replication.log = mock.Mock()


@pytest.fixture
def instance_config():
    service = 'fake_service'
    instance = 'fake_instance'
    job_id = compose_job_id(service, instance)
    mock_instance_config = mock.Mock(
        service=service,
        instance=instance,
        cluster='fake_cluster',
        soa_dir='fake_soa_dir',
        job_id=job_id,
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 90
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config


def test_send_event_users_monitoring_tools_send_event_properly(instance_config):
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    instance_config.get_monitoring.return_value = {'fake_key': 'fake_value'}

    expected_check_name = 'check_marathon_services_replication.%s' % instance_config.job_id
    with mock.patch(
        'paasta_tools.monitoring_tools.send_event', autospec=True,
    ) as send_event_patch, mock.patch(
        'paasta_tools.check_marathon_services_replication._log', autospec=True,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.monitoring_tools.get_runbook',
        autospec=True,
        return_value='y/runbook',
    ):
        check_marathon_services_replication.send_event(
            instance_config=instance_config,
            status=fake_status,
            output=fake_output,
        )
        send_event_patch.assert_called_once_with(
            service=instance_config.service,
            check_name=expected_check_name,
            overrides={
                'fake_key': 'fake_value', 'runbook': 'y/runbook',
                'alert_after': '2m', 'check_every': '1m',
            },
            status=fake_status,
            output=fake_output,
            soa_dir=instance_config.soa_dir,
            cluster=instance_config.cluster,
        )


def test_send_event_users_monitoring_tools_send_event_respects_alert_after(instance_config):
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    instance_config.get_monitoring.return_value = {'alert_after': '666m'}
    expected_check_name = (
        'check_marathon_services_replication.%s' %
        instance_config.job_id
    )
    with mock.patch(
        "paasta_tools.monitoring_tools.send_event", autospec=True,
    ) as send_event_patch, mock.patch(
        "paasta_tools.check_marathon_services_replication._log", autospec=True,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.monitoring_tools.get_runbook',
        autospec=True,
        return_value='y/runbook',
    ):
        check_marathon_services_replication.send_event(
            instance_config=instance_config,
            status=fake_status,
            output=fake_output,
        )
        send_event_patch.call_count == 1
        send_event_patch.assert_called_once_with(
            service=instance_config.service,
            check_name=expected_check_name,
            overrides={
                'runbook': 'y/runbook',
                'alert_after': '666m', 'check_every': '1m',
            },
            status=fake_status,
            output=fake_output,
            soa_dir=instance_config.soa_dir,
            cluster=instance_config.cluster,
        )


def test_check_smartstack_replication_for_instance_ok_when_expecting_zero(instance_config):
    expected_replication_count = 0
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {'fake_region': {'test.main': 1, 'test.three': 4, 'test.four': 8}}

    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )


def test_check_smartstack_replication_for_instance_crit_when_absent(instance_config):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {'fake_region': {'test.two': 1, 'test.three': 4, 'test.four': 8}}
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )


def test_check_smartstack_replication_for_instance_crit_when_zero_replication(instance_config):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {
                'fake_service.fake_instance': 0,
                'test.main': 8,
                'test.fully_replicated': 8,
            },
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ('Service {} has 0 out of 8 expected instances in fake_region'
                .format(instance_config.job_id)) in alert_output
        assert ('paasta status -s {} -i {} -c {} -vv'
                .format(
                    instance_config.service,
                    instance_config.instance,
                    instance_config.cluster,
                )) in alert_output


def test_check_smartstack_replication_for_instance_crit_when_low_replication(instance_config):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {
                'test.canary': 1,
                'fake_service.fake_instance': 4,
                'test.fully_replicated': 8,
            },
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ('Service {} has 4 out of 8 expected instances in fake_region'
                .format(instance_config.job_id)) in alert_output
        assert ('paasta status -s {} -i {} -c {} -vv'
                .format(
                    instance_config.service,
                    instance_config.instance,
                    instance_config.cluster,
                )) in alert_output


def test_check_smartstack_replication_for_instance_ok_with_enough_replication(instance_config):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {
                'test.canary': 1,
                'test.low_replication': 4,
                'fake_service.fake_instance': 8,
            },
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ('{} has 8 out of 8 expected instances in fake_region (OK: 100%)'
                .format(instance_config.job_id)) in alert_output


def test_check_smartstack_replication_for_instance_ok_with_enough_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {'fake_service.fake_instance': 1},
            'fake_other_region': {'fake_service.fake_instance': 1},
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 1 out of 1 expected instances in fake_region"
                .format(instance_config.job_id)) in alert_output
        assert ("{} has 1 out of 1 expected instances in fake_other_region"
                .format(instance_config.job_id)) in alert_output


def test_check_smartstack_replication_for_instance_crit_when_low_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {'fake_service.fake_instance': 1},
            'fake_other_region': {'fake_service.fake_instance': 0},
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 1 out of 1 expected instances in fake_region"
                .format(instance_config.job_id)) in alert_output
        assert ("{} has 0 out of 1 expected instances in fake_other_region"
                .format(instance_config.job_id)) in alert_output
        assert ("paasta status -s {} -i {} -c {} -vv"
                .format(
                    instance_config.service,
                    instance_config.instance,
                    instance_config.cluster,
                )) in alert_output


def test_check_smartstack_replication_for_instance_crit_when_zero_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {
            'fake_region': {'fake_service.fake_instance': 0},
            'fake_other_region': {'fake_service.fake_instance': 0},
        }
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 0 out of 1 expected instances in fake_region"
                .format(instance_config.job_id)) in alert_output
        assert ("{} has 0 out of 1 expected instances in fake_other_region"
                .format(instance_config.job_id)) in alert_output
        assert ("paasta status -s {} -i {} -c {} -vv"
                .format(
                    instance_config.service,
                    instance_config.instance,
                    instance_config.cluster,
                )) in alert_output


def test_check_smartstack_replication_for_instance_crit_when_missing_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = \
        {'fake_region': {'test.main': 0}, 'fake_other_region': {'test.main': 0}}
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 0 out of 1 expected instances in fake_region"
                .format(instance_config.job_id)) in alert_output
        assert ("{} has 0 out of 1 expected instances in fake_other_region"
                .format(instance_config.job_id)) in alert_output


def test_check_smartstack_replication_for_instance_crit_when_no_smartstack_info(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {}
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            smartstack_replication_checker=mock_smartstack_replication_checker,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has no Smartstack replication info."
                .format(instance_config.job_id)) in alert_output


def test_check_service_replication_for_normal_smartstack(instance_config):
    instance_config.get_instances.return_value = 100
    all_tasks = []
    with mock.patch(
        'paasta_tools.marathon_tools.get_proxy_port_for_instance',
        autospec=True,
        return_value=666,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.check_smartstack_replication_for_instance',
        autospec=True,
    ) as mock_check_smartstack_replication_for_service:
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks=all_tasks,
            smartstack_replication_checker=None,
        )
        mock_check_smartstack_replication_for_service.assert_called_once_with(
            instance_config=instance_config,
            expected_count=100,
            smartstack_replication_checker=None,
        )


def test_check_service_replication_for_smartstack_with_different_namespace(instance_config):
    instance_config.get_instances.return_value = 100
    all_tasks = []
    with mock.patch(
        'paasta_tools.marathon_tools.get_proxy_port_for_instance',
        autospec=True,
        return_value=666,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.check_smartstack_replication_for_instance',
        autospec=True,
    ) as mock_check_smartstack_replication_for_service, mock.patch(
        'paasta_tools.check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance',
        autospec=True,
    ) as mock_check_healthy_marathon_tasks:
        instance_config.get_registrations.return_value = ['some-random-other-namespace']
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks=all_tasks,
            smartstack_replication_checker=None,
        )
        assert not mock_check_smartstack_replication_for_service.called
        mock_check_healthy_marathon_tasks.assert_called_once_with(
            instance_config=instance_config,
            expected_count=100,
            all_tasks=[],
        )


def test_check_service_replication_for_non_smartstack(instance_config):
    instance_config.get_instances.return_value = 100

    with mock.patch(
        'paasta_tools.marathon_tools.get_proxy_port_for_instance',
        autospec=True,
        return_value=None,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance',
        autospec=True,
    ) as mock_check_healthy_marathon_tasks:
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks=[],
            smartstack_replication_checker=None,
        )
        mock_check_healthy_marathon_tasks.assert_called_once_with(
            instance_config=instance_config,
            expected_count=100,
            all_tasks=[],
        )


def _make_fake_task(app_id, **kwargs):
    kwargs.setdefault('started_at', datetime(1991, 7, 5, 6, 13, 0))
    return mock.Mock(app_id=app_id, **kwargs)


def test_filter_healthy_marathon_instances_for_short_app_id_correctly_counts_alive_tasks():
    fakes = []
    for i in range(0, 4):
        fake_task = _make_fake_task('/service.instance.foo%s.bar%s' % (i, i))
        mock_result = mock.Mock(alive=i % 2 == 0)
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        app_id='service.instance',
        all_tasks=fakes,
    )
    assert actual == 2


def test_filter_healthy_marathon_instances_for_short_app_id_considers_new_tasks_not_healthy_yet():
    one_minute = timedelta(minutes=1)
    fakes = []
    for i in range(0, 4):
        fake_task = _make_fake_task(
            '/service.instance.foo%s.bar%s' % (i, i),
            # when i == 0, produces a task that has just started (not healthy yet)
            # otherwise produces a task that was started over a minute ago (healthy)
            started_at=datetime.now() - one_minute * i,
        )

        mock_result = mock.Mock(alive=True)
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=fakes,
        app_id='service.instance',
    )
    assert actual == 3


def test_get_healthy_marathon_instances_for_short_app_id_considers_none_start_time_unhealthy():
    fake_task = _make_fake_task('/service.instance.foo.bar', started_at=None)
    mock_result = mock.Mock(alive=True)
    fake_task.health_check_results = [mock_result]
    fakes = [fake_task]
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=fakes,
        app_id='service.instance',
    )
    assert actual == 0


@mock.patch('paasta_tools.check_marathon_services_replication.send_event_if_under_replication', autospec=True)
@mock.patch('paasta_tools.check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id', autospec=True)  # noqa
def test_check_healthy_marathon_tasks_for_service_instance(
    mock_healthy_instances,
    mock_send_event_if_under_replication,
    instance_config,
):
    mock_healthy_instances.return_value = 2
    check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance(
        instance_config=instance_config,
        expected_count=10,
        all_tasks=mock.Mock(),
    )
    mock_send_event_if_under_replication.assert_called_once_with(
        instance_config=instance_config,
        expected_count=10,
        num_available=2,
    )


def test_send_event_if_under_replication_handles_0_expected(instance_config):
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.send_event_if_under_replication(
            instance_config=instance_config,
            expected_count=0,
            num_available=0,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=0,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 0 out of 0 expected instances available!\n(threshold: 90%)"
                .format(instance_config.job_id)) in alert_output


def test_send_event_if_under_replication_good(instance_config):
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.send_event_if_under_replication(
            instance_config=instance_config,
            expected_count=100,
            num_available=100,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=0,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 100 out of 100 expected instances available!\n(threshold: 90%)"
                .format(instance_config.job_id)) in alert_output


def test_send_event_if_under_replication_critical(instance_config):
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.send_event', autospec=True,
    ) as mock_send_event:
        check_marathon_services_replication.send_event_if_under_replication(
            instance_config=instance_config,
            expected_count=100,
            num_available=89,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=2,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert ("{} has 89 out of 100 expected instances available!\n(threshold: 90%)"
                .format(instance_config.job_id)) in alert_output
        assert ("paasta status -s {} -i {} -c {} -vv"
                .format(
                    instance_config.service,
                    instance_config.instance,
                    instance_config.cluster,
                )) in alert_output


def test_main(instance_config):
    soa_dir = 'anw'
    crit = 1
    args = mock.Mock(soa_dir=soa_dir, crit=crit, verbose=False)
    instance_config.get_docker_image.return_value = True
    with mock.patch(
        'paasta_tools.check_marathon_services_replication.parse_args',
        return_value=args, autospec=True,
    ) as mock_parse_args, mock.patch(
        'paasta_tools.check_marathon_services_replication.list_services',
        autospec=True,
        return_value=['a'],
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.check_service_replication',
        autospec=True,
    ) as mock_check_service_replication, mock.patch(
        'paasta_tools.check_marathon_services_replication.load_system_paasta_config',
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
        'paasta_tools.check_marathon_services_replication.marathon_tools.get_marathon_clients',
        autospec=True,
    ) as mock_get_marathon_clients, mock.patch(
        'paasta_tools.check_marathon_services_replication.get_slaves',
        autospec=True,
    ), mock.patch(
        'paasta_tools.check_marathon_services_replication.PaastaServiceConfigLoader',
        autospec=True,
    ) as mock_paasta_service_config_loader:
        mock_paasta_service_config_loader.return_value.instance_configs.return_value = [instance_config]
        mock_client = mock.Mock()
        mock_client.list_tasks.return_value = []
        mock_get_marathon_clients.return_value = mock.Mock(get_all_clients=mock.Mock(return_value=[mock_client]))
        mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
        check_marathon_services_replication.main()
        mock_parse_args.assert_called_once_with()
        mock_paasta_service_config_loader.assert_called_once_with(
            service='a',
            soa_dir=soa_dir,
        )
        instance_config.get_docker_image.assert_called_once_with()
        assert mock_check_service_replication.called
