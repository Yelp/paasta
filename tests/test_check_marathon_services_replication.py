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
import contextlib
from datetime import datetime
from datetime import timedelta

import mock
import pysensu_yelp

from paasta_tools import check_marathon_services_replication
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import SystemPaastaConfig

check_marathon_services_replication.log = mock.Mock()


def test_send_event_users_monitoring_tools_send_event_properly():
    fake_service_name = 'superfast'
    fake_namespace = 'jellyfish'
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    fake_cluster = 'fake_cluster'
    fake_monitoring_overrides = {'fake_key': 'fake_value'}
    fake_soa_dir = '/hi/hello/hey'
    fake_cluster = 'fake_cluster'
    expected_check_name = 'check_marathon_services_replication.%s' % compose_job_id(fake_service_name, fake_namespace)
    with contextlib.nested(
        mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.load_system_paasta_config', autospec=True),
        mock.patch("paasta_tools.check_marathon_services_replication._log", autospec=True),
        mock.patch("paasta_tools.marathon_tools.load_marathon_service_config", autospec=True),
    ) as (
        send_event_patch,
        load_system_paasta_config_patch,
        log_patch,
        load_marathon_service_config_patch,
    ):
        load_marathon_service_config_patch.return_value.get_monitoring.return_value = fake_monitoring_overrides
        check_marathon_services_replication.send_event(fake_service_name,
                                                       fake_namespace,
                                                       fake_cluster,
                                                       fake_soa_dir,
                                                       fake_status,
                                                       fake_output)
        send_event_patch.assert_called_once_with(
            fake_service_name,
            expected_check_name,
            mock.ANY,
            fake_status,
            fake_output,
            fake_soa_dir
        )
        # The overrides dictionary is mutated in the function under test, so
        # we expect the send_event_patch to be called with something that is a
        # superset of what we originally put in (fake_monitoring_overrides)
        actual_overrides_used = send_event_patch.call_args[0][2]
        assert set({'alert_after': '2m'}.items()).issubset(set(actual_overrides_used.items()))
        assert 'runbook' in actual_overrides_used


def test_send_event_users_monitoring_tools_send_event_respects_alert_after():
    fake_service_name = 'superfast'
    fake_namespace = 'jellyfish'
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    fake_cluster = 'fake_cluster'
    fake_monitoring_overrides = {'alert_after': '666m'}
    fake_soa_dir = '/hi/hello/hey'
    fake_cluster = 'fake_cluster'
    expected_check_name = 'check_marathon_services_replication.%s' % compose_job_id(fake_service_name, fake_namespace)
    with contextlib.nested(
        mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.load_system_paasta_config', autospec=True),
        mock.patch("paasta_tools.check_marathon_services_replication._log", autospec=True),
        mock.patch("paasta_tools.marathon_tools.load_marathon_service_config", autospec=True),
    ) as (
        send_event_patch,
        load_system_paasta_config_patch,
        log_patch,
        load_marathon_service_config_patch,
    ):
        load_marathon_service_config_patch.return_value.get_monitoring.return_value = fake_monitoring_overrides
        check_marathon_services_replication.send_event(fake_service_name,
                                                       fake_namespace,
                                                       fake_cluster,
                                                       fake_soa_dir,
                                                       fake_status,
                                                       fake_output)
        send_event_patch.call_count == 1
        send_event_patch.assert_called_once_with(
            fake_service_name,
            expected_check_name,
            mock.ANY,
            fake_status,
            fake_output,
            fake_soa_dir
        )
        # The overrides dictionary is mutated in the function under test, so
        # we expect the send_event_patch to be called with something that is a
        # superset of what we originally put in (fake_monitoring_overrides)
        actual_overrides_used = send_event_patch.call_args[0][2]
        assert set({'alert_after': '666m'}.items()).issubset(set(actual_overrides_used.items()))
        assert not set({'alert_after': '2m'}.items()).issubset(set(actual_overrides_used.items()))


def test_check_smartstack_replication_for_instance_ok_when_expecting_zero():
    service = 'test'
    instance = 'main'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.main': 1, 'test.three': 4, 'test.four': 8}}
    expected_replication_count = 0
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90

    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_load_smartstack_info_for_service.return_value = available

        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config

        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )


def test_check_smartstack_replication_for_instance_crit_when_absent():
    service = 'test'
    instance = 'some_absent_instance'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.two': 1, 'test.three': 4, 'test.four': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_load_smartstack_info_for_service.return_value = available
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_zero_replication():
    service = 'test'
    instance = 'zero_running'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.zero_running': 0, 'test.main': 8, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_load_smartstack_info_for_service.return_value = available
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert 'Service test.zero_running has 0 out of 8 expected instances in fake_region' in alert_output
        assert "paasta status -s test -i zero_running -c fake_cluster -vv" in alert_output


def test_check_smartstack_replication_for_instance_crit_when_low_replication():
    service = 'test'
    instance = 'not_enough'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.canary': 1, 'test.not_enough': 4, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert 'Service test.not_enough has 4 out of 8 expected instances in fake_region' in alert_output
        assert "paasta status -s test -i not_enough -c fake_cluster -vv" in alert_output


def test_check_smartstack_replication_for_instance_ok_with_enough_replication():
    service = 'test'
    instance = 'everything_up'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.canary': 1, 'test.low_replication': 4, 'test.everything_up': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.everything_up has 8 out of 8 expected instances in fake_region (OK: 100%)" in alert_output


def test_check_smartstack_replication_for_instance_ignores_things_under_a_different_namespace():
    service = 'test'
    instance = 'main'
    namespace = 'canary'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.canary': 1, 'test.main': 4, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=namespace),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event_if_under_replication,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event_if_under_replication.call_count == 0


def test_check_smartstack_replication_for_instance_ok_with_enough_replication_multilocation():
    service = 'test'
    instance = 'everything_up'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.everything_up': 1}, 'fake_other_region': {'test.everything_up': 1}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True),
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.everything_up has 1 out of 1 expected instances in fake_region" in alert_output
        assert "test.everything_up has 1 out of 1 expected instances in fake_other_region" in alert_output


def test_check_smartstack_replication_for_instance_crit_when_low_replication_multilocation():
    service = 'test'
    instance = 'low_replication'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.low_replication': 1}, 'fake_other_region': {'test.low_replication': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.low_replication has 1 out of 1 expected instances in fake_region" in alert_output
        assert "test.low_replication has 0 out of 1 expected instances in fake_other_region" in alert_output
        assert "paasta status -s test -i low_replication -c fake_cluster -vv" in alert_output


def test_check_smartstack_replication_for_instance_crit_when_zero_replication_multilocation():
    service = 'test'
    instance = 'zero_running'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.zero_running': 0}, 'fake_other_region': {'test.zero_running': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.zero_running has 0 out of 1 expected instances in fake_region (CRITICAL: 0%)" in alert_output
        assert "test.zero_running has 0 out of 1 expected instances in fake_other_region (CRITICAL: 0%)" in alert_output
        assert "paasta status -s test -i zero_running -c fake_cluster -vv" in alert_output


def test_check_smartstack_replication_for_instance_crit_when_missing_replication_multilocation():
    service = 'test'
    instance = 'missing_instance'
    cluster = 'fake_cluster'
    available = {'fake_region': {'test.main': 0}, 'fake_other_region': {'test.main': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        mock_load_smartstack_info_for_service.return_value = available
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config,
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.missing_instance has 0 out of 1 expected instances in fake_region" in alert_output
        assert "test.missing_instance has 0 out of 1 expected instances in fake_other_region" in alert_output


def test_check_smartstack_replication_for_instance_crit_when_no_smartstack_info():
    service = 'test'
    instance = 'some_instance'
    cluster = 'fake_cluster'
    available = {}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    crit = 90
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_registration_for_service_instance',
                   autospec=True, return_value=compose_job_id(service, instance)),
        mock.patch('paasta_tools.check_marathon_services_replication.load_smartstack_info_for_service', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    ) as (
        mock_send_event,
        mock_read_registration_for_service_instance,
        mock_load_smartstack_info_for_service,
        mock_load_marathon_service_config,
    ):
        mock_load_smartstack_info_for_service.return_value = available
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, cluster, soa_dir, expected_replication_count, fake_system_paasta_config
        )
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test.some_instance has no Smartstack replication info." in alert_output


def test_check_service_replication_for_normal_smartstack():
    service = 'test_service'
    instance = 'test_instance'
    cluster = 'fake_cluster'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance',
                   autospec=True, return_value=666),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True, return_value=100),
        mock.patch('paasta_tools.check_marathon_services_replication.check_smartstack_replication_for_instance',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
        mock_check_smartstack_replication_for_service
    ):
        mock_client = mock.Mock()
        check_marathon_services_replication.check_service_replication(
            client=mock_client, service=service, instance=instance, cluster=cluster, soa_dir=None,
            system_paasta_config=fake_system_paasta_config)
        mock_check_smartstack_replication_for_service.assert_called_once_with(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=None,
            expected_count=100,
            system_paasta_config=fake_system_paasta_config,
        )


def test_check_service_replication_for_non_smartstack():
    service = 'test_service'
    instance = 'worker'
    cluster = 'fake_cluster'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance', autospec=True, return_value=None),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True, return_value=100),
        mock.patch('paasta_tools.check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
        mock_check_healthy_marathon_tasks,
    ):
        mock_client = mock.Mock()
        check_marathon_services_replication.check_service_replication(
            client=mock_client, service=service, instance=instance, cluster=cluster, soa_dir=None,
            system_paasta_config=fake_system_paasta_config)

        mock_check_healthy_marathon_tasks.assert_called_once_with(
            client=mock_client,
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=None,
            expected_count=100,
        )


def test_get_healthy_marathon_instances_for_short_app_id_correctly_counts_alive_tasks():
    fake_client = mock.Mock()
    fakes = []
    for i in range(0, 4):
        fake_task = mock.Mock()
        fake_task.app_id = '/service.instance.foo%s.bar%s' % (i, i)
        mock_result = mock.Mock()
        mock_result.alive = True if i % 2 == 0 else False
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    fake_client.list_tasks.return_value = fakes
    actual = check_marathon_services_replication.get_healthy_marathon_instances_for_short_app_id(
        fake_client,
        'service.instance',
    )
    assert actual == 2


def test_get_healthy_marathon_instances_for_short_app_id_considers_new_tasks_not_healthy_yet():
    fake_client = mock.Mock()
    fakes = []
    one_minute = timedelta(minutes=1)
    for i in range(0, 4):
        fake_task = mock.Mock()
        fake_task.app_id = '/service.instance.foo%s.bar%s' % (i, i)

        # when i == 0, produces a task that has just started (not healthy yet)
        # otherwise produces a task that was started over a minute ago (healthy)
        fake_task.started_at = datetime.now() - one_minute * i
        mock_result = mock.Mock()
        mock_result.alive = True
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    fake_client.list_tasks.return_value = fakes
    actual = check_marathon_services_replication.get_healthy_marathon_instances_for_short_app_id(
        fake_client,
        'service.instance',
    )
    assert actual == 3


def test_get_healthy_marathon_instances_for_short_app_id_considers_none_start_time_unhealthy():
    fake_client = mock.Mock()

    fake_task = mock.Mock()
    fake_task.app_id = '/service.instance.foo.bar'
    fake_task.started_at = None
    mock_result = mock.Mock()
    mock_result.alive = True
    fake_task.health_check_results = [mock_result]

    fakes = [fake_task]
    fake_client.list_tasks.return_value = fakes
    actual = check_marathon_services_replication.get_healthy_marathon_instances_for_short_app_id(
        fake_client,
        'service.instance',
    )
    assert actual == 0


@mock.patch('paasta_tools.check_marathon_services_replication.send_event_if_under_replication', autospec=True)
@mock.patch('paasta_tools.check_marathon_services_replication.get_healthy_marathon_instances_for_short_app_id', autospec=True)  # noqa
def test_check_healthy_marathon_tasks_for_service_instance(mock_healthy_instances,
                                                           mock_send_event_if_under_replication):
    service = 'service'
    instance = 'instance'
    cluster = 'cluster'
    soa_dir = 'soa_dir'
    client = mock.Mock()
    mock_healthy_instances.return_value = 2
    check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance(
        client=client,
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
        expected_count=10
    )
    mock_send_event_if_under_replication.assert_called_once_with(
        service=service,
        instance=instance,
        cluster=cluster,
        expected_count=10,
        num_available=2,
        soa_dir=soa_dir
    )


def test_check_service_replication_for_namespace_with_no_deployments():
    service = 'test_service'
    instance = 'worker'
    cluster = 'fake_cluster'
    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')

    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance', autospec=True, return_value=None),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
    ):
        mock_client = mock.Mock()
        mock_get_expected_count.side_effect = check_marathon_services_replication.NoDeploymentsAvailable
        check_marathon_services_replication.check_service_replication(
            client=mock_client, service=service, instance=instance, cluster=cluster, soa_dir=None,
            system_paasta_config=fake_system_paasta_config)
        assert mock_get_proxy_port_for_instance.call_count == 0


def test_send_event_if_under_replication_handles_0_expected():
    service = 'test_service'
    instance = 'worker'
    cluster = 'fake_cluster'
    crit = 90
    expected_count = 0
    available = 0
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True),
    ) as (
        mock_send_event,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config

        check_marathon_services_replication.send_event_if_under_replication(
            service, instance, cluster, expected_count, available, soa_dir)
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=0,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test_service.worker has 0 out of 0 expected instances available!\n(threshold: 90%)" in alert_output


def test_send_event_if_under_replication_good():
    service = 'test_service'
    instance = 'worker'
    cluster = 'fake_cluster'
    crit = 90
    expected_count = 100
    available = 100
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True),
    ) as (
        mock_send_event,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config

        check_marathon_services_replication.send_event_if_under_replication(
            service, instance, cluster, expected_count, available, soa_dir)
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=0,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test_service.worker has 100 out of 100 expected instances available!\n(threshold: 90%)" in alert_output


def test_send_event_if_under_replication_critical():
    service = 'test_service'
    instance = 'worker'
    cluster = 'fake_cluster'
    crit = 90
    expected_count = 100
    available = 89
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True),
    ) as (
        mock_send_event,
        mock_load_marathon_service_config,
    ):
        mock_service_job_config = mock.MagicMock(spec_set=MarathonServiceConfig)
        mock_service_job_config.get_replication_crit_percentage.return_value = crit
        mock_load_marathon_service_config.return_value = mock_service_job_config

        check_marathon_services_replication.send_event_if_under_replication(
            service=service,
            instance=instance,
            cluster=cluster,
            expected_count=expected_count,
            num_available=available,
            soa_dir=soa_dir)
        mock_send_event.assert_called_once_with(
            service=service,
            namespace=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            status=2,
            output=mock.ANY,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert "test_service.worker has 89 out of 100 expected instances available!\n(threshold: 90%)" in alert_output
        assert "paasta status -s test_service -i worker -c fake_cluster -vv" in alert_output


def test_main():
    soa_dir = 'anw'
    crit = 1
    services = [('a', 'main'), ('b', 'main'), ('c', 'main')]
    args = mock.Mock(soa_dir=soa_dir, crit=crit, verbose=False)
    with contextlib.nested(
        mock.patch('paasta_tools.check_marathon_services_replication.parse_args',
                   return_value=args, autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.get_services_for_cluster',
                   return_value=services, autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.check_service_replication',
                   autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.load_system_paasta_config',
                   autospec=True),
        mock.patch('paasta_tools.check_marathon_services_replication.marathon_tools.load_marathon_config',
                   autospec=True)
    ) as (
        mock_parse_args,
        mock_get_services_for_cluster,
        mock_check_service_replication,
        mock_load_system_paasta_config,
        mock_load_marathon_config,
    ):
        mock_config = mock.Mock()
        mock_load_marathon_config.return_value = mock_config
        mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
        check_marathon_services_replication.main()
        mock_parse_args.assert_called_once_with()
        mock_get_services_for_cluster.assert_called_once_with(
            cluster='fake_cluster', instance_type='marathon', soa_dir=soa_dir)
