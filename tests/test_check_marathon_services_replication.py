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
from datetime import timezone

import mock
import pytest

from paasta_tools import check_marathon_services_replication
from paasta_tools.utils import compose_job_id

check_marathon_services_replication.log = mock.Mock()


@pytest.fixture
def instance_config():
    service = "fake_service"
    instance = "fake_instance"
    job_id = compose_job_id(service, instance)
    mock_instance_config = mock.Mock(
        service=service,
        instance=instance,
        cluster="fake_cluster",
        soa_dir="fake_soa_dir",
        job_id=job_id,
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 90
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config


def test_check_service_replication_for_normal_smartstack(instance_config):
    instance_config.get_instances.return_value = 100
    all_tasks = []
    with mock.patch(
        "paasta_tools.check_marathon_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=666,
    ), mock.patch(
        "paasta_tools.monitoring_tools.check_replication_for_instance", autospec=True,
    ) as mock_check_replication_for_service:
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks_or_pods=all_tasks,
            replication_checker=None,
        )
        mock_check_replication_for_service.assert_called_once_with(
            instance_config=instance_config,
            expected_count=100,
            replication_checker=None,
        )


def test_check_service_replication_for_smartstack_with_different_namespace(
    instance_config,
):
    instance_config.get_instances.return_value = 100
    all_tasks = []
    with mock.patch(
        "paasta_tools.check_marathon_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=666,
    ), mock.patch(
        "paasta_tools.monitoring_tools.check_replication_for_instance", autospec=True,
    ) as mock_check_replication_for_service, mock.patch(
        "paasta_tools.check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance",
        autospec=True,
    ) as mock_check_healthy_marathon_tasks:
        instance_config.get_registrations.return_value = ["some-random-other-namespace"]
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks_or_pods=all_tasks,
            replication_checker=None,
        )
        assert not mock_check_replication_for_service.called
        mock_check_healthy_marathon_tasks.assert_called_once_with(
            instance_config=instance_config, expected_count=100, all_tasks=[]
        )


def test_check_service_replication_for_non_smartstack(instance_config):
    instance_config.get_instances.return_value = 100

    with mock.patch(
        "paasta_tools.check_marathon_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=None,
    ), mock.patch(
        "paasta_tools.check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance",
        autospec=True,
    ) as mock_check_healthy_marathon_tasks:
        check_marathon_services_replication.check_service_replication(
            instance_config=instance_config,
            all_tasks_or_pods=[],
            replication_checker=None,
        )
        mock_check_healthy_marathon_tasks.assert_called_once_with(
            instance_config=instance_config, expected_count=100, all_tasks=[]
        )


def _make_fake_task(app_id, **kwargs):
    kwargs.setdefault("started_at", datetime(1991, 7, 5, 6, 13, 0, tzinfo=timezone.utc))
    return mock.Mock(app_id=app_id, **kwargs)


def test_filter_healthy_marathon_instances_for_short_app_id_correctly_counts_alive_tasks():
    fakes = []
    for i in range(0, 4):
        fake_task = _make_fake_task(f"/service.instance.foo{i}.bar{i}")
        mock_result = mock.Mock(alive=i % 2 == 0)
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        app_id="service.instance", all_tasks=fakes
    )
    assert actual == 2


def test_filter_healthy_marathon_instances_for_short_app_id_considers_new_tasks_not_healthy_yet():
    one_minute = timedelta(minutes=1)
    fakes = []
    for i in range(0, 4):
        fake_task = _make_fake_task(
            f"/service.instance.foo{i}.bar{i}",
            # when i == 0, produces a task that has just started (not healthy yet)
            # otherwise produces a task that was started over a minute ago (healthy)
            started_at=datetime.now(timezone.utc) - one_minute * i,
        )

        mock_result = mock.Mock(alive=True)
        fake_task.health_check_results = [mock_result]
        fakes.append(fake_task)
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=fakes, app_id="service.instance"
    )
    assert actual == 3


def test_get_healthy_marathon_instances_for_short_app_id_considers_none_start_time_unhealthy():
    fake_task = _make_fake_task("/service.instance.foo.bar", started_at=None)
    mock_result = mock.Mock(alive=True)
    fake_task.health_check_results = [mock_result]
    fakes = [fake_task]
    actual = check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=fakes, app_id="service.instance"
    )
    assert actual == 0


@mock.patch(
    "paasta_tools.monitoring_tools.send_replication_event_if_under_replication",
    autospec=True,
)
@mock.patch(
    "paasta_tools.check_marathon_services_replication.filter_healthy_marathon_instances_for_short_app_id",
    autospec=True,
)  # noqa
def test_check_healthy_marathon_tasks_for_service_instance(
    mock_healthy_instances,
    mock_send_replication_event_if_under_replication,
    instance_config,
):
    mock_healthy_instances.return_value = 2
    check_marathon_services_replication.check_healthy_marathon_tasks_for_service_instance(
        instance_config=instance_config, expected_count=10, all_tasks=mock.Mock()
    )
    mock_send_replication_event_if_under_replication.assert_called_once_with(
        instance_config=instance_config, expected_count=10, num_available=2
    )
