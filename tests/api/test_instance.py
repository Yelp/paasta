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
import datetime

import asynctest
import mock
import pytest
from kubernetes.client import V1Pod
from marathon.models.app import MarathonApp
from marathon.models.app import MarathonTask
from pyramid import testing
from requests.exceptions import ReadTimeout

from paasta_tools import kubernetes_tools
from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.api.views import instance
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.autoscaling.autoscaling_service_lib import ServiceAutoscalingInfo
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.marathon_serviceinit import get_short_task_id
from paasta_tools.mesos.exceptions import SlaveDoesNotExist
from paasta_tools.mesos.slave import MesosSlave
from paasta_tools.mesos.task import Task
from paasta_tools.smartstack_tools import HaproxyBackend
from paasta_tools.smartstack_tools import SmartstackHost
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TimeoutError


@pytest.mark.parametrize("omit_mesos", [True, False])
@pytest.mark.parametrize("omit_smartstack", [True, False])
@mock.patch("paasta_tools.api.views.instance.marathon_mesos_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.marathon_smartstack_status", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.load_service_namespace_config",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.marathon_job_status", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_matching_apps_with_clients",
    autospec=True,
)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_marathon_apps_with_clients",
    autospec=True,
)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.load_marathon_service_config",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_actual_deployments", autospec=True)
def test_instance_status_marathon(
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_load_marathon_service_config,
    mock_get_marathon_apps_with_clients,
    mock_get_matching_apps_with_clients,
    mock_marathon_job_status,
    mock_load_service_namespace_config,
    mock_marathon_smartstack_status,
    mock_marathon_mesos_status,
    omit_smartstack,
    omit_mesos,
):
    settings.cluster = "fake_cluster"

    mock_get_actual_deployments.return_value = {
        "fake_cluster.fake_instance": "GIT_SHA",
        "fake_cluster.fake_instance2": "GIT_SHA",
        "fake_cluster2.fake_instance": "GIT_SHA",
        "fake_cluster2.fake_instance2": "GIT_SHA",
    }
    mock_validate_service_instance.return_value = "marathon"

    settings.marathon_clients = mock.Mock()

    mock_service_config = marathon_tools.MarathonServiceConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"bounce_method": "fake_bounce"},
        branch_dict=None,
    )
    mock_load_marathon_service_config.return_value = mock_service_config
    mock_app = mock.Mock(tasks=[mock.Mock()])
    mock_get_matching_apps_with_clients.return_value = [(mock_app, mock.Mock())]

    mock_marathon_job_status.return_value = {
        "marathon_job_status_field1": "field1_value",
        "marathon_job_status_field2": "field2_value",
    }
    mock_load_service_namespace_config.return_value = {"proxy_port": 1234}

    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_instance",
        "verbose": 2,
        "omit_smartstack": omit_smartstack,
        "omit_mesos": omit_mesos,
    }
    response = instance.instance_status(request)

    expected_response = {
        "marathon_job_status_field1": "field1_value",
        "marathon_job_status_field2": "field2_value",
    }
    if not omit_smartstack:
        expected_response["smartstack"] = mock_marathon_smartstack_status.return_value
    if not omit_mesos:
        expected_response["mesos"] = mock_marathon_mesos_status.return_value
    assert response["marathon"] == expected_response

    mock_marathon_job_status.assert_called_once_with(
        "fake_service",
        "fake_instance",
        mock_service_config,
        mock_get_matching_apps_with_clients.return_value,
        2,
    )
    if not omit_mesos:
        mock_marathon_mesos_status.assert_called_once_with(
            "fake_service", "fake_instance", 2
        )
    if not omit_smartstack:
        mock_marathon_smartstack_status.assert_called_once_with(
            "fake_service",
            "fake_instance",
            mock_service_config,
            mock_load_service_namespace_config.return_value,
            mock_app.tasks,
            should_return_individual_backends=True,
        )


@pytest.fixture
def mock_service_config():
    return marathon_tools.MarathonServiceConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"bounce_method": "fake_bounce"},
        branch_dict=None,
    )


@mock.patch("paasta_tools.api.views.instance.marathon_app_status", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_marathon_app_deploy_status",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.get_autoscaling_info", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.get_marathon_dashboard_links", autospec=True
)
def test_marathon_job_status(
    mock_get_marathon_dashboard_links,
    mock_get_autoscaling_info,
    mock_get_marathon_app_deploy_status,
    mock_marathon_app_status,
    mock_service_config,
):
    mock_service_config.format_marathon_app_dict = lambda: {"id": "foo"}
    settings.system_paasta_config = mock.create_autospec(SystemPaastaConfig)

    mock_get_marathon_app_deploy_status.return_value = 0  # Running status
    mock_get_autoscaling_info.return_value = ServiceAutoscalingInfo(
        current_instances=1,
        max_instances=5,
        min_instances=1,
        current_utilization=None,
        target_instances=3,
    )

    mock_app = mock.Mock(id="/foo", tasks_running=2)
    job_status = instance.marathon_job_status(
        "fake_service",
        "fake_instance",
        mock_service_config,
        marathon_apps_with_clients=[(mock_app, mock.Mock())],
        verbose=3,
    )

    expected_autoscaling_info = mock_get_autoscaling_info.return_value._asdict()
    del expected_autoscaling_info["current_utilization"]

    assert job_status == {
        "app_statuses": [mock_marathon_app_status.return_value],
        "app_count": 1,
        "desired_state": "start",
        "bounce_method": "fake_bounce",
        "expected_instance_count": 1,
        "desired_app_id": "foo",
        "deploy_status": "Running",
        "running_instance_count": 2,
        "autoscaling_info": expected_autoscaling_info,
    }

    assert mock_marathon_app_status.call_count == 1


def test_marathon_job_status_error(mock_service_config):
    mock_service_config.format_marathon_app_dict = mock.Mock(
        side_effect=NoDockerImageError
    )

    job_status = instance.marathon_job_status(
        "fake_service",
        "fake_instance",
        mock_service_config,
        marathon_apps_with_clients=[],
        verbose=0,
    )

    assert len(job_status["error_message"]) > 0


@mock.patch("paasta_tools.api.views.instance.marathon_app_status", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.get_marathon_dashboard_links", autospec=True
)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_marathon_app_deploy_status",
    autospec=True,
)
def test_marathon_job_status_no_dashboard_links(
    mock_get_marathon_app_deploy_status,
    mock_get_marathon_dashboard_links,
    mock_marathon_app_status,
    mock_service_config,
):
    mock_service_config.format_marathon_app_dict = lambda: {"id": "foo"}
    settings.system_paasta_config = mock.create_autospec(SystemPaastaConfig)
    mock_get_marathon_app_deploy_status.return_value = 0  # Running status
    mock_app = mock.Mock(id="/foo", tasks_running=2)

    mock_get_marathon_dashboard_links.return_value = None

    # just make sure we don't throw an error
    instance.marathon_job_status(
        "fake_service",
        "fake_instance",
        mock_service_config,
        marathon_apps_with_clients=[(mock_app, mock.Mock())],
        verbose=0,
    )


@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.summarize_unused_offers",
    autospec=True,
)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_app_queue", autospec=True
)
class TestMarathonAppStatus:
    @pytest.fixture
    def mock_app(self):
        mock_task = mock.create_autospec(
            MarathonTask,
            id="bar.baz",
            host="host1.paasta.party",
            ports=[123, 456],
            staged_at=datetime.datetime(2019, 6, 14, 13, 0, 0),
            health_check_results=[mock.Mock(alive=True)],
        )
        return mock.create_autospec(
            MarathonApp,
            id="foo",
            tasks_running=3,
            tasks_healthy=2,
            tasks_staged=0,
            instances=4,
            version="2019-06-14T12:34:56",
            tasks=[mock_task],
        )

    def test_app_status(
        self, mock_get_app_queue, mock_summarize_unused_offers, mock_app
    ):
        app_status = instance.marathon_app_status(
            mock_app,
            marathon_client=mock.Mock(),
            dashboard_link="https://paasta.party/",
            deploy_status=marathon_tools.MarathonDeployStatus.Running,
            list_tasks=False,
        )
        assert app_status == {
            "tasks_running": 3,
            "tasks_healthy": 2,
            "tasks_staged": 0,
            "tasks_total": 4,
            "create_timestamp": 1560515696,
            "deploy_status": "Running",
            "unused_offers": mock_summarize_unused_offers.return_value,
            "dashboard_url": "https://paasta.party/ui/#/apps/%2Ffoo",
        }

    @mock.patch(
        "paasta_tools.api.views.instance.marathon_tools.get_app_queue_status_from_queue",
        autospec=True,
    )
    def test_backoff_seconds(
        self,
        mock_get_app_queue_status_from_queue,
        mock_get_app_queue,
        mock_summarize_unused_offers,
        mock_app,
    ):
        mock_get_app_queue_status_from_queue.return_value = (mock.Mock(), 5)
        app_status = instance.marathon_app_status(
            mock_app,
            marathon_client=mock.Mock(),
            dashboard_link="https://paasta.party/",
            deploy_status=marathon_tools.MarathonDeployStatus.Delayed,
            list_tasks=False,
        )

        assert app_status["backoff_seconds"] == 5
        mock_get_app_queue_status_from_queue.assert_called_once_with(
            mock_get_app_queue.return_value
        )

    def test_list_tasks(
        self, mock_get_app_queue, mock_summarize_unused_offers, mock_app
    ):
        app_status = instance.marathon_app_status(
            mock_app,
            marathon_client=mock.Mock(),
            dashboard_link="https://paasta.party/",
            deploy_status=marathon_tools.MarathonDeployStatus.Running,
            list_tasks=True,
        )
        assert app_status["tasks"] == [
            {
                "id": "baz",
                "host": "host1",
                "port": 123,
                "deployed_timestamp": 1560517200,
                "is_healthy": True,
            }
        ]


@mock.patch("paasta_tools.api.views.instance.match_backends_and_tasks", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_backends", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_expected_instance_count_for_namespace",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.get_slaves", autospec=True)
def test_marathon_smartstack_status(
    mock_get_slaves,
    mock_get_expected_instance_count_for_namespace,
    mock_get_backends,
    mock_match_backends_and_tasks,
):
    mock_get_slaves.return_value = [
        {"hostname": "host1.paasta.party", "attributes": {"region": "us-north-3"}}
    ]
    mock_get_expected_instance_count_for_namespace.return_value = 2

    mock_backend = HaproxyBackend(
        status="UP",
        svname="host1_1.2.3.4:123",
        check_status="L7OK",
        check_code="0",
        check_duration="1",
        lastchg="9876",
    )
    mock_task = mock.create_autospec(MarathonTask)
    mock_match_backends_and_tasks.return_value = [(mock_backend, mock_task)]

    settings.system_paasta_config = mock.create_autospec(SystemPaastaConfig)
    settings.system_paasta_config.get_deploy_blacklist.return_value = []
    mock_service_config = marathon_tools.MarathonServiceConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"bounce_method": "fake_bounce"},
        branch_dict=None,
    )
    mock_service_namespace_config = ServiceNamespaceConfig()

    smartstack_status = instance.marathon_smartstack_status(
        "fake_service",
        "fake_instance",
        mock_service_config,
        mock_service_namespace_config,
        tasks=[mock_task],
        should_return_individual_backends=True,
    )
    assert smartstack_status == {
        "registration": "fake_service.fake_instance",
        "expected_backends_per_location": 2,
        "locations": [
            {
                "name": "us-north-3",
                "running_backends_count": 1,
                "backends": [
                    {
                        "hostname": "host1",
                        "port": 123,
                        "status": "UP",
                        "check_status": "L7OK",
                        "check_code": "0",
                        "last_change": 9876,
                        "has_associated_task": True,
                        "check_duration": 1,
                    }
                ],
            }
        ],
    }


@mock.patch("paasta_tools.api.views.instance.match_backends_and_pods", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_backends", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.KubeSmartstackReplicationChecker", autospec=True
)
@mock.patch(
    "paasta_tools.api.views.instance.kubernetes_tools.get_all_nodes", autospec=True
)
def test_kubernetes_smartstack_status(
    mock_get_all_nodes,
    mock_kube_smartstack_replication_checker,
    mock_get_backends,
    mock_match_backends_and_pods,
):
    mock_get_all_nodes.return_value = [
        {"hostname": "host1.paasta.party", "attributes": {"region": "us-north-3"}}
    ]

    mock_kube_smartstack_replication_checker.return_value.get_allowed_locations_and_hosts.return_value = {
        "us-north-3": [SmartstackHost(hostname="host1.paasta.party", pool="default")]
    }

    mock_backend = HaproxyBackend(
        status="UP",
        svname="host1_1.2.3.4:123",
        check_status="L7OK",
        check_code="0",
        check_duration="1",
        lastchg="9876",
    )
    mock_pod = mock.create_autospec(V1Pod)
    mock_match_backends_and_pods.return_value = [(mock_backend, mock_pod)]

    mock_job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"bounce_method": "fake_bounce"},
        branch_dict=None,
    )
    mock_service_namespace_config = ServiceNamespaceConfig()

    smartstack_status = instance.kubernetes_smartstack_status(
        "fake_service",
        "fake_instance",
        mock_job_config,
        mock_service_namespace_config,
        pods=[mock_pod],
        should_return_individual_backends=True,
    )
    assert smartstack_status == {
        "registration": "fake_service.fake_instance",
        "expected_backends_per_location": 1,
        "locations": [
            {
                "name": "us-north-3",
                "running_backends_count": 1,
                "backends": [
                    {
                        "hostname": "host1",
                        "port": 123,
                        "status": "UP",
                        "check_status": "L7OK",
                        "check_code": "0",
                        "last_change": 9876,
                        "has_associated_task": True,
                        "check_duration": 1,
                    }
                ],
            }
        ],
    }


class TestMarathonMesosStatus:
    @pytest.fixture
    def mock_get_cached_list_of_running_tasks_from_frameworks(self):
        with asynctest.patch(
            "paasta_tools.api.views.instance.get_cached_list_of_running_tasks_from_frameworks",
            autospec=True,
        ) as fixture:
            yield fixture

    @pytest.fixture
    def mock_get_cached_list_of_not_running_tasks_from_frameworks(self):
        with asynctest.patch(
            "paasta_tools.api.views.instance.get_cached_list_of_not_running_tasks_from_frameworks",
            autospec=True,
        ) as fixture:
            yield fixture

    @pytest.fixture
    def mock_datetime(self):
        with mock.patch(
            "paasta_tools.api.views.instance.datetime", autospec=True
        ) as fixture:
            yield fixture

    def test_not_verbose(self, mock_get_cached_list_of_running_tasks_from_frameworks):
        mock_task1 = Task(
            master=mock.Mock(), items={"id": "fake--service.fake--instance.1"}
        )
        mock_task2 = Task(
            master=mock.Mock(), items={"id": "fake--service.fake--instance.2"}
        )
        mock_get_cached_list_of_running_tasks_from_frameworks.return_value = [
            mock_task1,
            mock_task2,
        ]
        mesos_status = instance.marathon_mesos_status(
            "fake_service", "fake_instance", verbose=0
        )

        assert mesos_status == {"running_task_count": 2}

    def test_mesos_timeout(self, mock_get_cached_list_of_running_tasks_from_frameworks):
        mock_get_cached_list_of_running_tasks_from_frameworks.side_effect = ReadTimeout
        mesos_status = instance.marathon_mesos_status(
            "fake_service", "fake_instance", verbose=0
        )
        assert mesos_status == {
            "error_message": "Talking to Mesos timed out. It may be overloaded."
        }

    def test_verbose(self, mock_get_cached_list_of_running_tasks_from_frameworks):
        with asynctest.patch(
            "paasta_tools.api.views.instance.get_cached_list_of_not_running_tasks_from_frameworks",
            autospec=True,
        ) as mock_get_cached_list_of_not_running_tasks_from_frameworks, asynctest.patch(
            "paasta_tools.api.views.instance.get_mesos_running_task_dict", autospec=True
        ) as mock_get_mesos_running_task_dict, asynctest.patch(
            "paasta_tools.api.views.instance.get_mesos_non_running_task_dict",
            autospec=True,
        ) as mock_get_mesos_non_running_task_dict:
            running_task_1 = Task(
                master=mock.Mock(), items={"id": "fake--service.fake--instance.1"}
            )
            running_task_2 = Task(
                master=mock.Mock(), items={"id": "fake--service.fake--instance.2"}
            )
            non_running_task = Task(
                master=mock.Mock(),
                items={
                    "id": "fake--service.fake--instance.3",
                    "statuses": [{"timestamp": 1560888500}],
                },
            )

            mock_get_cached_list_of_running_tasks_from_frameworks.return_value = [
                running_task_1,
                running_task_2,
            ]
            mock_get_cached_list_of_not_running_tasks_from_frameworks.return_value = [
                non_running_task
            ]
            mock_get_mesos_running_task_dict.side_effect = lambda task, nlines: {
                "id": task["id"]
            }
            mock_get_mesos_non_running_task_dict.side_effect = lambda task, nlines: {
                "id": task["id"],
                "state": "TASK_LOST",
            }

            mesos_status = instance.marathon_mesos_status(
                "fake_service", "fake_instance", verbose=2
            )

            assert mesos_status == {
                "running_task_count": 2,
                "running_tasks": [
                    {"id": "fake--service.fake--instance.1"},
                    {"id": "fake--service.fake--instance.2"},
                ],
                "non_running_tasks": [
                    {"id": "fake--service.fake--instance.3", "state": "TASK_LOST"}
                ],
            }

            assert mock_get_mesos_running_task_dict.call_count == 2
            mock_get_mesos_running_task_dict.assert_has_calls(
                [mock.call(running_task_1, 10), mock.call(running_task_2, 10)],
                any_order=True,
            )
            mock_get_mesos_non_running_task_dict.assert_called_once_with(
                non_running_task, 10
            )


@pytest.mark.asyncio
class TestGetMesosRunningTaskDict:
    @pytest.fixture
    def mock_task(self):
        mock_slave = MesosSlave(config=mock.Mock(), items={"hostname": "abc.def"})
        mock_task = Task(
            master=mock.Mock(),
            items={
                "id": "fake--service.fake--instance.1",
                "statuses": [{"timestamp": 1560888500}],
            },
        )

        with asynctest.patch.object(
            mock_task, "slave", autospec=True
        ), asynctest.patch.object(mock_task, "stats", autospec=True):
            mock_task.slave.return_value = mock_slave
            yield mock_task

    @pytest.fixture(autouse=True)
    def mock_datetime(self):
        with asynctest.patch(
            "paasta_tools.api.views.instance.datetime", autospec=True
        ) as mock_datetime:
            mock_datetime.datetime.now.return_value = datetime.datetime(
                2019, 6, 18, 22, 14, 0
            )
            yield

    async def test_get_mesos_running_task_dict(self, mock_task):
        mock_task.stats.return_value = {
            "cpus_system_time_secs": 1.2,
            "cpus_user_time_secs": 0.3,
            "mem_limit_bytes": 4,
            "mem_rss_bytes": 5,
            "cpus_limit": 2.2,
        }
        running_task_dict = await instance.get_mesos_running_task_dict(mock_task, 0)

        assert running_task_dict == {
            "id": "1",
            "hostname": "abc",
            "mem_limit": {"value": 4},
            "rss": {"value": 5},
            "cpu_shares": {"value": 2.1},
            "cpu_used_seconds": {"value": 1.5},
            "deployed_timestamp": 1560888500,
            "duration_seconds": 7540,
            "tail_lines": {},
        }

    @pytest.mark.parametrize(
        "error_type,error_message",
        [
            (AttributeError, "None"),
            (SlaveDoesNotExist, "None"),
            (TimeoutError, "Timed Out"),
            (Exception, "Unknown"),
        ],
    )
    async def test_error_handling(self, mock_task, error_type, error_message):
        mock_task.stats.side_effect = error_type
        running_task_dict = await instance.get_mesos_running_task_dict(mock_task, 0)

        for field_name in ("mem_limit", "rss", "cpu_shares", "cpu_used_seconds"):
            assert running_task_dict[field_name] == {"error_message": error_message}

    async def test_tail_lines(self, mock_task):
        with asynctest.patch(
            "paasta_tools.api.views.instance.get_tail_lines_for_mesos_task"
        ) as mock_get_tail_lines:
            running_task_dict = await instance.get_mesos_running_task_dict(
                mock_task, 10
            )

        assert running_task_dict["tail_lines"] == mock_get_tail_lines.return_value
        mock_get_tail_lines.assert_called_once_with(mock_task, get_short_task_id, 10)


@pytest.mark.asyncio
class TestGetMesosNonRunningTaskDict:
    @pytest.fixture
    def mock_task(self):
        mock_slave = MesosSlave(config=mock.Mock(), items={"hostname": "abc.def"})
        mock_task = Task(
            master=mock.Mock(),
            items={
                "id": "fake--service.fake--instance.2",
                "state": "TASK_LOST",
                "statuses": [{"timestamp": 1560888200}],
            },
        )
        with asynctest.patch.object(mock_task, "slave", autospec=True):
            mock_task.slave.return_value = mock_slave
            yield mock_task

    async def test_get_mesos_non_running_task_dict(self, mock_task):
        non_running_task_dict = await instance.get_mesos_non_running_task_dict(
            mock_task, 0
        )
        assert non_running_task_dict == {
            "id": "2",
            "hostname": "abc",
            "state": "TASK_LOST",
            "tail_lines": {},
            "deployed_timestamp": 1560888200,
        }

    async def test_tail_lines(self, mock_task):
        with asynctest.patch(
            "paasta_tools.api.views.instance.get_tail_lines_for_mesos_task"
        ) as mock_get_tail_lines:
            running_task_dict = await instance.get_mesos_non_running_task_dict(
                mock_task, 100
            )

        assert running_task_dict["tail_lines"] == mock_get_tail_lines.return_value
        mock_get_tail_lines.assert_called_once_with(mock_task, get_short_task_id, 100)


@mock.patch("paasta_tools.chronos_tools.load_chronos_config", autospec=True)
@mock.patch(
    "paasta_tools.api.views.instance.chronos_tools.get_chronos_client", autospec=True
)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_actual_deployments", autospec=True)
@mock.patch("paasta_tools.chronos_serviceinit.status_chronos_jobs", autospec=True)
def test_chronos_instance_status(
    mock_status_chronos_jobs,
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_get_chronos_client,
    mock_load_chronos_config,
):
    settings.cluster = "fake_cluster"
    mock_get_actual_deployments.return_value = {
        "fake_cluster.fake_instance": "GIT_SHA",
        "fake_cluster.fake_instance2": "GIT_SHA",
        "fake_cluster2.fake_instance": "GIT_SHA",
        "fake_cluster2.fake_instance2": "GIT_SHA",
    }
    mock_validate_service_instance.return_value = "chronos"

    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service", "instance": "fake_instance"}

    instance.instance_status(request)
    assert mock_status_chronos_jobs.call_args == mock.call(
        mock_get_chronos_client.return_value,
        "fake_service",
        "fake_instance",
        "fake_cluster",
        "/nail/etc/services",
        0,
    )


@mock.patch("paasta_tools.api.views.instance.adhoc_instance_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_actual_deployments", autospec=True)
def test_instances_status_adhoc(
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_adhoc_instance_status,
):
    settings.cluster = "fake_cluster"
    mock_get_actual_deployments.return_value = {
        "fake_cluster.fake_instance": "GIT_SHA",
        "fake_cluster.fake_instance2": "GIT_SHA",
        "fake_cluster2.fake_instance": "GIT_SHA",
        "fake_cluster2.fake_instance2": "GIT_SHA",
    }
    mock_validate_service_instance.return_value = "adhoc"
    mock_adhoc_instance_status.return_value = {}

    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service", "instance": "fake_instance"}

    response = instance.instance_status(request)
    assert mock_adhoc_instance_status.called
    assert response == {
        "service": "fake_service",
        "instance": "fake_instance",
        "git_sha": "GIT_SHA",
        "adhoc": {},
    }


@mock.patch("paasta_tools.api.views.instance.add_executor_info", autospec=True)
@mock.patch("paasta_tools.api.views.instance.add_slave_info", autospec=True)
@mock.patch("paasta_tools.api.views.instance.instance_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_tasks_from_app_id", autospec=True)
def test_instance_tasks(
    mock_get_tasks_from_app_id,
    mock_instance_status,
    mock_add_slave_info,
    mock_add_executor_info,
):
    mock_request = mock.Mock(swagger_data={"task_id": "123", "slave_hostname": "host1"})
    mock_instance_status.return_value = {"marathon": {"app_id": "app1"}}

    mock_task_1 = mock.Mock()
    mock_task_2 = mock.Mock()
    mock_get_tasks_from_app_id.return_value = [mock_task_1, mock_task_2]
    ret = instance.instance_tasks(mock_request)
    assert not mock_add_slave_info.called
    assert not mock_add_executor_info.called

    mock_request = mock.Mock(
        swagger_data={"task_id": "123", "slave_hostname": "host1", "verbose": True}
    )
    ret = instance.instance_tasks(mock_request)
    mock_add_executor_info.assert_has_calls(
        [mock.call(mock_task_1), mock.call(mock_task_2)]
    )
    mock_add_slave_info.assert_has_calls(
        [
            mock.call(mock_add_executor_info.return_value),
            mock.call(mock_add_executor_info.return_value),
        ]
    )
    expected = [
        mock_add_slave_info.return_value._Task__items,
        mock_add_slave_info.return_value._Task__items,
    ]

    def ids(l):
        return {id(x) for x in l}

    assert len(ret) == len(expected) and ids(expected) == ids(ret)

    mock_instance_status.return_value = {"chronos": {}}
    with pytest.raises(ApiFailure):
        ret = instance.instance_tasks(mock_request)


@mock.patch("paasta_tools.api.views.instance.add_executor_info", autospec=True)
@mock.patch("paasta_tools.api.views.instance.add_slave_info", autospec=True)
@mock.patch("paasta_tools.api.views.instance.instance_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_task", autospec=True)
def test_instance_task(
    mock_get_task, mock_instance_status, mock_add_slave_info, mock_add_executor_info
):
    mock_request = mock.Mock(swagger_data={"task_id": "123", "slave_hostname": "host1"})
    mock_instance_status.return_value = {"marathon": {"app_id": "app1"}}

    mock_task_1 = mock.Mock()
    mock_get_task.return_value = mock_task_1
    ret = instance.instance_task(mock_request)
    assert not mock_add_slave_info.called
    assert not mock_add_executor_info.called
    assert ret == mock_task_1._Task__items

    mock_request = mock.Mock(
        swagger_data={"task_id": "123", "slave_hostname": "host1", "verbose": True}
    )
    ret = instance.instance_task(mock_request)
    mock_add_slave_info.assert_called_with(mock_task_1)
    mock_add_executor_info.assert_called_with(mock_add_slave_info.return_value)
    expected = mock_add_executor_info.return_value._Task__items
    assert ret == expected

    mock_instance_status.return_value = {"chronos": {}}
    with pytest.raises(ApiFailure):
        ret = instance.instance_task(mock_request)


@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.get_app_queue", autospec=True
)
@mock.patch(
    "paasta_tools.api.views.instance.marathon_tools.load_marathon_service_config",
    autospec=True,
)
def test_instance_delay(mock_load_config, mock_get_app_queue):
    mock_unused_offers = mock.Mock()
    mock_unused_offers.last_unused_offers = [
        {"reason": ["foo", "bar"]},
        {"reason": ["bar", "baz"]},
        {"reason": []},
    ]
    mock_get_app_queue.return_value = mock_unused_offers

    mock_config = mock.Mock()
    mock_config.format_marathon_app_dict = lambda: {"id": "foo"}
    mock_load_config.return_value = mock_config

    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service", "instance": "fake_instance"}

    response = instance.instance_delay(request)
    assert response["foo"] == 1
    assert response["bar"] == 2
    assert response["baz"] == 1


def test_add_executor_info():
    mock_mesos_task = mock.Mock()
    mock_executor = {
        "tasks": [mock_mesos_task],
        "some": "thing",
        "completed_tasks": [mock_mesos_task],
        "queued_tasks": [mock_mesos_task],
    }
    mock_task = mock.Mock(
        _Task__items={"a": "thing"},
        executor=asynctest.CoroutineMock(
            return_value=mock_executor,
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        ),
    )
    ret = instance.add_executor_info(mock_task)
    expected = {"a": "thing", "executor": {"some": "thing"}}
    assert ret._Task__items == expected
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["completed_tasks"]
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["tasks"]
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["queued_tasks"]


def test_add_slave_info():
    mock_slave = asynctest.CoroutineMock(
        return_value=mock.Mock(_MesosSlave__items={"some": "thing"}),
        func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
    )
    mock_task = mock.Mock(_Task__items={"a": "thing"}, slave=mock_slave)
    expected = {"a": "thing", "slave": {"some": "thing"}}
    assert instance.add_slave_info(mock_task)._Task__items == expected


@mock.patch(
    "paasta_tools.api.views.instance.tron_tools.get_tron_dashboard_for_cluster",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.tron_tools.TronClient", autospec=True)
@mock.patch("paasta_tools.api.views.instance.tron_tools.get_tron_client", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
def test_tron_instance_status(
    mock_validate_service_instance,
    mock_get_tron_client,
    mock_tron_client,
    mock_get_tron_dashboard_for_cluster,
):
    settings.cluster = "fake_cluster"
    mock_validate_service_instance.return_value = "tron"
    mock_client = mock_tron_client("fake_url")
    mock_get_tron_client.return_value = mock_client
    mock_client.get_job_content.return_value = {
        "status": "fake_status",
        "scheduler": {"type": "daily", "value": "1 2 3"},
    }
    mock_client.get_action_run.return_value = {
        "state": "fake_state",
        "start_time": "fake_start_time",
        "raw_command": "fake_raw_command",
        "command": "fake_command",
        "stdout": ["fake_stdout"],
        "stderr": ["fake_stderr"],
    }
    mock_get_tron_dashboard_for_cluster.return_value = "http://fake_url/"

    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_job.fake_action",
    }
    response = instance.instance_status(request)
    assert response["tron"]["job_name"] == "fake_job"
    assert response["tron"]["job_status"] == "fake_status"
    assert response["tron"]["job_schedule"] == "daily 1 2 3"
    assert response["tron"]["job_url"] == "http://fake_url/#job/fake_service.fake_job"
    assert response["tron"]["action_name"] == "fake_action"
    assert response["tron"]["action_state"] == "fake_state"
    assert response["tron"]["action_raw_command"] == "fake_raw_command"
    assert response["tron"]["action_command"] == "fake_command"
    assert response["tron"]["action_start_time"] == "fake_start_time"
    assert response["tron"]["action_stdout"] == "fake_stdout"
    assert response["tron"]["action_stderr"] == "fake_stderr"
