#!/usr/bin/env python
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
import re

import marathon
import mock

from paasta_tools import marathon_serviceinit
from paasta_tools import marathon_tools
from paasta_tools.autoscaling.autoscaling_service_lib import ServiceAutoscalingInfo
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences
from paasta_tools.utils import SystemPaastaConfig


fake_marathon_job_config = marathon_tools.MarathonServiceConfig(
    service="servicename",
    cluster="clustername",
    instance="instancename",
    config_dict={
        "instances": 3,
        "cpus": 1,
        "mem": 100,
        "disk": 512,
        "nerve_ns": "fake_nerve_ns",
    },
    branch_dict={
        "docker_image": "test_docker:1.0",
        "desired_state": "start",
        "force_bounce": None,
    },
)


def test_get_bouncing_status():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.marathon_tools.get_matching_appids",
        autospec=True,
    ) as mock_get_matching_appids:
        mock_get_matching_appids.return_value = ["a", "b"]
        mock_config = marathon_tools.MarathonServiceConfig(
            service="fake_service",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={"bounce_method": "fake_bounce"},
            branch_dict=None,
        )
        actual = marathon_serviceinit.get_bouncing_status(
            "fake_service", "fake_instance", "unused", mock_config
        )
        assert "fake_bounce" in actual
        assert "Bouncing" in actual


def test_status_desired_state():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_bouncing_status", autospec=True
    ) as mock_get_bouncing_status:
        mock_get_bouncing_status.return_value = "Bouncing (fake_bounce)"
        fake_complete_config = mock.Mock()
        fake_complete_config.get_desired_state = mock.Mock(return_value="start")
        actual = marathon_serviceinit.status_desired_state(
            "fake_service", "fake_instance", "unused", fake_complete_config
        )
        assert "Started" in actual
        assert "Bouncing" in actual


def test_status_marathon_job_verbose():
    client = mock.create_autospec(marathon.MarathonClient)
    clients = mock.Mock(get_all_clients_for_service=mock.Mock(return_value=[client]))
    task = mock.Mock()
    app = mock.create_autospec(
        marathon.models.app.MarathonApp,
        id="servicename.instancename.gitAAAAA.configBBBBB",
        tasks=[task],
    )
    client.get_app.return_value = app
    service = "servicename"
    instance = "instancename"
    with mock.patch(
        "paasta_tools.marathon_serviceinit.marathon_tools.get_marathon_apps_with_clients",
        autospec=True,
    ) as mock_get_marathon_apps_with_clients, mock.patch(
        "paasta_tools.marathon_serviceinit.marathon_tools.get_matching_apps_with_clients",
        autospec=True,
    ) as mock_get_matching_apps_with_clients, mock.patch(
        "paasta_tools.marathon_serviceinit.status_marathon_app", autospec=True
    ) as mock_status_marathon_app, mock.patch(
        "paasta_tools.marathon_serviceinit.get_autoscaling_info",
        autospec=True,
        return_value=ServiceAutoscalingInfo(
            current_instances=1,
            max_instances=2,
            min_instances=3,
            current_utilization=0.4,
            target_instances=5,
        ),
    ):
        mock_get_matching_apps_with_clients.return_value = [(app, client)]
        mock_status_marathon_app.return_value = (0, 0, "fake_return")
        tasks, out = marathon_serviceinit.status_marathon_job(
            service=service,
            instance=instance,
            clients=clients,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            job_config=mock.Mock(),
            dashboards=None,
            verbose=1,
            normal_instance_count=3,
            desired_app_id="servicename.instancename.gitAAAAA.configBBBBB",
        )
        mock_get_marathon_apps_with_clients.assert_called_once_with(
            clients=[client], embed_tasks=True, service_name=service
        )
        mock_get_matching_apps_with_clients.assert_called_once_with(
            service, instance, mock_get_marathon_apps_with_clients.return_value
        )
        mock_status_marathon_app.assert_called_once_with(
            marathon_client=client,
            app=app,
            service=service,
            instance=instance,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            dashboards=None,
            verbose=1,
        )
        assert tasks == [task]
        assert "fake_return" in out
        assert "  Autoscaling Info:" in out


def test_status_marathon_app():
    mock_autoscaling_info = ServiceAutoscalingInfo(
        current_instances=str(3),
        max_instances=str(5),
        min_instances=str(1),
        current_utilization="81%",
        target_instances=str(4),
    )

    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_autoscaling_info",
        autospec=True,
        return_value=mock_autoscaling_info,
    ):
        mock_marathon_client = mock.Mock(
            name="client", list_queue=mock.Mock(return_value=[])
        )
        fake_app = mock.Mock(name="app", deployments=[])
        fake_app.version = "2015-01-15T05:30:49.862Z"
        fake_app.id = "/fake--service"
        fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task.id = "fake_task_id"
        fake_task.host = "fake_deployed_host"
        fake_task.ports = [6666]
        fake_task.staged_at = datetime.datetime.fromtimestamp(0)
        fake_task.health_check_results = []
        fake_app.tasks = [fake_task]
        status, tasks, out = marathon_serviceinit.status_marathon_app(
            mock_marathon_client,
            fake_app,
            "fake_service",
            "main",
            "fake_cluster",
            "/nail/blah",
            dashboards={mock_marathon_client: "http://marathon/"},
            verbose=1,
        )
        assert "fake_task_id" in out
        assert "http://marathon/ui/#/apps/%2Ffake--service" in out
        assert "App created: 2015-01-15 05:30:49" in out
        assert "fake_deployed_host:6666" in out
        assert tasks == fake_app.tasks_running


def test_status_marathon_app_no_autoscaling():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_autoscaling_info",
        autospec=True,
        return_value=None,
    ):
        mock_marathon_client = mock.Mock()
        fake_app = mock.Mock()
        fake_app.version = "2015-01-15T05:30:49.862Z"
        fake_app.id = "/fake--service"
        fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task.id = "fake_task_id"
        fake_task.host = "fake_deployed_host"
        fake_task.ports = [6666]
        fake_task.staged_at = datetime.datetime.fromtimestamp(0)
        fake_task.health_check_results = []
        fake_app.tasks = [fake_task]
        fake_app.tasks_running = 1
        fake_app.deployments = []
        mock_marathon_client.list_queue.return_value = []
        deploy_status, num_tasks, out = marathon_serviceinit.status_marathon_app(
            mock_marathon_client,
            fake_app,
            "fake_service",
            "main",
            "fake_cluster",
            "/nail/blah",
            dashboards=None,
            verbose=1,
        )
        assert "fake_task_id" in out
        assert "/fake--service" in out
        assert "App created: 2015-01-15 05:30:49" in out
        assert "fake_deployed_host:6666" in out
        assert "Autoscaling Info" not in out
        assert num_tasks == 1


def test_status_marathon_app_column_alignment():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_autoscaling_info",
        autospec=True,
        return_value=None,
    ):
        mock_marathon_client = mock.Mock()
        mock_marathon_client.list_queue.return_value = []
        fake_app = mock.Mock()
        fake_app.version = "2015-01-15T05:30:49.862Z"
        fake_app.id = "/fake--service"
        fake_app.deployments = []

        fake_task1 = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task1.id = "fake_task1_id"
        fake_task1.host = "fake_deployed_host"
        fake_task1.ports = [6666]
        fake_task1.staged_at = datetime.datetime.fromtimestamp(0)
        fake_task1.health_check_results = []

        fake_task2 = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task2.id = "fake_task2_id"
        fake_task2.host = "fake_deployed_host_with_a_really_long_name"
        fake_task2.ports = [6666]
        fake_task2.staged_at = datetime.datetime.fromtimestamp(0)
        fake_task2.health_check_results = []

        fake_app.tasks = [fake_task1, fake_task2]
        status, tasks, out = marathon_serviceinit.status_marathon_app(
            mock_marathon_client,
            fake_app,
            "fake_service",
            "main",
            "fake_cluster",
            "/nail/blah",
            dashboards={mock_marathon_client: "http://marathon"},
            verbose=1,
        )

        (headers_line, task1_line, task2_line) = out.split("\n")[-3:]
        assert headers_line.index("Host deployed to") == task1_line.index(
            "fake_deployed_host"
        )
        assert headers_line.index("Host deployed to") == task2_line.index(
            "fake_deployed_host_with_a_really_long_name"
        )
        assert headers_line.index("Deployed at what localtime") == task1_line.index(
            "1970-01-01T00:00"
        )
        assert headers_line.index("Deployed at what localtime") == task2_line.index(
            "1970-01-01T00:00"
        )


def tests_status_marathon_job_when_running_running_tasks_with_deployments():
    client = mock.create_autospec(
        marathon.MarathonClient, servers=["server"], name="client"
    )
    app_id = "servicename.instancename.gitAAAA.configBBBB"
    app = mock.Mock(
        name="app",
        id=f"/{app_id}",
        tasks=[],
        deployments=["test_deployment"],
        version="1970-01-01T00:00:00Z",
    )
    client.get_app.return_value = app
    client.list_apps.return_value = [app]
    clients = marathon_tools.MarathonClients(current=[client], previous=[client])
    service = "servicename"
    instance = "instancename"
    cluster = "my_cluster"
    soa_dir = "/soa/dir"
    job_config = mock.Mock()
    job_config.get_marathon_shard.return_value = None
    job_config.get_previous_marathon_shards.return_value = None
    normal_instance_count = 5
    mock_tasks_running = 0
    app.tasks_running = mock_tasks_running
    app.instances = normal_instance_count

    _, output = marathon_serviceinit.status_marathon_job(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
        dashboards=None,
        normal_instance_count=normal_instance_count,
        clients=clients,
        job_config=job_config,
        desired_app_id=app_id,
        verbose=0,
    )

    assert "Deploying" in output


def tests_status_marathon_job_when_running_running_tasks_with_delayed_deployment():
    client = mock.create_autospec(
        marathon.MarathonClient, servers=["server"], name="client"
    )
    app_id = "servicename.instancename.gitAAAA.configBBBB"
    app = mock.Mock(
        name="app",
        id=f"/{app_id}",
        tasks=[],
        deployments=["test_deployment"],
        version="1970-01-01T00:00:00Z",
    )
    client.get_app.return_value = app
    client.list_apps.return_value = [app]
    clients = marathon_tools.MarathonClients(current=[client], previous=[client])
    service = "servicename"
    instance = "instancename"
    cluster = "my_cluster"
    soa_dir = "/soa/dir"
    job_config = mock.Mock()
    job_config.get_marathon_shard.return_value = None
    job_config.get_previous_marathon_shards.return_value = None
    normal_instance_count = 5
    mock_tasks_running = 0
    app.tasks_running = mock_tasks_running
    app.instances = normal_instance_count

    with mock.patch(
        "paasta_tools.marathon_tools.get_app_queue_status",
        return_value=(False, 10),
        autospec=True,
    ) as get_app_queue_status_patch:
        _, output = marathon_serviceinit.status_marathon_job(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            dashboards=None,
            normal_instance_count=normal_instance_count,
            clients=clients,
            job_config=job_config,
            desired_app_id=app_id,
            verbose=0,
        )
        get_app_queue_status_patch.assert_called_with(client, app.id)
        assert "Delayed" in output


def tests_status_marathon_job_when_running_running_tasks_with_waiting_deployment():
    client = mock.create_autospec(
        marathon.MarathonClient, servers=["server"], name="client"
    )
    app_id = "servicename.instancename.gitAAAA.configBBBB"
    app = mock.Mock(
        name="app",
        id=f"/{app_id}",
        tasks=[],
        deployments=["test_deployment"],
        version="1970-01-01T00:00:00Z",
    )
    client.get_app.return_value = app
    client.list_apps.return_value = [app]
    clients = marathon_tools.MarathonClients(current=[client], previous=[client])
    service = "servicename"
    instance = "instancename"
    cluster = "my_cluster"
    soa_dir = "/soa/dir"
    job_config = mock.Mock()
    job_config.get_marathon_shard.return_value = None
    job_config.get_previous_marathon_shards.return_value = None
    normal_instance_count = 5
    mock_tasks_running = 0
    app.tasks_running = mock_tasks_running
    app.instances = normal_instance_count

    with mock.patch(
        "paasta_tools.marathon_tools.get_app_queue_status",
        return_value=(True, 0),
        autospec=True,
    ) as get_app_queue_status_patch:
        _, output = marathon_serviceinit.status_marathon_job(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            dashboards=None,
            normal_instance_count=normal_instance_count,
            clients=clients,
            job_config=job_config,
            desired_app_id=app_id,
            verbose=0,
        )
        get_app_queue_status_patch.assert_called_with(client, app.id)
        assert "Waiting" in output


def tests_status_marathon_job_when_running_running_tasks_with_suspended_deployment():
    client = mock.create_autospec(
        marathon.MarathonClient, servers=["server"], name="client"
    )
    app_id = "servicename.instancename.gitAAAA.configBBBB"
    app = mock.Mock(
        name="app",
        id=f"/{app_id}",
        tasks=[],
        deployments=[],
        version="1970-01-01T00:00:00Z",
    )
    client.get_app.return_value = app
    client.list_apps.return_value = [app]
    clients = marathon_tools.MarathonClients(current=[client], previous=[client])
    service = "servicename"
    instance = "instancename"
    cluster = "my_cluster"
    soa_dir = "/soa/dir"
    job_config = mock.Mock()
    job_config.get_marathon_shard.return_value = None
    job_config.get_previous_marathon_shards.return_value = None
    normal_instance_count = 5
    app.tasks_running = 0
    app.instances = 0

    with mock.patch(
        "paasta_tools.marathon_tools.get_app_queue_status",
        return_value=(None, None),
        autospec=True,
    ) as get_app_queue_status_patch:
        _, output = marathon_serviceinit.status_marathon_job(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            dashboards=None,
            normal_instance_count=normal_instance_count,
            clients=clients,
            job_config=job_config,
            desired_app_id=app_id,
            verbose=0,
        )
        get_app_queue_status_patch.assert_called_with(client, app.id)
        assert "Stopped" in output


def test_format_haproxy_backend_row():
    actual = marathon_serviceinit.format_haproxy_backend_row(
        backend={
            "svname": "169.254.123.1:1234_host1",
            "status": "UP",
            "check_status": "L7OK",
            "check_code": "200",
            "check_duration": 4,
            "lastchg": 0,
        },
        is_correct_instance=True,
    )
    expected = (
        "      host1:1234",
        "L7OK/200 in 4ms",
        "now",
        PaastaColors.default("UP"),
    )
    assert actual == expected


def test_status_smartstack_backends_normal():
    service = "servicename"
    instance = "instancename"

    cluster = "fake_cluster"
    good_task = mock.Mock()
    bad_task = mock.Mock()
    other_task = mock.Mock()
    haproxy_backends_by_task = {
        good_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress1:1001_hostname1",
            "check_status": "L7OK",
            "check_duration": 1,
        },
        bad_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress2:1002_hostname2",
            "check_status": "L7OK",
            "check_duration": 1,
        },
    }

    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "fakehost", "attributes": {"fake_discover": "fakelocation"}}
        ]

        mock_get_backends.return_value = haproxy_backends_by_task.values()
        mock_match_backends_and_tasks.return_value = [
            (haproxy_backends_by_task[good_task], good_task),
            (haproxy_backends_by_task[bad_task], None),
            (None, other_task),
        ]
        tasks = [good_task, other_task]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=len(haproxy_backends_by_task),
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        mock_get_backends.assert_called_once_with(
            "servicename.fake_nerve_ns",
            synapse_host="fakehost",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fakelocation" in actual
        assert "Healthy" in actual


def test_status_smartstack_backends_different_nerve_ns():
    service = "servicename"
    instance = "instancename"
    different_ns = "different_ns"
    service_instance = compose_job_id(service, different_ns)

    cluster = "fake_cluster"
    good_task = mock.Mock()
    bad_task = mock.Mock()
    other_task = mock.Mock()
    haproxy_backends_by_task = {
        good_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress1:1001_hostname1",
            "check_status": "L7OK",
            "check_duration": 1,
        },
        bad_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress2:1002_hostname2",
            "check_status": "L7OK",
            "check_duration": 1,
        },
    }

    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "fakehost", "attributes": {"fake_discover": "fakelocation"}}
        ]

        mock_get_backends.return_value = haproxy_backends_by_task.values()
        mock_match_backends_and_tasks.return_value = [
            (haproxy_backends_by_task[good_task], good_task),
            (haproxy_backends_by_task[bad_task], None),
            (None, other_task),
        ]

        tasks = [good_task, other_task]
        with mock.patch.object(
            fake_marathon_job_config,
            "get_registrations",
            return_value=[compose_job_id(service, different_ns)],
        ):
            actual = marathon_serviceinit.status_smartstack_backends(
                service=service,
                instance=instance,
                cluster=cluster,
                job_config=fake_marathon_job_config,
                service_namespace_config=fake_service_namespace_config,
                tasks=tasks,
                expected_count=len(haproxy_backends_by_task),
                soa_dir=None,
                verbose=False,
                synapse_port=123456,
                synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
                system_deploy_blacklist=[],
                system_deploy_whitelist=[],
            )
        mock_get_backends.assert_called_once_with(
            service_instance,
            synapse_host="fakehost",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fakelocation" in actual
        assert "Healthy" in actual


def test_status_smartstack_backends_no_smartstack_replication_info():
    service = "servicename"
    instance = "instancename"

    cluster = "fake_cluster"
    tasks = mock.Mock()
    normal_count = 10
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_all_slaves_for_blacklist_whitelist.return_value = {}
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        assert "servicename.fake_nerve_ns is NOT in smartstack" in actual


def test_status_smartstack_backends_multiple_locations():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {
        "status": "UP",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1001_hostname1",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [(fake_backend, good_task)]
        tasks = [good_task, other_task]
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "fakehost", "attributes": {"fake_discover": "fakelocation"}},
            {
                "hostname": "fakeotherhost",
                "attributes": {"fake_discover": "fakeotherlocation"},
            },
        ]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=len(mock_get_backends.return_value),
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="fakehost",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="fakeotherhost",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fakelocation - %s" % PaastaColors.green("Healthy") in actual
        assert "fakeotherlocation - %s" % PaastaColors.green("Healthy") in actual


def test_status_smartstack_backends_multiple_locations_expected_count():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"
    normal_count = 10

    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {
        "status": "UP",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1001_hostname1",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks, mock.patch(
        "paasta_tools.marathon_serviceinit.haproxy_backend_report", autospec=True
    ) as mock_haproxy_backend_report:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [(fake_backend, good_task)]
        tasks = [good_task, other_task]
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "hostname1", "attributes": {"fake_discover": "fakelocation"}},
            {"hostname": "hostname2", "attributes": {"fake_discover": "fakelocation2"}},
        ]
        marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="hostname1",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="hostname2",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        expected_count_per_location = int(normal_count / 2)
        mock_haproxy_backend_report.assert_any_call(expected_count_per_location, 1)


def test_status_smartstack_backends_verbose_multiple_apps():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"

    good_task = mock.Mock()
    bad_task = mock.Mock()
    other_task = mock.Mock()
    haproxy_backends_by_task = {
        good_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress1:1001_hostname1",
            "check_status": "L7OK",
            "check_duration": 1,
        },
        bad_task: {
            "status": "UP",
            "lastchg": "1",
            "last_chk": "OK",
            "check_code": "200",
            "svname": "ipaddress2:1002_hostname2",
            "check_status": "L7OK",
            "check_duration": 1,
        },
    }

    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_backends.return_value = haproxy_backends_by_task.values()
        mock_match_backends_and_tasks.return_value = [
            (haproxy_backends_by_task[good_task], good_task),
            (haproxy_backends_by_task[bad_task], None),
            (None, other_task),
        ]
        tasks = [good_task, other_task]
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "hostname1", "attributes": {"fake_discover": "fakelocation"}}
        ]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=len(haproxy_backends_by_task),
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        mock_get_backends.assert_called_once_with(
            "servicename.fake_nerve_ns",
            synapse_host="hostname1",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fakelocation" in actual
        assert "hostname1:1001" in actual
        assert re.search(
            r"%s[^\n]*hostname2:1002" % re.escape(PaastaColors.GREY), actual
        )


def test_status_smartstack_backends_verbose_multiple_locations():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {
        "status": "UP",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1001_hostname1",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    fake_other_backend = {
        "status": "UP",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1002_hostname2",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends",
        autospec=True,
        side_effect=[[fake_backend], [fake_other_backend]],
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks",
        autospec=True,
        side_effect=[[(fake_backend, good_task)], [(fake_other_backend, good_task)]],
    ):
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "hostname1", "attributes": {"fake_discover": "fakelocation"}},
            {
                "hostname": "hostname2",
                "attributes": {"fake_discover": "fakeotherlocation"},
            },
        ]
        tasks = [good_task, other_task]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=1,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="hostname1",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            "servicename.fake_nerve_ns",
            synapse_host="hostname2",
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_all_slaves_for_blacklist_whitelist.assert_called_once_with(
            blacklist=[], whitelist=[]
        )
        assert "fakelocation - %s" % PaastaColors.green("Healthy") in actual
        assert "hostname1:1001" in actual
        assert "fakeotherlocation - %s" % PaastaColors.green("Healthy") in actual
        assert "hostname2:1002" in actual


def test_status_smartstack_backends_verbose_emphasizes_maint_instances():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"
    normal_count = 10
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {
        "status": "MAINT",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1001_hostname1",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_mesos_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        mock_get_mesos_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "fake", "attributes": {"fake_discover": "fake_location_1"}}
        ]
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"

        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [(fake_backend, good_task)]
        tasks = [good_task, other_task]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        assert PaastaColors.red("MAINT") in actual


def test_status_smartstack_backends_verbose_demphasizes_maint_instances_for_unrelated_tasks():
    service = "servicename"
    instance = "instancename"
    cluster = "fake_cluster"
    normal_count = 10
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {
        "status": "MAINT",
        "lastchg": "1",
        "last_chk": "OK",
        "check_code": "200",
        "svname": "ipaddress1:1001_hostname1",
        "check_status": "L7OK",
        "check_duration": 1,
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_all_slaves_for_blacklist_whitelist",
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends", autospec=True
    ) as mock_get_backends, mock.patch(
        "paasta_tools.marathon_serviceinit.match_backends_and_tasks", autospec=True
    ) as mock_match_backends_and_tasks:
        mock_get_all_slaves_for_blacklist_whitelist.return_value = [
            {"hostname": "fake", "attributes": {"fake_discover": "fake_location_1"}}
        ]
        fake_service_namespace_config = mock.Mock()
        fake_service_namespace_config.get_discover.return_value = "fake_discover"
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [(fake_backend, None)]
        tasks = [good_task, other_task]
        actual = marathon_serviceinit.status_smartstack_backends(
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            service_namespace_config=fake_service_namespace_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            system_deploy_blacklist=[],
            system_deploy_whitelist=[],
        )
        assert PaastaColors.red("MAINT") not in actual
        assert re.search(
            r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.GREY), actual
        )


def test_haproxy_backend_report_healthy():
    normal_count = 10
    actual_count = 11
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Healthy" in status


def test_haproxy_backend_report_critical():
    normal_count = 10
    actual_count = 1
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Critical" in status


def test_get_short_task_id():
    task_id = "service.instance.githash.confighash.uuid"
    assert marathon_serviceinit.get_short_task_id(task_id) == "uuid"


def test_status_mesos_tasks_working():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
    ) as mock_tasks:
        mock_tasks.return_value = [
            {"id": "unused{0}unused{0}".format(marathon_tools.MESOS_TASK_SPACER)}
            for _ in range(2)
        ]
        normal_count = 2
        actual = marathon_serviceinit.status_mesos_tasks(
            "unused", "unused", normal_count, verbose=0
        )
        assert "Healthy" in actual


def test_status_mesos_tasks_warning():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
    ) as mock_tasks:
        mock_tasks.return_value = [
            {"id": "fake{0}fake{0}".format(marathon_tools.MESOS_TASK_SPACER)}
            for _ in range(2)
        ]
        normal_count = 4
        actual = marathon_serviceinit.status_mesos_tasks(
            "fake", "fake", normal_count, verbose=0
        )
        assert "Warning" in actual


def test_status_mesos_tasks_critical():
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
    ) as mock_tasks:
        mock_tasks.return_value = []
        normal_count = 10
        actual = marathon_serviceinit.status_mesos_tasks(
            "unused", "unused", normal_count, verbose=0
        )
        assert "Critical" in actual


def test_perform_command_handles_no_docker_and_doesnt_raise():
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_cluster = "fake_cluster"
    soa_dir = "fake_soa_dir"
    with mock.patch.object(
        fake_marathon_job_config,
        "format_marathon_app_dict",
        autospec=True,
        side_effect=NoDockerImageError,
    ), mock.patch(
        "paasta_tools.marathon_serviceinit.load_system_paasta_config",
        autospec=True,
        return_value=SystemPaastaConfig({}, "/fake/config"),
    ):
        actual = marathon_serviceinit.perform_command(
            "start",
            fake_service,
            fake_instance,
            fake_cluster,
            False,
            soa_dir,
            job_config=fake_marathon_job_config,
            clients=mock.Mock(),
        )
        assert actual == 1


def test_pretty_print_smartstack_backends_for_locations_verbose():
    hosts_grouped_by_location = {
        "place1": ["host1"],
        "place2": ["host2"],
        "place3": ["host3"],
    }
    host_ip_mapping = {
        "host1": "169.254.123.1",
        "host2": "169.254.123.2",
        "host3": "169.254.123.3",
    }
    tasks = [
        mock.Mock(host="host1", ports=[1234]),
        mock.Mock(host="host2", ports=[1234]),
        mock.Mock(host="host3", ports=[1234]),
    ]
    backends = {
        "host1": {
            "svname": "169.254.123.1:1234_host1",
            "status": "UP",
            "check_status": "L7OK",
            "check_code": "200",
            "check_duration": 4,
            "lastchg": 0,
        },
        "host2": {
            "svname": "169.254.123.2:1234_host2",
            "status": "UP",
            "check_status": "L7OK",
            "check_code": "200",
            "check_duration": 4,
            "lastchg": 0,
        },
        "host3": {
            "svname": "169.254.123.3:1234_host3",
            "status": "UP",
            "check_status": "L7OK",
            "check_code": "200",
            "check_duration": 4,
            "lastchg": 0,
        },
    }
    with mock.patch(
        "paasta_tools.marathon_serviceinit.get_backends",
        autospec=True,
        side_effect=lambda _, synapse_host, synapse_port, synapse_haproxy_url_format: [
            backends[synapse_host]
        ],
    ), mock.patch(
        "socket.gethostbyname",
        side_effect=lambda name: host_ip_mapping[name],
        autospec=True,
    ):
        actual = marathon_serviceinit.pretty_print_smartstack_backends_for_locations(
            registration="fake_service.fake_instance",
            tasks=tasks,
            locations=hosts_grouped_by_location,
            expected_count=3,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )

        colorstripped_actual = [remove_ansi_escape_sequences(l) for l in actual]
        assert colorstripped_actual == [
            "      Name        LastCheck        LastChange  Status",
            "    place1 - Healthy - in haproxy with (1/1) total backends UP in this namespace.",
            "      host1:1234  L7OK/200 in 4ms  now         UP",
            "    place2 - Healthy - in haproxy with (1/1) total backends UP in this namespace.",
            "      host2:1234  L7OK/200 in 4ms  now         UP",
            "    place3 - Healthy - in haproxy with (1/1) total backends UP in this namespace.",
            "      host3:1234  L7OK/200 in 4ms  now         UP",
        ]


def test_get_marathon_dashboard_links():
    system_paasta_config = SystemPaastaConfig(
        config={
            "cluster": "fake_cluster",
            "dashboard_links": {
                "fake_cluster": {
                    "Marathon RO": [
                        "http://marathon",
                        "http://marathon1",
                        "http://marathon2",
                    ]
                }
            },
        },
        directory="/fake/config",
    )

    marathon_clients = mock.Mock(current=["client", "client1", "client2"])
    assert marathon_serviceinit.get_marathon_dashboard_links(
        marathon_clients, system_paasta_config
    ) == {
        "client": "http://marathon",
        "client1": "http://marathon1",
        "client2": "http://marathon2",
    }

    marathon_clients = mock.Mock(current=["client", "client1", "client2", "client3"])
    assert (
        marathon_serviceinit.get_marathon_dashboard_links(
            marathon_clients, system_paasta_config
        )
        is None
    )

    marathon_clients = mock.Mock(current=["client", "client1"])
    assert marathon_serviceinit.get_marathon_dashboard_links(
        marathon_clients, system_paasta_config
    ) == {"client": "http://marathon", "client1": "http://marathon1"}


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
