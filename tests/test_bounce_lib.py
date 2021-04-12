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
from unittest import mock

import marathon
import pytz
from requests.exceptions import ConnectionError
from requests.exceptions import RequestException

from paasta_tools import bounce_lib
from paasta_tools import utils


class TestBounceLib:
    def fake_system_paasta_config(self):
        return utils.SystemPaastaConfig({"synapse_port": 123456}, "/fake/configs")

    def test_bounce_lock(self):
        import fcntl

        lock_name = "the_internet"
        lock_file = "/var/lock/%s.lock" % lock_name
        fake_fd = mock.mock_open()
        with mock.patch("builtins.open", fake_fd, autospec=None) as open_patch:
            with mock.patch("fcntl.lockf", autospec=None) as lockf_patch:
                with mock.patch("os.remove", autospec=None) as remove_patch:
                    with bounce_lib.bounce_lock(lock_name):
                        pass
        open_patch.assert_called_once_with(lock_file, "w")
        lockf_patch.assert_called_once_with(
            fake_fd.return_value, fcntl.LOCK_EX | fcntl.LOCK_NB
        )
        fake_fd.return_value.__exit__.assert_called_once_with(None, None, None)
        remove_patch.assert_called_once_with(lock_file)

    def test_bounce_lock_zookeeper(self):
        lock_name = "watermelon"
        fake_lock = mock.Mock()
        fake_zk = mock.MagicMock(Lock=mock.Mock(return_value=fake_lock))
        fake_zk_hosts = "awjti42ior"
        with mock.patch(
            "paasta_tools.bounce_lib.KazooClient", return_value=fake_zk, autospec=True
        ) as client_patch, mock.patch(
            "paasta_tools.bounce_lib.load_system_paasta_config",
            return_value=mock.Mock(get_zk_hosts=lambda: fake_zk_hosts),
            autospec=True,
        ) as hosts_patch:
            with bounce_lib.bounce_lock_zookeeper(lock_name):
                pass
            hosts_patch.assert_called_once_with()
            client_patch.assert_called_once_with(
                hosts=fake_zk_hosts, timeout=bounce_lib.ZK_LOCK_CONNECT_TIMEOUT_S
            )
            fake_zk.start.assert_called_once_with()
            fake_zk.Lock.assert_called_once_with(
                f"{bounce_lib.ZK_LOCK_PATH}/{lock_name}"
            )
            fake_lock.acquire.assert_called_once_with(timeout=1)
            fake_lock.release.assert_called_once_with()
            fake_zk.stop.assert_called_once_with()

    def test_create_marathon_app(self):
        marathon_client_mock = mock.create_autospec(marathon.MarathonClient)
        fake_client = marathon_client_mock
        fake_config = {"id": "fake_creation"}
        with mock.patch(
            "paasta_tools.bounce_lib.wait_for_create", autospec=True
        ) as wait_patch:
            with mock.patch("time.sleep", autospec=True):
                bounce_lib.create_marathon_app(
                    "fake_creation", fake_config, fake_client
                )
            assert fake_client.create_app.call_count == 1
            actual_call_args = fake_client.create_app.call_args
            actual_config = actual_call_args[0][1]
            assert actual_config.id == "fake_creation"
            wait_patch.assert_called_once_with(fake_config["id"], fake_client)

    def test_delete_marathon_app(self):
        fake_client = mock.Mock(delete_app=mock.Mock())
        fake_id = "fake_deletion"
        with mock.patch(
            "paasta_tools.bounce_lib.wait_for_delete", autospec=True
        ) as wait_patch, mock.patch("time.sleep", autospec=True):
            bounce_lib.delete_marathon_app(fake_id, fake_client)
            fake_client.scale_app.assert_called_once_with(
                fake_id, instances=0, force=True
            )
            fake_client.delete_app.assert_called_once_with(fake_id, force=True)
            wait_patch.assert_called_once_with(fake_id, fake_client)

    def test_kill_old_ids(self):
        old_ids = ["mmm.whatcha.say", "that.you", "only.meant.well"]
        fake_client = mock.MagicMock()
        with mock.patch(
            "paasta_tools.bounce_lib.delete_marathon_app", autospec=True
        ) as delete_patch:
            bounce_lib.kill_old_ids(old_ids, fake_client)
            for old_id in old_ids:
                delete_patch.assert_any_call(old_id, fake_client)
            assert delete_patch.call_count == len(old_ids)

    def test_wait_for_create_slow(self):
        fake_id = "my_created"
        fake_client = mock.Mock(spec="paasta_tools.setup_marathon_job.MarathonClient")
        fake_is_app_running_values = iter([False, False, True])
        with mock.patch(
            "paasta_tools.marathon_tools.is_app_id_running", autospec=True
        ) as is_app_id_running_patch, mock.patch(
            "time.sleep", autospec=True
        ) as sleep_patch:
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_create(fake_id, fake_client)
        assert sleep_patch.call_count == 2
        assert is_app_id_running_patch.call_count == 3

    def test_wait_for_create_fast(self):
        fake_id = "my_created"
        fake_client = mock.Mock(spec="paasta_tools.setup_marathon_job.MarathonClient")
        fake_is_app_running_values = iter([True])
        with mock.patch(
            "paasta_tools.marathon_tools.is_app_id_running", autospec=True
        ) as is_app_id_running_patch, mock.patch(
            "time.sleep", autospec=True
        ) as sleep_patch:
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_create(fake_id, fake_client)
        assert sleep_patch.call_count == 0
        assert is_app_id_running_patch.call_count == 1

    def test_wait_for_delete_slow(self):
        fake_id = "my_deleted"
        fake_client = mock.Mock(spec="paasta_tools.setup_marathon_job.MarathonClient")
        fake_is_app_running_values = iter([True, True, False])
        with mock.patch(
            "paasta_tools.marathon_tools.is_app_id_running", autospec=True
        ) as is_app_id_running_patch, mock.patch(
            "time.sleep", autospec=True
        ) as sleep_patch:
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_delete(fake_id, fake_client)
        assert sleep_patch.call_count == 2
        assert is_app_id_running_patch.call_count == 3

    def test_wait_for_delete_fast(self):
        fake_id = "my_deleted"
        fake_client = mock.Mock(spec="paasta_tools.setup_marathon_job.MarathonClient")
        fake_is_app_running_values = iter([False])
        with mock.patch(
            "paasta_tools.marathon_tools.is_app_id_running", autospec=True
        ) as is_app_id_running_patch, mock.patch(
            "time.sleep", autospec=True
        ) as sleep_patch:
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_delete(fake_id, fake_client)
        assert sleep_patch.call_count == 0
        assert is_app_id_running_patch.call_count == 1

    def test_get_bounce_method_func(self):
        actual = bounce_lib.get_bounce_method_func("brutal")
        expected = bounce_lib.brutal_bounce
        assert actual == expected

    def test_filter_tasks_in_smartstack(self):
        service = "foo"
        nerve_ns = "bar"
        fake_task = mock.Mock(name="fake_task", host="foo", ports=[123456])
        fake_backend = {"svname": "foo_256.256.256.256:123456", "status": "UP"}

        with mock.patch(
            "paasta_tools.smartstack_tools.get_multiple_backends",
            autospec=True,
            return_value=[fake_backend],
        ):
            with mock.patch(
                "socket.gethostbyname", autospec=True, return_value="256.256.256.256"
            ):
                assert [fake_task] == bounce_lib.filter_tasks_in_smartstack(
                    [fake_task], service, nerve_ns, self.fake_system_paasta_config()
                )

        with mock.patch(
            "paasta_tools.smartstack_tools.get_multiple_backends",
            autospec=True,
            return_value=[],
        ):
            with mock.patch(
                "socket.gethostbyname", autospec=True, return_value="256.256.256.256"
            ):
                assert [] == bounce_lib.filter_tasks_in_smartstack(
                    [fake_task], service, nerve_ns, self.fake_system_paasta_config()
                )

        with mock.patch(
            "paasta_tools.bounce_lib.get_registered_marathon_tasks",
            autospec=True,
            side_effect=[[fake_task], [ConnectionError], [RequestException]],
        ):
            assert [fake_task] == bounce_lib.filter_tasks_in_smartstack(
                [fake_task], service, nerve_ns, self.fake_system_paasta_config()
            )
            assert [] == bounce_lib.filter_tasks_in_smartstack(
                [fake_task], service, nerve_ns, self.fake_system_paasta_config()
            )
            assert [] == bounce_lib.filter_tasks_in_smartstack(
                [fake_task], service, nerve_ns, self.fake_system_paasta_config()
            )

    def test_get_happy_tasks_when_running_without_healthchecks_defined(self):
        """All running tasks with no health checks results are healthy if the app does not define healthchecks"""
        tasks = [mock.Mock(health_check_results=[]) for _ in range(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert (
            bounce_lib.get_happy_tasks(
                fake_app, "service", "namespace", self.fake_system_paasta_config()
            )
            == tasks
        )

    def test_get_happy_tasks_when_running_with_healthchecks_defined(self):
        """All running tasks with no health check results are unhealthy if the app defines healthchecks"""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        tasks = [
            mock.Mock(
                health_check_results=[],
                started_at=(now - datetime.timedelta(minutes=i)),
            )
            for i in range(5)
        ]
        fake_app = mock.Mock(
            tasks=tasks,
            health_checks=[mock.Mock(grace_period_seconds=1234, interval_seconds=4321)],
        )
        with mock.patch(
            "paasta_tools.marathon_tools.datetime.datetime",
            now=lambda x: now,
            autospec=True,
        ):
            assert (
                bounce_lib.get_happy_tasks(
                    fake_app, "service", "namespace", self.fake_system_paasta_config()
                )
                == []
            )

    def test_get_happy_tasks_when_some_old_and_unknown(self):
        """Only tasks with a passing healthcheck should be happy"""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        fake_successful_healthcheck_results = [mock.Mock(alive=True)]
        tasks = [
            mock.Mock(
                health_check_results=[], started_at=(now - datetime.timedelta(days=20))
            ),
            mock.Mock(health_check_results=fake_successful_healthcheck_results),
            mock.Mock(health_check_results=fake_successful_healthcheck_results),
        ]
        fake_app = mock.Mock(
            tasks=tasks,
            health_checks=[mock.Mock(grace_period_seconds=1234, interval_seconds=4321)],
        )
        with mock.patch(
            "paasta_tools.marathon_tools.datetime.datetime",
            now=lambda x: now,
            autospec=True,
        ):
            actual = bounce_lib.get_happy_tasks(
                fake_app, "service", "namespace", self.fake_system_paasta_config()
            )
        expected = tasks
        assert actual == expected

    def test_get_happy_tasks_when_all_healthy(self):
        """All tasks with only passing healthchecks should be happy"""
        tasks = [
            mock.Mock(health_check_results=[mock.Mock(alive=True)]) for _ in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert (
            bounce_lib.get_happy_tasks(
                fake_app, "service", "namespace", self.fake_system_paasta_config()
            )
            == tasks
        )

    def test_get_happy_tasks_when_some_unhealthy(self):
        """Only tasks with a passing healthcheck should be happy"""
        fake_failing_healthcheck_results = [mock.Mock(alive=False)]
        fake_successful_healthcheck_results = [mock.Mock(alive=True)]
        tasks = [
            mock.Mock(health_check_results=fake_failing_healthcheck_results),
            mock.Mock(health_check_results=fake_failing_healthcheck_results),
            mock.Mock(health_check_results=fake_successful_healthcheck_results),
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        actual = bounce_lib.get_happy_tasks(
            fake_app, "service", "namespace", self.fake_system_paasta_config()
        )
        expected = tasks[-1:]
        assert actual == expected

    def test_get_happy_tasks_with_multiple_healthchecks_success(self):
        """All tasks with at least one passing healthcheck should be happy"""
        fake_successful_healthcheck_results = [
            mock.Mock(alive=True),
            mock.Mock(alive=False),
        ]
        tasks = [mock.Mock(health_check_results=fake_successful_healthcheck_results)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert (
            bounce_lib.get_happy_tasks(
                fake_app, "service", "namespace", self.fake_system_paasta_config()
            )
            == tasks
        )

    def test_get_happy_tasks_with_multiple_healthchecks_fail(self):
        """Only tasks with at least one passing healthcheck should be happy"""
        fake_successful_healthcheck_results = [
            mock.Mock(alive=False),
            mock.Mock(alive=False),
        ]
        tasks = [mock.Mock(health_check_results=fake_successful_healthcheck_results)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert (
            bounce_lib.get_happy_tasks(
                fake_app, "service", "namespace", self.fake_system_paasta_config()
            )
            == []
        )

    def test_get_happy_tasks_min_task_uptime(self):
        """If we specify a minimum task age, tasks newer than that should not be considered happy."""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        tasks = [
            mock.Mock(
                health_check_results=[],
                started_at=(now - datetime.timedelta(minutes=i)),
            )
            for i in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])

        # I would have just mocked datetime.datetime.utcnow, but that's apparently difficult; I have to mock
        # datetime.datetime instead, and give it a utcnow attribute.
        with mock.patch(
            "paasta_tools.bounce_lib.datetime.datetime",
            now=lambda tz: now,
            autospec=True,
        ):
            actual = bounce_lib.get_happy_tasks(
                fake_app,
                "service",
                "namespace",
                self.fake_system_paasta_config(),
                min_task_uptime=121,
            )
            expected = tasks[3:]
            assert actual == expected

    def test_get_happy_tasks_min_task_uptime_when_unhealthy(self):
        """If we specify a minimum task age, tasks newer than that should not be considered happy."""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        tasks = [
            mock.Mock(
                health_check_results=[mock.Mock(alive=False)],
                started_at=(now - datetime.timedelta(minutes=i)),
            )
            for i in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])

        with mock.patch(
            "paasta_tools.bounce_lib.datetime.datetime",
            now=lambda tz: now,
            autospec=True,
        ):
            actual = bounce_lib.get_happy_tasks(
                fake_app,
                "service",
                "namespace",
                self.fake_system_paasta_config(),
                min_task_uptime=121,
            )
            expected = []
            assert actual == expected

    def test_get_happy_tasks_check_haproxy(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [
            mock.Mock(health_check_results=[mock.Mock(alive=True)]) for i in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with mock.patch(
            "paasta_tools.bounce_lib.get_registered_marathon_tasks",
            return_value=tasks[2:],
            autospec=True,
        ):
            actual = bounce_lib.get_happy_tasks(
                fake_app,
                "service",
                "namespace",
                self.fake_system_paasta_config(),
                check_haproxy=True,
            )
            expected = tasks[2:]
            assert actual == expected

    def test_get_happy_tasks_check_haproxy_when_unhealthy(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [
            mock.Mock(health_check_results=[mock.Mock(alive=False)]) for i in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with mock.patch(
            "paasta_tools.bounce_lib.get_registered_marathon_tasks",
            return_value=tasks[2:],
            autospec=True,
        ):
            actual = bounce_lib.get_happy_tasks(
                fake_app,
                "service",
                "namespace",
                self.fake_system_paasta_config(),
                check_haproxy=True,
            )
            expected = []
            assert actual == expected

    def test_get_happy_tasks_check_each_host(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [
            mock.Mock(health_check_results=[mock.Mock(alive=True)], host="fake_host1")
            for i in range(5)
        ]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with mock.patch(
            "paasta_tools.bounce_lib.get_registered_marathon_tasks",
            side_effect=[tasks[2:]],
            autospec=True,
        ) as get_registered_marathon_tasks_patch:
            actual = bounce_lib.get_happy_tasks(
                fake_app,
                "service",
                "namespace",
                self.fake_system_paasta_config(),
                check_haproxy=True,
            )
            expected = tasks[2:]
            assert actual == expected

            get_registered_marathon_tasks_patch.assert_called_once_with(
                "fake_host1",
                123456,
                utils.DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
                "service.namespace",
                tasks,
            )

    def test_filter_tasks_in_smartstack_only_calls_n_hosts(self):
        tasks = [
            mock.Mock(
                health_check_results=[mock.Mock(alive=True)], host=f"fake_host{i}"
            )
            for i in range(5)
        ]
        with mock.patch(
            "paasta_tools.bounce_lib.get_registered_marathon_tasks",
            return_value=tasks,
            autospec=True,
        ) as get_registered_marathon_tasks_patch:
            actual = bounce_lib.filter_tasks_in_smartstack(
                tasks,
                service="service",
                nerve_ns="nerve_ns",
                system_paasta_config=self.fake_system_paasta_config(),
                max_hosts_to_query=3,
            )
            assert actual == tasks
            assert get_registered_marathon_tasks_patch.call_count == 3

    def test_flatten_tasks(self):
        """Simple check of flatten_tasks."""
        all_tasks = [mock.Mock(task_id="id_%d" % i) for i in range(10)]

        expected = set(all_tasks)
        actual = bounce_lib.flatten_tasks(
            {"app_id_1": set(all_tasks[:5]), "app_id_2": set(all_tasks[5:])}
        )

        assert actual == expected


class TestBrutalBounce:
    def test_brutal_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, brutal bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345"}
        happy_tasks = []

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_brutal_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, brutal bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_brutal_bounce_mid_bounce(self):
        """When marathon has the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_brutal_bounce_old_but_no_new(self):
        """When marathon does not have the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones and start
        the new one."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }


class TestUpthendownBounce:
    def test_upthendown_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, upthendown bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345"}
        happy_tasks = []

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_upthendown_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, upthendown bounce should start the new one. but
        not stop the old one yet."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_upthendown_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of
        the service running, but the new app is not fully up, upthendown bounce
        should not stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_upthendown_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, and the new app is fully up, upthendown bounce
        should stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_upthendown_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, upthendown bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}


class TestCrossoverBounce:
    def test_crossover_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, crossover bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_crossover_bounce_old_but_no_new(self):
        """When marathon only has old apps for this service, crossover bounce should start the new one, but not kill any
        old tasks yet."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_crossover_bounce_old_app_is_happy_but_no_new_app_happy_tasks(self):
        """When marathon only has old apps for this service and margin_factor != 1,
        crossover bounce should start the new app and kill some old tasks."""

        new_config = {"id": "foo.bar.12345", "instances": 100}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(100)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.95,
        )
        assert actual["create_app"] is True
        assert len(actual["tasks_to_drain"]) == 5

    def test_crossover_bounce_some_unhappy_old_some_happy_old_no_new(self):
        """When marathon only has old apps for this service, and some of them are unhappy (maybe they've been recently
        started), the crossover bounce should start a new app and prefer killing the unhappy tasks over the happy ones.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set(old_app_live_unhappy_tasks)}

    def test_crossover_bounce_some_unhappy_old_no_happy_old_no_new_tasks_no_excess(
        self,
    ):
        """When marathon only has old apps for this service, and all of their tasks are unhappy, and there are no excess
        tasks, the crossover bounce should start a new app and not kill any old tasks.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_crossover_bounce_lots_of_unhappy_old_no_happy_old_no_new(self):
        """When marathon has a new app and multiple old apps, no new tasks are up, all old tasks are unhappy, and there
        are too many tasks running, the crossover bounce should kill some (but not all) of the old
        tasks.

        This represents a situation where
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(10)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 5

    def test_crossover_bounce_lots_of_unhappy_old_some_happy_old_new_app_exists_no_new_tasks(
        self,
    ):
        """When marathon has a new app and multiple old apps, no new tasks are up, one of the old apps is healthy and
        the other is not, only unhealthy tasks should get killed.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert actual["tasks_to_drain"] == set(old_app_live_unhappy_tasks)
        # Since there are plenty of unhappy old tasks, we should not kill any new ones.
        assert len(actual["tasks_to_drain"] & set(old_app_live_happy_tasks)) == 0

    def test_crossover_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of the service running, but the new app is not
        fully up, crossover bounce should only stop a few of the old instances."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 3

    def test_crossover_bounce_mid_bounce_some_happy_old_some_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, and some of those
        older tasks are unhappy, we should prefer killing the unhappy tasks."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(1)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 4
        # There are fewer unhappy old tasks than excess tasks, so we should kill all unhappy old ones, plus a few
        # happy ones.
        assert set(old_app_live_unhappy_tasks).issubset(actual["tasks_to_drain"])

    def test_crossover_bounce_mid_bounce_some_happy_old_lots_of_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, and there are more
        unhappy old tasks than excess tasks, we should only kill unhappy tasks.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(2)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        # There are as many unhappy old tasks as excess tasks, so all tasks that we kill should be old unhappy ones.
        assert len(actual["tasks_to_drain"]) == 5
        assert actual["tasks_to_drain"] == set(old_app_live_unhappy_tasks)

    def test_crossover_bounce_mid_bounce_no_happy_old_lots_of_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, but none of the old
        tasks are happy, and there are excess tasks, we should kill some (but not all) unhappy old tasks."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(6)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 4

    def test_crossover_bounce_using_margin_factor_big_numbers(self):
        new_config = {"id": "foo.bar.12345", "instances": 500}
        happy_tasks = [mock.Mock() for _ in range(100)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(300)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(100)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.95,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 25

    def test_crossover_bounce_using_margin_factor_small_numbers(self):
        new_config = {"id": "foo.bar.12345", "instances": 3}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.66,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 1

    def test_crossover_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, crossover bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}


class TestDownThenUpBounce:
    def test_downthenup_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, downthenup bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_downthenup_bounce_old_but_no_new(self):
        """When marathon has only old copies of the service, downthenup_bounce should kill them and not start a new one
        yet."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(1)]

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_downthenup_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of the service running, downthenup bounce
        should neither start nor stop anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": False, "tasks_to_drain": set()}


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
