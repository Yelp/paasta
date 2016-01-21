# Copyright 2015 Yelp Inc.
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
import datetime

import marathon
import mock

from paasta_tools import bounce_lib
from paasta_tools.smartstack_tools import DEFAULT_SYNAPSE_PORT


class TestBounceLib:

    def test_bounce_lock(self):
        import fcntl
        lock_name = 'the_internet'
        lock_file = '/var/lock/%s.lock' % lock_name
        fake_fd = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.open', create=True, return_value=fake_fd),
            mock.patch('fcntl.lockf'),
            mock.patch('os.remove')
        ) as (
            open_patch,
            lockf_patch,
            remove_patch
        ):
            with bounce_lib.bounce_lock(lock_name):
                pass
            open_patch.assert_called_once_with(lock_file, 'w')
            lockf_patch.assert_called_once_with(fake_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fake_fd.close.assert_called_once_with()
            remove_patch.assert_called_once_with(lock_file)

    def test_bounce_lock_zookeeper(self):
        lock_name = 'watermelon'
        fake_lock = mock.Mock()
        fake_zk = mock.MagicMock(Lock=mock.Mock(return_value=fake_lock))
        fake_zk_hosts = 'awjti42ior'
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.KazooClient', return_value=fake_zk, autospec=True),
            mock.patch(
                'paasta_tools.bounce_lib.load_system_paasta_config',
                return_value=mock.Mock(
                    get_zk_hosts=lambda: fake_zk_hosts
                ),
                autospec=True,
            ),
        ) as (
            client_patch,
            hosts_patch,
        ):
            with bounce_lib.bounce_lock_zookeeper(lock_name):
                pass
            hosts_patch.assert_called_once_with()
            client_patch.assert_called_once_with(hosts=fake_zk_hosts,
                                                 timeout=bounce_lib.ZK_LOCK_CONNECT_TIMEOUT_S)
            fake_zk.start.assert_called_once_with()
            fake_zk.Lock.assert_called_once_with('%s/%s' % (bounce_lib.ZK_LOCK_PATH, lock_name))
            fake_lock.acquire.assert_called_once_with(timeout=1)
            fake_lock.release.assert_called_once_with()
            fake_zk.stop.assert_called_once_with()

    def test_create_marathon_app(self):
        marathon_client_mock = mock.create_autospec(marathon.MarathonClient)
        fake_client = marathon_client_mock
        fake_config = {'id': 'fake_creation'}
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.create_app_lock', spec=contextlib.contextmanager),
            mock.patch('paasta_tools.bounce_lib.wait_for_create'),
        ) as (
            lock_patch,
            wait_patch,
        ):
            bounce_lib.create_marathon_app('fake_creation', fake_config, fake_client)
            assert lock_patch.called
            assert fake_client.create_app.call_count == 1
            actual_call_args = fake_client.create_app.call_args
            actual_config = actual_call_args[0][1]
            assert actual_config.id == 'fake_creation'
            wait_patch.assert_called_once_with(fake_config['id'], fake_client)

    def test_delete_marathon_app(self):
        fake_client = mock.Mock(delete_app=mock.Mock())
        fake_id = 'fake_deletion'
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.create_app_lock', spec=contextlib.contextmanager),
            mock.patch('paasta_tools.bounce_lib.wait_for_delete'),
            mock.patch('time.sleep')
        ) as (
            lock_patch,
            wait_patch,
            sleep_patch
        ):
            bounce_lib.delete_marathon_app(fake_id, fake_client)
            fake_client.scale_app.assert_called_once_with(fake_id, instances=0, force=True)
            fake_client.delete_app.assert_called_once_with(fake_id, force=True)
            sleep_patch.assert_called_once_with(1)
            wait_patch.assert_called_once_with(fake_id, fake_client)
            assert lock_patch.called

    def test_kill_old_ids(self):
        old_ids = ['mmm.whatcha.say', 'that.you', 'only.meant.well']
        fake_client = mock.MagicMock()
        with mock.patch('paasta_tools.bounce_lib.delete_marathon_app') as delete_patch:
            bounce_lib.kill_old_ids(old_ids, fake_client)
            for old_id in old_ids:
                delete_patch.assert_any_call(old_id, fake_client)
            assert delete_patch.call_count == len(old_ids)

    def test_wait_for_create_slow(self):
        fake_id = 'my_created'
        fake_client = mock.Mock(spec='paasta_tools.setup_marathon_job.MarathonClient')
        fake_is_app_running_values = [False, False, True]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running'),
            mock.patch('time.sleep'),
        ) as (
            is_app_id_running_patch,
            sleep_patch,
        ):
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_create(fake_id, fake_client)
        assert sleep_patch.call_count == 2
        assert is_app_id_running_patch.call_count == 3

    def test_wait_for_create_fast(self):
        fake_id = 'my_created'
        fake_client = mock.Mock(spec='paasta_tools.setup_marathon_job.MarathonClient')
        fake_is_app_running_values = [True]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running'),
            mock.patch('time.sleep'),
        ) as (
            is_app_id_running_patch,
            sleep_patch,
        ):
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_create(fake_id, fake_client)
        assert sleep_patch.call_count == 0
        assert is_app_id_running_patch.call_count == 1

    def test_wait_for_delete_slow(self):
        fake_id = 'my_deleted'
        fake_client = mock.Mock(spec='paasta_tools.setup_marathon_job.MarathonClient')
        fake_is_app_running_values = [True, True, False]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running'),
            mock.patch('time.sleep'),
        ) as (
            is_app_id_running_patch,
            sleep_patch,
        ):
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_delete(fake_id, fake_client)
        assert sleep_patch.call_count == 2
        assert is_app_id_running_patch.call_count == 3

    def test_wait_for_delete_fast(self):
        fake_id = 'my_deleted'
        fake_client = mock.Mock(spec='paasta_tools.setup_marathon_job.MarathonClient')
        fake_is_app_running_values = [False]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running'),
            mock.patch('time.sleep'),
        ) as (
            is_app_id_running_patch,
            sleep_patch,
        ):
            is_app_id_running_patch.side_effect = fake_is_app_running_values
            bounce_lib.wait_for_delete(fake_id, fake_client)
        assert sleep_patch.call_count == 0
        assert is_app_id_running_patch.call_count == 1

    def test_get_bounce_method_func(self):
        actual = bounce_lib.get_bounce_method_func('brutal')
        expected = bounce_lib.brutal_bounce
        assert actual == expected

    def test_get_happy_tasks_when_running_without_healthchecks_defined(self):
        """All running tasks with no health checks results are healthy if the app does not define healthchecks"""
        tasks = [mock.Mock(health_check_results=[]) for _ in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == tasks

    def test_get_happy_tasks_when_running_with_healthchecks_defined(self):
        """All running tasks with no health check results are unhealthy if the app defines healthchecks"""
        tasks = [mock.Mock(health_check_results=[]) for _ in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=["fake_healthcheck_definition"])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == []

    def test_get_happy_tasks_when_all_healthy(self):
        """All tasks with only passing healthchecks should be happy"""
        tasks = [mock.Mock(health_check_results=[mock.Mock(alive=True)]) for _ in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == tasks

    def test_get_happy_tasks_when_some_unhealthy(self):
        """Only tasks with a passing healthcheck should be happy"""
        fake_failing_healthcheck_results = [mock.Mock(alive=False)]
        fake_successful_healthcheck_results = [mock.Mock(alive=True)]
        tasks = [mock.Mock(health_check_results=fake_failing_healthcheck_results),
                 mock.Mock(health_check_results=fake_failing_healthcheck_results),
                 mock.Mock(health_check_results=fake_successful_healthcheck_results)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == tasks[-1:]

    def test_get_happy_tasks_with_multiple_healthchecks_success(self):
        """All tasks with at least one passing healthcheck should be happy"""
        fake_successful_healthcheck_results = [mock.Mock(alive=True), mock.Mock(alive=False)]
        tasks = [mock.Mock(health_check_results=fake_successful_healthcheck_results)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == tasks

    def test_get_happy_tasks_with_multiple_healthchecks_fail(self):
        """Only tasks with at least one passing healthcheck should be happy"""
        fake_successful_healthcheck_results = [mock.Mock(alive=False), mock.Mock(alive=False)]
        tasks = [mock.Mock(health_check_results=fake_successful_healthcheck_results)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace') == []

    def test_get_happy_tasks_min_task_uptime(self):
        """If we specify a minimum task age, tasks newer than that should not be considered happy."""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0)
        tasks = [mock.Mock(health_check_results=[], started_at=(now - datetime.timedelta(minutes=i)))
                 for i in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])

        # I would have just mocked datetime.datetime.utcnow, but that's apparently difficult; I have to mock
        # datetime.datetime instead, and give it a utcnow attribute.
        with mock.patch('paasta_tools.bounce_lib.datetime.datetime', utcnow=lambda: now, autospec=True):
            assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace', min_task_uptime=121) == tasks[3:]

    def test_get_happy_tasks_min_task_uptime_when_unhealthy(self):
        """If we specify a minimum task age, tasks newer than that should not be considered happy."""
        now = datetime.datetime(2000, 1, 1, 0, 0, 0)
        tasks = [mock.Mock(health_check_results=[mock.Mock(alive=False)],
                           started_at=(now - datetime.timedelta(minutes=i)))
                 for i in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])

        with mock.patch('paasta_tools.bounce_lib.datetime.datetime', utcnow=lambda: now, autospec=True):
            assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace', min_task_uptime=121) == []

    def test_get_happy_tasks_check_haproxy(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [mock.Mock(health_check_results=[mock.Mock(alive=True)]) for i in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.get_registered_marathon_tasks', return_value=tasks[2:], autospec=True),
            mock.patch('paasta_tools.mesos_tools.get_mesos_slaves_grouped_by_attribute',
                       return_value={'fake_region': ['fake_host']}, autospec=True),
        ) as (
            _,
            get_mesos_slaves_grouped_by_attribute_patch,
        ):
            assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace', check_haproxy=True) == tasks[2:]

    def test_get_happy_tasks_check_haproxy_when_unhealthy(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [mock.Mock(health_check_results=[mock.Mock(alive=False)]) for i in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with contextlib.nested(
            mock.patch('paasta_tools.bounce_lib.get_registered_marathon_tasks', return_value=tasks[2:], autospec=True),
            mock.patch('paasta_tools.mesos_tools.get_mesos_slaves_grouped_by_attribute',
                       return_value={'fake_region': ['fake_host']}, autospec=True),
        ) as (
            _,
            get_mesos_slaves_grouped_by_attribute_patch,
        ):
            assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace', check_haproxy=True) == []

    def test_get_happy_tasks_check_haproxy_multiple_locations(self):
        """If we specify that a task should be in haproxy, don't call it happy unless it's in haproxy."""

        tasks = [mock.Mock(health_check_results=[mock.Mock(alive=True)]) for i in xrange(5)]
        fake_app = mock.Mock(tasks=tasks, health_checks=[])
        with contextlib.nested(
            mock.patch(
                'paasta_tools.bounce_lib.get_registered_marathon_tasks',
                side_effect=[tasks[2:3], tasks[3:]], autospec=True,
            ),
            mock.patch('paasta_tools.mesos_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
        ) as (
            get_registered_marathon_tasks_patch,
            get_mesos_slaves_grouped_by_attribute_patch,
        ):
            get_mesos_slaves_grouped_by_attribute_patch.return_value = {
                'fake_region': ['fake_host1'],
                'fake_other_region': ['fake_host2'],
            }
            assert bounce_lib.get_happy_tasks(fake_app, 'service', 'namespace', check_haproxy=True) == tasks[2:]
            get_registered_marathon_tasks_patch.assert_any_call(
                'fake_host1',
                DEFAULT_SYNAPSE_PORT,
                'service.namespace',
                tasks,
            )
            get_registered_marathon_tasks_patch.assert_any_call(
                'fake_host2',
                DEFAULT_SYNAPSE_PORT,
                'service.namespace',
                tasks,
            )


class TestBrutalBounce:

    def test_brutal_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, brutal bounce should try to
        create a marathon app."""
        new_config = {'id': 'foo.bar.12345'}
        happy_tasks = []

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks={},
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_brutal_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, brutal bounce should neither start nor stop
        anything."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks={},
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }

    def test_brutal_bounce_mid_bounce(self):
        """When marathon has the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": old_app_live_tasks['app1'] | old_app_live_tasks['app2'],
        }

    def test_brutal_bounce_old_but_no_new(self):
        """When marathon does not have the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones and start
        the new one."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": old_app_live_tasks['app1'] | old_app_live_tasks['app2'],
        }


class TestUpthendownBounce:

    def test_upthendown_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, upthendown bounce should try to
        create a marathon app."""
        new_config = {'id': 'foo.bar.12345'}
        happy_tasks = []

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks={},
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_upthendown_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, upthendown bounce should start the new one. but
        not stop the old one yet."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_upthendown_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of
        the service running, but the new app is not fully up, upthendown bounce
        should not stop the old ones."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(3)]
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }

    def test_upthendown_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, and the new app is fully up, upthendown bounce
        should stop the old ones."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": old_app_live_tasks['app1'] | old_app_live_tasks['app2'],
        }

    def test_upthendown_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, upthendown bounce should neither start nor stop
        anything."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {}

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }


class TestCrossoverBounce:

    def test_crossover_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, crossover bounce should try to
        create a marathon app."""
        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = []
        old_app_live_tasks = {}

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_crossover_bounce_old_but_no_new(self):
        """When marathon only has old apps for this service, crossover bounce should start the new one, but not kill any
        old tasks yet."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = []
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_crossover_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of the service running, but the new app is not
        fully up, crossover bounce should only stop a few of the old instances."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(3)]
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        )

        assert actual['create_app'] is False
        assert len(actual['tasks_to_drain']) == 3

    def test_crossover_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, which have no remaining tasks, those apps should
        be killed."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {
            'app1': set(),
            'app2': set(),
        }

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }

    def test_crossover_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, crossover bounce should neither start nor stop
        anything."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {}

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }


class TestDownThenUpBounce(object):

    def test_downthenup_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, downthenup bounce should try to
        create a marathon app."""
        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = []
        old_app_live_tasks = {}

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(),
        }

    def test_downthenup_bounce_old_but_no_new(self):
        """When marathon has only old copies of the service, downthenup_bounce should kill them and not start a new one
        yet."""
        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = []
        old_app_live_tasks = {
            'app1': set(mock.Mock() for _ in xrange(3)),
            'app2': set(mock.Mock() for _ in xrange(2)),
        }

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": old_app_live_tasks['app1'] | old_app_live_tasks['app2'],
        }

    def test_downthenup_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of the service running, downthenup bounce
        should neither start nor stop anything."""

        new_config = {'id': 'foo.bar.12345', 'instances': 5}
        happy_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_live_tasks = {}

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_app_live_tasks=old_app_live_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(),
        }

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
