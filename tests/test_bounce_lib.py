import bounce_lib
import contextlib
import mock
import marathon


class TestBounceLib:

    def test_bounce_lock(self):
        import fcntl
        lock_name = 'the_internet'
        lock_file = '/var/lock/%s.lock' % lock_name
        fake_fd = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('bounce_lib.open', create=True, return_value=fake_fd),
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
            mock.patch('bounce_lib.KazooClient', return_value=fake_zk, autospec=True),
            mock.patch('marathon_tools.get_zk_hosts', return_value=fake_zk_hosts),
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
            mock.patch('bounce_lib.create_app_lock', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.wait_for_create'),
        ) as (
            lock_patch,
            wait_patch,
        ):
            bounce_lib.create_marathon_app('fake_creation', fake_config, fake_client)
            lock_patch.assert_called_once_with()
            assert fake_client.create_app.call_count == 1
            actual_call_args = fake_client.create_app.call_args
            actual_config = actual_call_args[0][1]
            assert actual_config.id == 'fake_creation'
            wait_patch.assert_called_once_with(fake_config['id'], fake_client)

    def test_delete_marathon_app(self):
        fake_client = mock.Mock(delete_app=mock.Mock())
        fake_id = 'fake_deletion'
        with contextlib.nested(
            mock.patch('bounce_lib.create_app_lock', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.wait_for_delete'),
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
            lock_patch.assert_called_once_with()

    def test_kill_old_ids(self):
        old_ids = ['mmm.whatcha.say', 'that.you', 'only.meant.well']
        fake_client = mock.MagicMock()
        with mock.patch('bounce_lib.delete_marathon_app') as delete_patch:
            bounce_lib.kill_old_ids(old_ids, fake_client)
            for old_id in old_ids:
                delete_patch.assert_any_call(old_id, fake_client)
            assert delete_patch.call_count == len(old_ids)

    def test_wait_for_create_slow(self):
        fake_id = 'my_created'
        fake_client = mock.Mock(spec='paasta_tools.setup_marathon_job.MarathonClient')
        fake_is_app_running_values = [False, False, True]
        with contextlib.nested(
            mock.patch('marathon_tools.is_app_id_running'),
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
            mock.patch('marathon_tools.is_app_id_running'),
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
            mock.patch('marathon_tools.is_app_id_running'),
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
            mock.patch('marathon_tools.is_app_id_running'),
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


class TestBrutalBounce:
    def test_brutal_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, brutal bounce should try to
        create a marathon app."""
        new_config = {'id': 'foo.bar.12345'}
        client = mock.Mock()

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.brutal_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            kill_old_ids_patch.assert_called_once_with(set(), client)

    def test_brutal_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, brutal bounce should neither start nor stop
        anything."""

        new_config = {'id': 'foo.bar.12345'}
        client = mock.Mock()
        app = mock.Mock(id='foo.bar.12345')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.brutal_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(set(), client)

    def test_brutal_bounce_mid_bounce(self):
        """When marathon has the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones."""

        new_config = {'id': 'foo.bar.12345'}
        client = mock.Mock()
        new_app = mock.Mock(id='foo.bar.12345')
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.brutal_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[new_app, old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(
                set([old_app.id]),
                client
            )

    def test_brutal_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones and start
        the new one."""

        new_config = {'id': 'foo.bar.12345'}
        client = mock.Mock()
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.brutal_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            kill_old_ids_patch.assert_called_once_with(
                set([old_app.id]),
                client
            )


class TestUpthendownBounce:
    def test_upthendown_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, upthendown bounce should try to
        create a marathon app."""
        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.upthendown_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            assert kill_old_ids_patch.call_count == 0

    def test_upthendown_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, upthendown bounce should start the new one. but
        not stop the old one yet."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.upthendown_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            assert kill_old_ids_patch.call_count == 0

    def test_upthendown_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of
        the service running, but the new app is not fully up, upthendown bounce
        should not stop the old ones."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        new_app = mock.Mock(id='foo.bar.12345', tasks_running=5)
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.upthendown_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[new_app, old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            assert kill_old_ids_patch.call_count == 0

    def test_upthendown_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, and the new app is fully up, upthendown bounce
        should stop the old ones."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        new_app = mock.Mock(id='foo.bar.12345', tasks_running=10)
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.upthendown_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[new_app, old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(
                set([old_app.id]),
                client
            )

    def test_upthendown_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, upthendown bounce should neither start nor stop
        anything."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        app = mock.Mock(id='foo.bar.12345')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.upthendown_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(set(), client)


class TestCrossoverBounce:
    def test_crossover_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, crossover bounce should try to
        create a marathon app."""
        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.get_registered_marathon_tasks',
                       side_effect=lambda _, __, x: x.tasks)
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
            get_registered_marathon_tasks_patch,
        ):
            bounce_lib.crossover_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            assert kill_old_ids_patch.call_count == 0

    def test_crossover_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, crossover bounce should stop the old ones and start
        the new one."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.get_registered_marathon_tasks',
                       side_effect=lambda _, __, x: x.tasks)
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
            get_registered_marathon_tasks_patch,
        ):
            bounce_lib.crossover_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            assert kill_old_ids_patch.call_count == 0
            assert client.kill_task.call_count == 0

    def test_crossover_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of
        the service running, but the new app is not fully up, crossover bounce
        should not stop the old ones."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()

        new_app_tasks = [mock.Mock() for _ in xrange(5)]
        old_app_tasks = [mock.Mock() for _ in xrange(10)]

        new_app = mock.Mock(
            id='foo.bar.12345',
            tasks_running=5,
            tasks=new_app_tasks,
        )
        old_app = mock.Mock(
            id='foo.bar.11111',
            tasks_running=10,
            tasks=old_app_tasks,
        )

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.get_registered_marathon_tasks',
                       side_effect=lambda _, __, x: x.tasks),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
            get_registered_marathon_tasks_patch,
        ):
            bounce_lib.crossover_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[new_app, old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(set([]), client)
            assert client.kill_task.call_count == 5

    def test_crossover_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, which have no remaining tasks, those apps should
        be killed."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        new_app = mock.Mock(id='foo.bar.12345', tasks_running=10,
                            tasks=[mock.Mock() for _ in xrange(10)])
        old_app = mock.Mock(id='foo.bar.11111', tasks_running=0, tasks=[])

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.get_registered_marathon_tasks',
                       side_effect=lambda _, __, x: x.tasks)
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
            get_registered_marathon_tasks_patch,
        ):
            bounce_lib.crossover_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[new_app, old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(
                set([old_app.id]),
                client
            )
            assert client.kill_task.call_count == 0

    def test_crossover_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, crossover bounce should neither start nor stop
        anything."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        app = mock.Mock(
            id='foo.bar.12345',
            tasks=[mock.Mock() for _ in xrange(10)]
        )

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.get_registered_marathon_tasks',
                       side_effect=lambda _, __, x: x.tasks)
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
            get_registered_marathon_tasks_patch,
        ):
            bounce_lib.crossover_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(set([]), client)
            assert client.kill_task.call_count == 0


class TestDownThenUpBounce(object):
    def test_downthenup_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, downthenup bounce should try to
        create a marathon app."""
        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.downthenup_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            create_marathon_app_patch.assert_called_once_with(
                new_config['id'],
                new_config,
                client
            )

            kill_old_ids_patch.assert_called_once_with(set(), client)

    def test_downthenup_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, downthenup bounce should stop the old ones and start
        the new one."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        old_app = mock.Mock(id='foo.bar.11111')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.downthenup_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[old_app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0

            kill_old_ids_patch.assert_called_once_with(
                set([old_app.id]),
                client
            )

    def test_downthenup_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, downthenup bounce should neither start nor stop
        anything."""

        new_config = {
            'id': 'foo.bar.12345',
            'instances': 10,
        }
        client = mock.Mock()
        app = mock.Mock(id='foo.bar.12345')

        with contextlib.nested(
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            create_marathon_app_patch,
            kill_old_ids_patch,
        ):
            bounce_lib.downthenup_bounce(
                service_name='foo',
                instance_name='bar',
                existing_apps=[app],
                new_config=new_config,
                client=client,
                nerve_ns='bar',
            )

            assert create_marathon_app_patch.call_count == 0
            kill_old_ids_patch.assert_called_once_with(set(), client)

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
