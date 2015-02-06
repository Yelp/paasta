import bounce_lib
import contextlib
import mock
import pytest
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
            mock.patch('bounce_lib.KazooClient', return_value=fake_zk),
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

    def test_brutal_bounce(self):
        old_ids = ["bbounce", "the_best_bounce_method"]
        new_config = {"now_featuring": "no_gracefuls", "guaranteed": "or_your_money_back",
                      'id': 'sockem.fun'}
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('bounce_lib.bounce_lock_zookeeper', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
        ) as (
            lock_patch,
            create_app_patch,
            kill_patch
        ):
            bounce_lib.brutal_bounce('fake_service', 'fake_instance', old_ids, new_config, fake_client)
            lock_patch.assert_called_once_with('fake_service.fake_instance')
            create_app_patch.assert_called_once_with(new_config['id'], new_config, fake_client)
            kill_patch.assert_called_once_with(old_ids, fake_client)

    def test_upthendown_bounce(self):
        old_ids = ["oldapp1", "oldapp2"]
        new_config = {"now_featuring": "no_gracefuls", "guaranteed": "or_your_money_back",
                      'id': 'sockem.fun'}
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('bounce_lib.bounce_lock_zookeeper', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.create_marathon_app', autospec=True),
            mock.patch('bounce_lib.kill_old_ids', autospec=True),
            mock.patch('bounce_lib.time.sleep', autospec=True),
        ) as (
            lock_patch,
            create_app_patch,
            kill_patch,
            sleep_patch,
        ):
            bounce_lib.upthendown_bounce('fake_service', 'fake_instance', old_ids, new_config, fake_client)
            lock_patch.assert_called_once_with('fake_service.fake_instance')
            create_app_patch.assert_called_once_with(new_config['id'], new_config, fake_client)
            kill_patch.assert_called_once_with(old_ids, fake_client)
            sleep_patch.assert_called_once_with(120)

    def test_scale_apps_delta_valid(self):
        fake_scalable = [('poker.face', 10), ('roker.race', 5)]
        fake_delta = 14
        fake_client = mock.MagicMock()
        with mock.patch('bounce_lib.delete_marathon_app', autospec=True) as delete_marathon_app_patch:
            assert bounce_lib.scale_apps(fake_scalable, fake_delta, fake_client) == 14
            delete_marathon_app_patch.assert_called_once_with('roker.race', fake_client)
            fake_client.scale_app.assert_called_once_with('poker.face', delta=-9)

    def test_scale_apps_delta_invalid(self):
        fake_scalable = [('muh.mah.muh.mah', 9999), ('uh.huh.huh.uh.huh', 7777)]
        fake_delta = -9
        fake_client = mock.MagicMock()
        with mock.patch('bounce_lib.delete_marathon_app', autospec=True) as delete_marathon_app_patch:
            assert bounce_lib.scale_apps(fake_scalable, fake_delta, fake_client) == 0
            assert delete_marathon_app_patch.call_count == 0

    def test_crossover_bounce_exact_delta(self):
        fake_new_config = {
            'id': 'shake.bake',
            'instances': 10
        }
        fake_namespace = 'wake'
        fake_service_config = {
            'nerve_ns': fake_namespace,
        }
        fake_old_ids = ['fake.make', 'lake.quake']
        fake_old_instance_counts = [mock.Mock(instances=4), mock.Mock(instances=15)]
        fake_client = mock.MagicMock(
                        get_app=mock.Mock(side_effect=lambda a: fake_old_instance_counts.pop()))
        haproxy_instance_count = [{'shake.wake': 18}, {'shake.wake': 9}]
        with contextlib.nested(
            mock.patch('marathon_tools.read_service_config', return_value=fake_service_config),
            mock.patch('bounce_lib.get_replication_for_services',
                       side_effect=lambda a, b: haproxy_instance_count.pop()),
            mock.patch('bounce_lib.bounce_lock_zookeeper', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.create_marathon_app'),
            mock.patch('bounce_lib.time_limit', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.scale_apps', return_value=9),
            mock.patch('bounce_lib.kill_old_ids'),
            mock.patch('time.sleep'),
        ) as (
            read_service_config_patch,
            replication_patch,
            lock_patch,
            create_app_patch,
            time_limit_patch,
            scale_patch,
            kill_patch,
            sleep_patch
        ):
            bounce_lib.crossover_bounce('shake', 'bake', fake_old_ids, fake_new_config, fake_client)
            replication_patch.assert_any_call(bounce_lib.DEFAULT_SYNAPSE_HOST, ['shake.wake'])
            assert replication_patch.call_count == 2
            lock_patch.assert_called_once_with('shake.wake')
            create_app_patch.assert_called_once_with(fake_new_config['id'], fake_new_config, fake_client)
            time_limit_patch.assert_called_once_with(bounce_lib.CROSSOVER_MAX_TIME_M)
            sleep_patch.assert_any_call(bounce_lib.CROSSOVER_SLEEP_INTERVAL_S)
            fake_client.get_app.assert_any_call('fake.make')
            fake_client.get_app.assert_any_call('lake.quake')
            assert fake_client.get_app.call_count == 2
            scale_patch.assert_called_once_with([('fake.make', 15), ('lake.quake', 4)], 9, fake_client)
            assert fake_client.scale_app.call_count == 0
            kill_patch.assert_called_once_with(fake_old_ids, fake_client)

    def test_crossover_bounce_apps_scaled(self):
        fake_new_config = {
            'id': 'hello.everyone',
            'instances': 100
        }
        fake_namespace = 'world'
        fake_service_config = {
            'nerve_ns': fake_namespace,
        }
        fake_old_ids = ['pen.hen', 'tool.rule']
        fake_old_instance_counts = [mock.Mock(instances=7), mock.Mock(instances=19)]
        fake_client = mock.MagicMock(
                        get_app=mock.Mock(side_effect=lambda a: fake_old_instance_counts.pop()))
        haproxy_instance_count = [{'hello.world': 16}, {'hello.world': 10}, {'hello.world': 5}]
        with contextlib.nested(
            mock.patch('marathon_tools.read_service_config', return_value=fake_service_config),
            mock.patch('bounce_lib.get_replication_for_services',
                       side_effect=lambda a, b: haproxy_instance_count.pop()),
            mock.patch('bounce_lib.bounce_lock_zookeeper', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.create_marathon_app'),
            mock.patch('bounce_lib.time_limit', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.scale_apps',
                       side_effect=lambda apps, b, c: apps.pop()[1]),
            mock.patch('bounce_lib.kill_old_ids'),
            mock.patch('time.sleep'),
        ) as (
            read_service_config_patch,
            replication_patch,
            lock_patch,
            create_app_patch,
            time_limit_patch,
            scale_patch,
            kill_patch,
            sleep_patch
        ):
            bounce_lib.crossover_bounce('hello', 'everyone', fake_old_ids, fake_new_config, fake_client)
            replication_patch.assert_any_call(bounce_lib.DEFAULT_SYNAPSE_HOST, ['hello.world'])
            assert replication_patch.call_count == 3
            lock_patch.assert_called_once_with('hello.world')
            create_app_patch.assert_called_once_with(fake_new_config['id'], fake_new_config, fake_client)
            time_limit_patch.assert_called_once_with(bounce_lib.CROSSOVER_MAX_TIME_M)
            sleep_patch.assert_any_call(bounce_lib.CROSSOVER_SLEEP_INTERVAL_S)
            fake_client.get_app.assert_any_call('pen.hen')
            fake_client.get_app.assert_any_call('tool.rule')
            assert fake_client.get_app.call_count == 2
            scale_patch.assert_any_call([], 5, fake_client)
            scale_patch.assert_any_call([], 11, fake_client)
            assert scale_patch.call_count == 2
            kill_patch.assert_called_once_with(fake_old_ids, fake_client)

    def test_crossover_bounce_immediate_timeout(self):
        fake_new_config = {
            'id': 'the.hustle',
            'instances': 6
        }
        fake_namespace = 'electricslide'
        fake_service_config = {
            'nerve_ns': fake_namespace,
        }
        fake_old_ids = ['70s.disco', '80s.funk']
        fake_old_instance_counts = [mock.Mock(instances=77), mock.Mock(instances=55)]
        fake_client = mock.MagicMock(
                        get_app=mock.Mock(side_effect=lambda a: fake_old_instance_counts.pop()))
        haproxy_instance_count = {'the.electricslide': 10}

        def raiser(a):
            raise bounce_lib.TimeoutException

        with contextlib.nested(
            mock.patch('marathon_tools.read_service_config', return_value=fake_service_config),
            mock.patch('bounce_lib.get_replication_for_services', return_value=haproxy_instance_count),
            mock.patch('bounce_lib.bounce_lock_zookeeper', spec=contextlib.contextmanager),
            mock.patch('bounce_lib.create_marathon_app'),
            mock.patch('bounce_lib.time_limit', spec=contextlib.contextmanager,
                       side_effect=raiser),
            mock.patch('bounce_lib.scale_apps', return_value=9),
            mock.patch('bounce_lib.kill_old_ids'),
            mock.patch('time.sleep'),
        ) as (
            read_service_config_patch,
            replication_patch,
            lock_patch,
            create_app_patch,
            time_limit_patch,
            scale_patch,
            kill_patch,
            sleep_patch
        ):
            with pytest.raises(bounce_lib.TimeoutException):
                bounce_lib.crossover_bounce('the', 'hustle', fake_old_ids, fake_new_config, fake_client)
            replication_patch.assert_called_once_with(bounce_lib.DEFAULT_SYNAPSE_HOST, ['the.electricslide'])
            lock_patch.assert_called_once_with('the.electricslide')
            assert create_app_patch.call_count == 0
            time_limit_patch.assert_called_once_with(bounce_lib.CROSSOVER_MAX_TIME_M)
            assert sleep_patch.call_count == 0
            fake_client.get_app.assert_any_call('70s.disco')
            fake_client.get_app.assert_any_call('80s.funk')
            assert fake_client.get_app.call_count == 2
            assert scale_patch.call_count == 0
            kill_patch.assert_called_once_with(fake_old_ids, fake_client)

    def test_get_bounce_method_func(self):
        actual = bounce_lib.get_bounce_method_func('brutal')
        expected = bounce_lib.brutal_bounce
        assert actual == expected

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
