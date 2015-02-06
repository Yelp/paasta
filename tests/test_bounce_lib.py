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

    def test_get_bounce_method_func(self):
        actual = bounce_lib.get_bounce_method_func('brutal')
        expected = bounce_lib.brutal_bounce
        assert actual == expected

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
