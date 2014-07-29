import bounce_lib
import contextlib
import mock


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

    def test_brutal_bounce(self):
        old_ids = ["bbounce", "the_best_bounce_method"]
        new_config = {"now_featuring": "no_gracefuls", "guaranteed": "or_your_money_back",
                      'id': 'none.fun'}
        fake_client = mock.MagicMock(delete_app=mock.Mock(), create_app=mock.Mock())
        from contextlib import contextmanager
        with mock.patch('bounce_lib.bounce_lock', spec=contextmanager) as lock_patch:
            bounce_lib.brutal_bounce(old_ids, new_config, fake_client)
            lock_patch.assert_called_once_with('none.fun')
            for oid in old_ids:
                fake_client.delete_app.assert_any_call(oid)
            assert fake_client.delete_app.call_count == len(old_ids)
            fake_client.create_app.assert_called_once_with(**new_config)