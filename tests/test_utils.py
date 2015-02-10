import json
import mock
import os
import shutil
import tempfile

from paasta_tools import utils


def test_get_git_url():
    service = 'giiiiiiiiiiit'
    expected = 'git@git.yelpcorp.com:services/%s.git' % service
    assert utils.get_git_url(service) == expected


def test_format_log_line():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_instance = 'fake_instance'
    fake_component = 'fake_component',
    fake_now = 'fake_now'
    expected = json.dumps({
        'timestamp': fake_now,
        'cluster': fake_cluster,
        'instance': fake_instance,
        'component': fake_component,
        'message': input_line,
    }, sort_keys=True)
    with mock.patch('paasta_tools.utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        assert utils.format_log_line(fake_cluster, fake_instance, fake_component, input_line) == expected


def test_get_log_name_for_service():
    service_name = 'foo'
    expected = 'stream_paasta_%s' % service_name
    assert utils.get_log_name_for_service(service_name) == expected


def test_atomic_file_write():
    with mock.patch('tempfile.NamedTemporaryFile', autospec=True) as NTF_patch:
        file_patch = NTF_patch().__enter__()
        file_patch.name = '/hurp/.durp-AAA'
        NTF_patch.reset_mock()

        with mock.patch('os.rename', autospec=True) as rename_patch:
            with utils.atomic_file_write('/hurp/durp'):
                NTF_patch.assert_called_once_with(
                    dir='/hurp',
                    prefix='.durp-',
                    delete=False,
                )

            rename_patch.assert_called_once_with(
                '/hurp/.durp-AAA',
                '/hurp/durp'
            )


def test_atomic_file_write_itest():
    tempdir = tempfile.mkdtemp()
    target_file_name = os.path.join(tempdir, 'test_atomic_file_write_itest.txt')

    try:
        with open(target_file_name, 'w') as f_before:
            f_before.write('old content')

        with utils.atomic_file_write(target_file_name) as f_new:
            f_new.write('new content')

            with open(target_file_name) as f_existing:
                # While in the middle of an atomic_file_write, the existing
                # file should still contain the old content, and should not
                # be truncated, etc.
                assert f_existing.read() == 'old content'

        with open(target_file_name) as f_done:
            # once we're done, the content should be in place.
            assert f_done.read() == 'new content'

    finally:
        shutil.rmtree(tempdir)
