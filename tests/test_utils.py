import json
import mock
import os
import shutil
import stat
import tempfile

from paasta_tools import utils
from pytest import raises


def test_get_git_url():
    service = 'giiiiiiiiiiit'
    expected = 'git@git.yelpcorp.com:services/%s.git' % service
    assert utils.get_git_url(service) == expected


def test_format_log_line():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_now = 'fake_now'
    expected = json.dumps({
        'timestamp': fake_now,
        'level': fake_level,
        'cluster': fake_cluster,
        'instance': fake_instance,
        'component': fake_component,
        'message': input_line,
    }, sort_keys=True)
    with mock.patch('paasta_tools.utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_log_line(fake_level, fake_cluster, fake_instance, fake_component, input_line)
        assert actual == expected


def test_format_log_line_rejects_invalid_components():
    with raises(utils.NoSuchLogComponent):
        utils.format_log_line('fake_service', 'fake_line', 'BOGUS_COMPONENT', 'debug', 'fake_input')


def test_log_raise_on_unknown_level():
    with raises(utils.NoSuchLogLevel):
        utils._log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')


def test_get_log_name_for_service():
    service_name = 'foo'
    expected = 'stream_paasta_%s' % service_name
    assert utils.get_log_name_for_service(service_name) == expected


def test_atomic_file_write():
    with mock.patch('tempfile.NamedTemporaryFile', autospec=True) as ntf_patch:
        file_patch = ntf_patch().__enter__()
        file_patch.name = '/hurp/.durp-AAA'
        ntf_patch.reset_mock()

        with mock.patch('os.rename', autospec=True) as rename_patch:
            with mock.patch('os.chmod', autospec=True) as chmod_patch:
                with utils.atomic_file_write('/hurp/durp'):
                    ntf_patch.assert_called_once_with(
                        dir='/hurp',
                        prefix='.durp-',
                        delete=False,
                    )
                chmod_patch.assert_called_once_with('/hurp/.durp-AAA', 0644)

            rename_patch.assert_called_once_with(
                '/hurp/.durp-AAA',
                '/hurp/durp'
            )


def test_atomic_file_write_itest():
    tempdir = tempfile.mkdtemp()
    target_file_name = os.path.join(tempdir, 'test_atomic_file_write_itest.txt')

    try:
        old_umask = os.umask(0022)
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

        file_stat = os.stat(target_file_name)
        assert stat.S_ISREG(file_stat.st_mode)
        assert stat.S_IMODE(file_stat.st_mode) == 0644

    finally:
        os.umask(old_umask)
        shutil.rmtree(tempdir)


def test_configure_log():
    utils.configure_log()


def test_build_docker_tag():
    upstream_job_name = 'fake_upstream_job_name'
    upstream_git_commit = 'fake_upstream_git_commit'
    expected = 'docker-paasta.yelpcorp.com:443/services-%s:paasta-%s' % (
        upstream_job_name,
        upstream_git_commit,
    )
    actual = utils.build_docker_tag(upstream_job_name, upstream_git_commit)
    assert actual == expected


def test_check_docker_image_false():
    fake_app = 'fake_app'
    fake_commit = 'fake_commit'
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch('docker.Client') as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [{
            'Created': 1425430339,
            'VirtualSize': 250344331,
            'ParentId': '1111',
            'RepoTags': [docker_tag],
            'Id': 'ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2',
            'Size': 0}
        ]
        assert utils.check_docker_image('test_service', 'tag2') is False


def test_check_docker_image_true():
    fake_app = 'fake_app'
    fake_commit = 'fake_commit'
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch('docker.Client') as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [{
            'Created': 1425430339,
            'VirtualSize': 250344331,
            'ParentId': '1111',
            'RepoTags': [docker_tag],
            'Id': 'ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2',
            'Size': 0}
        ]
        assert utils.check_docker_image(fake_app, fake_commit) is True


def test_remove_ansi_escape_sequences():
    plain_string = 'blackandwhite'
    colored_string = '\033[34m' + plain_string + '\033[0m'
    assert utils.remove_ansi_escape_sequences(colored_string) == plain_string
