import contextlib
import os
import shutil
import stat
import tempfile

import json
import mock

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


def test_load_system_paasta_config():
    expected = utils.SystemPaastaConfig({'foo': 'bar'})
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('paasta_tools.utils.open', create=True, return_value=file_mock),
        mock.patch('paasta_tools.utils.json.load', autospec=True, return_value=expected)
    ) as (
        open_file_patch,
        json_patch
    ):
        actual = utils.load_system_paasta_config()
        assert actual == expected
        # Kinda weird but without this load_system_paasta_config() can (and
        # did! during development) return a plain dict without the test
        # complaining.
        assert actual.__class__ == expected.__class__
        open_file_patch.assert_called_once_with('/etc/paasta_tools/paasta.json')
        json_patch.assert_called_once_with(file_mock.__enter__())


def test_load_system_paasta_config_file_dne():
    fake_path = '/var/dir_of_fake'
    with contextlib.nested(
        mock.patch('paasta_tools.utils.open', create=True, side_effect=IOError(2, 'a', 'b')),
    ) as (
        open_patch,
    ):
        with raises(utils.PaastaNotConfigured) as excinfo:
            utils.load_system_paasta_config(fake_path)
        assert str(excinfo.value) == "Could not load system paasta config file b: a"


def test_SystemPaastaConfig_get_cluster():
    fake_config = utils.SystemPaastaConfig({
        'cluster': 'peanut',
    })
    expected = 'peanut'
    actual = fake_config.get_cluster()
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_dne():
    fake_config = utils.SystemPaastaConfig()
    with raises(utils.NoMarathonClusterFoundException):
        fake_config.get_cluster()


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


@mock.patch('os.listdir', autospec=True)
def test_list_all_clusters(mock_os_listdir):
    mock_os_listdir.return_value = ['cluster1.yaml', 'cluster2.yaml']
    expected = set(['cluster1', 'cluster2'])
    actual = utils.list_all_clusters()
    assert actual == expected


@mock.patch('paasta_tools.utils.parse_yaml_file', autospec=True)
def test_get_infrastructure_zookeeper_servers(mock_parse_yaml_file):
    mock_parse_yaml_file.return_value = [
        ['1.2.3.4', 22181],
        ['5.6.7.8', 22181],
    ]
    actual = utils.get_infrastructure_zookeeper_servers('test-cluster')
    expected = ['1.2.3.4', '5.6.7.8']
    assert actual == expected
    mock_parse_yaml_file.assert_called_once_with('/nail/etc/zookeeper_discovery/infrastructure/test-cluster.yaml')


def test_color_text():
    expected = "%shi%s" % (utils.PaastaColors.RED, utils.PaastaColors.DEFAULT)
    actual = utils.PaastaColors.color_text(utils.PaastaColors.RED, "hi")
    assert actual == expected


def test_color_text_nested():
    expected = "%sred%sblue%sred%s" % (
        utils.PaastaColors.RED,
        utils.PaastaColors.BLUE,
        utils.PaastaColors.DEFAULT + utils.PaastaColors.RED,
        utils.PaastaColors.DEFAULT,
    )
    actual = utils.PaastaColors.color_text(utils.PaastaColors.RED, "red%sred" % utils.PaastaColors.blue("blue"))
    assert actual == expected
