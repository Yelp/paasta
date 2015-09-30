import contextlib
import os
import shutil
import stat
import tempfile

import json
import mock

import utils
from pytest import raises


def test_get_git_url_provided_by_serviceyaml():
    service = 'giiiiiiiiiiit'
    expected = 'git@some_random_host:foobar'
    with (
        mock.patch('service_configuration_lib.read_service_configuration', autospec=True)
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {'git_url': expected}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(service, soa_dir=utils.DEFAULT_SOA_DIR)


def test_get_git_url_default():
    service = 'giiiiiiiiiiit'
    expected = 'git@git.yelpcorp.com:services/%s.git' % service
    with (
        mock.patch('service_configuration_lib.read_service_configuration', autospec=True)
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(service, soa_dir=utils.DEFAULT_SOA_DIR)


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
    with mock.patch('utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_log_line(fake_level, fake_cluster, fake_instance, fake_component, input_line)
        assert actual == expected


def test_format_log_line_with_timestamp():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_timestamp = 'fake_timestamp'
    expected = json.dumps({
        'timestamp': fake_timestamp,
        'level': fake_level,
        'cluster': fake_cluster,
        'instance': fake_instance,
        'component': fake_component,
        'message': input_line,
    }, sort_keys=True)
    actual = utils.format_log_line(
        fake_level,
        fake_cluster,
        fake_instance,
        fake_component,
        input_line,
        timestamp=fake_timestamp
    )
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


def test_get_files_in_dir_ignores_unreadable():
    fake_dir = '/fake/dir/'
    fake_file_contents = {'foo': 'bar'}
    expected = [os.path.join(fake_dir, 'a.json'), os.path.join(fake_dir, 'c.json')]
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('os.listdir', autospec=True, return_value=['b.json', 'a.json', 'c.json']),
        mock.patch('os.path.isfile', autospec=True, return_value=True),
        mock.patch('os.access', autospec=True, side_effect=[True, False, True]),
        mock.patch('utils.open', create=True, return_value=file_mock),
        mock.patch('utils.json.load', autospec=True, return_value=fake_file_contents)
    ) as (
        listdir_patch,
        isfile_patch,
        access_patch,
        open_file_patch,
        json_patch,
    ):
        assert utils.get_files_in_dir(fake_dir) == expected


def test_get_files_in_dir_is_lexicographic():
    fake_dir = '/fake/dir/'
    fake_file_contents = {'foo': 'bar'}
    expected = [os.path.join(fake_dir, 'a.json'), os.path.join(fake_dir, 'b.json')]
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('os.listdir', autospec=True, return_value=['b.json', 'a.json']),
        mock.patch('os.path.isfile', autospec=True, return_value=True),
        mock.patch('os.access', autospec=True, return_value=True),
        mock.patch('utils.open', create=True, return_value=file_mock),
        mock.patch('utils.json.load', autospec=True, return_value=fake_file_contents)
    ) as (
        listdir_patch,
        isfile_patch,
        access_patch,
        open_file_patch,
        json_patch,
    ):
        assert utils.get_files_in_dir(fake_dir) == expected


def test_load_system_paasta_config():
    json_load_return_value = {'foo': 'bar'}
    expected = utils.SystemPaastaConfig(json_load_return_value, '/some/fake/dir')
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('os.path.isdir', return_value=True),
        mock.patch('os.access', return_value=True),
        mock.patch('utils.open', create=True, return_value=file_mock),
        mock.patch('utils.get_files_in_dir', autospec=True,
                   return_value=['/some/fake/dir/some_file.json']),
        mock.patch('utils.json.load', autospec=True, return_value=json_load_return_value)
    ) as (
        os_is_dir_patch,
        os_access_patch,
        open_file_patch,
        get_files_in_dir_patch,
        json_patch,
    ):
        actual = utils.load_system_paasta_config()
        assert actual == expected
        # Kinda weird but without this load_system_paasta_config() can (and
        # did! during development) return a plain dict without the test
        # complaining.
        assert actual.__class__ == expected.__class__
        open_file_patch.assert_any_call('/some/fake/dir/some_file.json')
        json_patch.assert_any_call(file_mock.__enter__())
        assert json_patch.call_count == 1


def test_load_system_paasta_config_file_non_existent_dir():
    fake_path = '/var/dir_of_fake'
    with contextlib.nested(
        mock.patch('os.path.isdir', return_value=False),
    ) as (
        isdir_patch,
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = "Could not find system paasta configuration directory: %s" % fake_path
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_non_readable_dir():
    fake_path = '/var/dir_of_fake'
    with contextlib.nested(
        mock.patch('os.path.isdir', return_value=True),
        mock.patch('os.access', return_value=False),
    ) as (
        isdir_patch,
        access_patch,
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = "Could not read from system paasta configuration directory: %s" % fake_path
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_dne():
    fake_path = '/var/dir_of_fake'
    with contextlib.nested(
        mock.patch('os.path.isdir', return_value=True),
        mock.patch('os.access', return_value=True),
        mock.patch('utils.open', create=True, side_effect=IOError(2, 'a', 'b')),
        mock.patch('utils.get_files_in_dir', autospec=True, return_value=[fake_path]),
    ) as (
        isdir_patch,
        access_patch,
        open_patch,
        get_files_in_dir_patch,
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        assert str(excinfo.value) == "Could not load system paasta config file b: a"


def test_load_system_paasta_config_merge_lexographically():
    fake_file_a = {'foo': 'this value will be overriden', 'fake': 'fake_data'}
    fake_file_b = {'foo': 'overriding value'}
    expected = utils.SystemPaastaConfig({'foo': 'overriding value', 'fake': 'fake_data'}, '/some/fake/dir')
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('os.path.isdir', return_value=True),
        mock.patch('os.access', return_value=True),
        mock.patch('utils.open', create=True, return_value=file_mock),
        mock.patch('utils.get_files_in_dir', autospec=True,
                   return_value=['a', 'b']),
        mock.patch('utils.json.load', autospec=True, side_effect=[fake_file_a, fake_file_b])
    ) as (
        os_is_dir_patch,
        os_access_patch,
        open_file_patch,
        get_files_in_dir_patch,
        json_patch,
    ):
        actual = utils.load_system_paasta_config()
        assert actual == expected


def test_SystemPaastaConfig_get_cluster():
    fake_config = utils.SystemPaastaConfig({
        'cluster': 'peanut',
    }, '/some/fake/dir')
    expected = 'peanut'
    actual = fake_config.get_cluster()
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_cluster()


def test_SystemPaastaConfig_get_volumes():
    fake_config = utils.SystemPaastaConfig({
        'volumes': [{'fake_path': "fake_other_path"}],
    }, '/some/fake/dir')
    expected = [{'fake_path': "fake_other_path"}]
    actual = fake_config.get_volumes()
    assert actual == expected


def test_SystemPaastaConfig_get_volumes_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_volumes()


def test_SystemPaastaConfig_get_zk():
    fake_config = utils.SystemPaastaConfig({
        'zookeeper': 'zk://fake_zookeeper_host'
    }, '/some/fake/dir')
    expected = 'fake_zookeeper_host'
    actual = fake_config.get_zk_hosts()
    assert actual == expected


def test_SystemPaastaConfig_get_zk_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_zk_hosts()


def test_SystemPaastaConfig_get_registry():
    fake_config = utils.SystemPaastaConfig({
        'docker_registry': 'fake_registry'
    }, '/some/fake/dir')
    expected = 'fake_registry'
    actual = fake_config.get_docker_registry()
    assert actual == expected


def test_SystemPaastaConfig_get_registry_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_docker_registry()


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


def test_compose_job_id_with_tag():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_tag = "git123abc.config456def"
    expected = "my_cool_service.main.git123abc.config456def"
    actual = utils.compose_job_id(fake_service, fake_instance, fake_tag)
    assert actual == expected


def test_compose_job_id_without_tag():
    fake_service = "my_cool_service"
    fake_instance = "main"
    expected = "my_cool_service.main"
    actual = utils.compose_job_id(fake_service, fake_instance)
    assert actual == expected


def test_decompose_job_id_with_tag():
    fake_job_id = "my_cool_service.main.git123abc.config456def"
    expected = ("my_cool_service", "main", "git123abc.config456def")
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


def test_decompose_job_id_without_tag():
    fake_job_id = "my_cool_service.main"
    expected = ("my_cool_service", "main", None)
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


def test_job_id_too_short():
    with raises(utils.InvalidJobNameError):
        utils.decompose_job_id('foo')


def test_remove_tag_from_job_id_with_tag():
    fake_job_id = "my_cool_service.main.git123abc.config456def"
    expected = "my_cool_service.main"
    actual = utils.remove_tag_from_job_id(fake_job_id)
    assert actual == expected


def test_remove_tag_from_job_id_without_tag():
    fake_job_id = "my_cool_service.main"
    expected = fake_job_id
    actual = utils.remove_tag_from_job_id(fake_job_id)
    assert actual == expected


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


def test_get_default_cluster_for_service():
    fake_service_name = 'fake_service'
    fake_clusters = ['fake_cluster-1', 'fake_cluster-2']
    with contextlib.nested(
        mock.patch('utils.list_clusters', autospec=True, return_value=fake_clusters),
        mock.patch('utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_list_clusters,
        mock_load_system_paasta_config,
    ):
        mock_load_system_paasta_config.side_effect = utils.PaastaNotConfiguredError
        assert utils.get_default_cluster_for_service(fake_service_name) == 'fake_cluster-1'
        mock_list_clusters.assert_called_once_with(fake_service_name)


def test_get_default_cluster_for_service_empty_deploy_config():
    fake_service_name = 'fake_service'
    with contextlib.nested(
        mock.patch('utils.list_clusters', autospec=True, return_value=[]),
        mock.patch('utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_list_clusters,
        mock_load_system_paasta_config,
    ):
        mock_load_system_paasta_config.side_effect = utils.PaastaNotConfiguredError
        with raises(utils.NoConfigurationForServiceError):
            utils.get_default_cluster_for_service(fake_service_name)
        mock_list_clusters.assert_called_once_with(fake_service_name)


def test_list_clusters_no_service_given_lists_all_of_them():
    fake_soa_dir = '/nail/etc/services'
    fake_cluster_configs = ['/nail/etc/services/service1/marathon-cluster1.yaml',
                            '/nail/etc/services/service2/chronos-cluster2.yaml']
    expected = ['cluster1', 'cluster2']
    with contextlib.nested(
        mock.patch('os.path.join', autospec=True, return_value='%s/*' % fake_soa_dir),
        mock.patch('glob.glob', autospec=True, return_value=fake_cluster_configs),
    ) as (
        mock_join_path,
        mock_glob,
    ):
        actual = utils.list_clusters(soa_dir=fake_soa_dir)
        assert actual == expected
        mock_join_path.assert_called_once_with(fake_soa_dir, '*')
        mock_glob.assert_called_once_with('%s/*/*.yaml' % fake_soa_dir)


def test_list_clusters_with_service():
    fake_soa_dir = '/nail/etc/services'
    fake_service = 'fake_service'
    fake_cluster_configs = ['/nail/etc/services/service1/marathon-cluster1.yaml',
                            '/nail/etc/services/service1/chronos-cluster2.yaml']
    expected = ['cluster1', 'cluster2']
    with contextlib.nested(
        mock.patch('os.path.join', autospec=True, return_value='%s/%s' % (fake_soa_dir, fake_service)),
        mock.patch('glob.glob', autospec=True, return_value=fake_cluster_configs),
    ) as (
        mock_join_path,
        mock_glob,
    ):
        actual = utils.list_clusters(fake_service, fake_soa_dir)
        assert actual == expected
        mock_join_path.assert_called_once_with(fake_soa_dir, fake_service)
        mock_glob.assert_called_once_with('%s/%s/*.yaml' % (fake_soa_dir, fake_service))


def test_list_clusters_ignores_bogus_clusters():
    fake_soa_dir = '/nail/etc/services'
    fake_service = 'fake_service'
    fake_cluster_configs = ['/nail/etc/services/service1/marathon-cluster1.yaml',
                            '/nail/etc/services/service1/marathon-PROD.yaml',
                            '/nail/etc/services/service1/chronos-cluster2.yaml',
                            '/nail/etc/services/service1/chronos-SHARED.yaml']
    expected = ['cluster1', 'cluster2']
    with contextlib.nested(
        mock.patch('os.path.join', autospec=True, return_value='%s/%s' % (fake_soa_dir, fake_service)),
        mock.patch('glob.glob', autospec=True, return_value=fake_cluster_configs),
    ) as (
        mock_join_path,
        mock_glob,
    ):
        actual = utils.list_clusters(service=fake_service)
        assert actual == expected


def test_list_all_instances_for_service():
    service = 'fake_service'
    clusters = ['fake_cluster']
    mock_instances = [(service, 'instance1'), (service, 'instance2')]
    expected = set(['instance1', 'instance2'])
    with contextlib.nested(
        mock.patch('utils.list_clusters', autospec=True),
        mock.patch('utils.get_service_instance_list', autospec=True),
    ) as (
        mock_list_clusters,
        mock_service_instance_list,
    ):
        mock_list_clusters.return_value = clusters
        mock_service_instance_list.return_value = mock_instances
        actual = utils.list_all_instances_for_service(service)
        assert actual == expected
        mock_list_clusters.assert_called_once_with(service)
        mock_service_instance_list.assert_called_once_with(service, clusters[0], None)


def test_get_service_instance_list():
    fake_name = 'hint'
    fake_instance_1 = 'unsweet'
    fake_instance_2 = 'water'
    fake_cluster = '16floz'
    fake_dir = '/nail/home/hipster'
    fake_job_config = {fake_instance_1: {},
                       fake_instance_2: {}}
    expected = [(fake_name, fake_instance_1), (fake_name, fake_instance_1),
                (fake_name, fake_instance_2), (fake_name, fake_instance_2)]
    with contextlib.nested(
        mock.patch('utils.service_configuration_lib.read_extra_service_information', autospec=True,
                   return_value=fake_job_config),
    ) as (
        read_extra_info_patch,
    ):
        actual = utils.get_service_instance_list(fake_name, fake_cluster, soa_dir=fake_dir)
        read_extra_info_patch.assert_any_call(fake_name, 'marathon-16floz', soa_dir=fake_dir)
        read_extra_info_patch.assert_any_call(fake_name, 'chronos-16floz', soa_dir=fake_dir)
        assert read_extra_info_patch.call_count == 2
        assert sorted(expected) == sorted(actual)


def test_get_services_for_cluster():
    cluster = 'honey_bunches_of_oats'
    soa_dir = 'completely_wholesome'
    instances = [['this_is_testing', 'all_the_things'], ['my_nerf_broke']]
    expected = ['my_nerf_broke', 'this_is_testing', 'all_the_things']
    with contextlib.nested(
        mock.patch('os.path.abspath', autospec=True, return_value='chex_mix'),
        mock.patch('os.listdir', autospec=True, return_value=['dir1', 'dir2']),
        mock.patch('utils.get_service_instance_list',
                   side_effect=lambda a, b, c, d: instances.pop()),
    ) as (
        abspath_patch,
        listdir_patch,
        get_instances_patch,
    ):
        actual = utils.get_services_for_cluster(cluster, soa_dir=soa_dir)
        assert expected == actual
        abspath_patch.assert_called_once_with(soa_dir)
        listdir_patch.assert_called_once_with('chex_mix')
        get_instances_patch.assert_any_call('dir1', cluster, None, soa_dir)
        get_instances_patch.assert_any_call('dir2', cluster, None, soa_dir)
        assert get_instances_patch.call_count == 2


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


def test_DeploymentsJson_read():
    file_mock = mock.MagicMock(spec=file)
    fake_dir = '/var/dir_of_fake'
    fake_path = '/var/dir_of_fake/fake_service/deployments.json'
    fake_json = {
        'v1': {
            'no_srv:blaster': {
                'docker_image': 'test_rocker:9.9',
                'desired_state': 'start',
                'force_bounce': None,
            },
            'dont_care:about': {
                'docker_image': 'this:guy',
                'desired_state': 'stop',
                'force_bounce': '12345',
            },
        },
    }
    with contextlib.nested(
        mock.patch('utils.open', create=True, return_value=file_mock),
        mock.patch('json.load', autospec=True, return_value=fake_json),
        mock.patch('utils.os.path.isfile', autospec=True, return_value=True),
    ) as (
        open_patch,
        json_patch,
        isfile_patch,
    ):
        actual = utils.load_deployments_json('fake_service', fake_dir)
        open_patch.assert_called_once_with(fake_path)
        json_patch.assert_called_once_with(file_mock.__enter__())
        assert actual == fake_json['v1']


def test_get_docker_url_no_error():
    fake_registry = "im.a-real.vm"
    fake_image = "and-i-can-run:1.0"
    expected = "%s/%s" % (fake_registry, fake_image)
    assert utils.get_docker_url(fake_registry, fake_image) == expected


def test_get_docker_url_with_no_docker_image():
    with raises(utils.NoDockerImageError):
        utils.get_docker_url('fake_registry', None)


def test_run_cancels_timer_thread_on_keyboard_interrupt():
    mock_process = mock.Mock()
    mock_timer_object = mock.Mock()
    with contextlib.nested(
        mock.patch('utils.Popen', autospec=True, return_value=mock_process),
        mock.patch('utils.threading.Timer', autospec=True, return_value=mock_timer_object),
    ) as (
        mock_popen,
        mock_timer
    ):
        mock_process.stdout.readline.side_effect = KeyboardInterrupt
        with raises(KeyboardInterrupt):
            utils._run('sh echo foo', timeout=10)
        assert mock_timer_object.cancel.call_count == 1


class TestInstanceConfig:

    def test_get_monitoring(self):
        fake_info = 'fake_info'
        assert utils.InstanceConfig({'monitoring': fake_info}, {}).get_monitoring() == fake_info

    def test_get_cpus_in_config(self):
        fake_conf = utils.InstanceConfig({'cpus': -5}, {})
        assert fake_conf.get_cpus() == -5

    def test_get_cpus_in_config_float(self):
        fake_conf = utils.InstanceConfig({'cpus': .66}, {})
        assert fake_conf.get_cpus() == .66

    def test_get_cpus_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_cpus() == .25

    def test_get_mem_in_config(self):
        fake_conf = utils.InstanceConfig({'mem': -999}, {})
        assert fake_conf.get_mem() == -999

    def test_get_mem_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_mem() == 1024

    def test_get_cmd_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_cmd() is None

    def test_get_cmd_in_config(self):
        fake_conf = utils.InstanceConfig({'cmd': 'FAKECMD'}, {})
        assert fake_conf.get_cmd() == 'FAKECMD'

    def test_get_env_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_env() == {}

    def test_get_env_with_config(self):
        fake_conf = utils.InstanceConfig({'env': {'SPECIAL_ENV': 'TRUE'}}, {})
        assert fake_conf.get_env() == {'SPECIAL_ENV': 'TRUE'}

    def test_get_args_default_no_cmd(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_args() == []

    def test_get_args_default_with_cmd(self):
        fake_conf = utils.InstanceConfig({'cmd': 'FAKECMD'}, {})
        assert fake_conf.get_args() is None

    def test_get_args_in_config(self):
        fake_conf = utils.InstanceConfig({'args': ['arg1', 'arg2']}, {})
        assert fake_conf.get_args() == ['arg1', 'arg2']

    def test_get_args_in_config_with_cmd(self):
        fake_conf = utils.InstanceConfig({'args': ['A'], 'cmd': 'C'}, {})
        fake_conf.get_cmd()
        with raises(utils.InvalidInstanceConfig):
            fake_conf.get_args()

    def test_get_force_bounce(self):
        fake_conf = utils.InstanceConfig({}, {'force_bounce': 'blurp'})
        assert fake_conf.get_force_bounce() == 'blurp'

    def test_get_desired_state(self):
        fake_conf = utils.InstanceConfig({}, {'desired_state': 'stop'})
        assert fake_conf.get_desired_state() == 'stop'

    def test_get_desired_state_human(self):
        fake_conf = utils.InstanceConfig({}, {'desired_state': 'stop'})
        assert 'Stopped' in fake_conf.get_desired_state_human()

    def test_monitoring_blacklist_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_monitoring_blacklist() == []

    def test_monitoring_blacklist_defaults_to_deploy_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = utils.InstanceConfig({'deploy_blacklist': fake_deploy_blacklist}, {})
        assert fake_conf.get_monitoring_blacklist() == fake_deploy_blacklist

    def test_deploy_blacklist_default(self):
        fake_conf = utils.InstanceConfig({}, {})
        assert fake_conf.get_deploy_blacklist() == []

    def test_deploy_blacklist_reads_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = utils.InstanceConfig({'deploy_blacklist': fake_deploy_blacklist}, {})
        assert fake_conf.get_deploy_blacklist() == fake_deploy_blacklist


def test_is_under_replicated_ok():
    num_available = 1
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_zero():
    num_available = 1
    expected_count = 0
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_critical():
    num_available = 0
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (True, float(0))


def test_blacklist_to_constraints():
    fake_blacklist = [["region", "useast1-prod"], ["habitat", "fake_habitat"]]
    expected_constraints = [["region", "UNLIKE", "useast1-prod"], ["habitat", "UNLIKE", "fake_habitat"]]
    actual = utils.blacklist_to_constraints(fake_blacklist)
    assert actual == expected_constraints
