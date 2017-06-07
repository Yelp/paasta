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
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import json
import os
import stat

import mock
import pytest
from pytest import raises

from paasta_tools import utils


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
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_now = 'fake_now'
    expected = json.dumps({
        'timestamp': fake_now,
        'level': fake_level,
        'cluster': fake_cluster,
        'service': fake_service,
        'instance': fake_instance,
        'component': fake_component,
        'message': input_line,
    }, sort_keys=True)
    with mock.patch('paasta_tools.utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_log_line(
            level=fake_level,
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            component=fake_component,
            line=input_line,
        )
        assert actual == expected


def test_deploy_whitelist_to_constraints():
    fake_whitelist = ['fake_location_type', ['fake_location', 'anotherfake_location']]
    expected_constraints = [['fake_location_type', 'LIKE', 'fake_location|anotherfake_location']]

    constraints = utils.deploy_whitelist_to_constraints(fake_whitelist)
    assert constraints == expected_constraints


def test_format_log_line_with_timestamp():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_timestamp = 'fake_timestamp'
    expected = json.dumps({
        'timestamp': fake_timestamp,
        'level': fake_level,
        'cluster': fake_cluster,
        'service': fake_service,
        'instance': fake_instance,
        'component': fake_component,
        'message': input_line,
    }, sort_keys=True)
    actual = utils.format_log_line(
        fake_level,
        fake_cluster,
        fake_service,
        fake_instance,
        fake_component,
        input_line,
        timestamp=fake_timestamp
    )
    assert actual == expected


def test_format_log_line_rejects_invalid_components():
    with raises(utils.NoSuchLogComponent):
        utils.format_log_line(
            level='debug',
            cluster='fake_cluster',
            service='fake_service',
            instance='fake_instance',
            line='fake_line',
            component='BOGUS_COMPONENT',
        )


def test_ScribeLogWriter_log_raise_on_unknown_level():
    with raises(utils.NoSuchLogLevel):
        utils.ScribeLogWriter().log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')


def test_get_log_name_for_service():
    service = 'foo'
    expected = 'stream_paasta_%s' % service
    assert utils.get_log_name_for_service(service) == expected


def test_get_readable_files_in_glob_ignores_unreadable(tmpdir):
    tmpdir.join('readable.json').ensure().chmod(0o644)
    tmpdir.join('unreadable.json').ensure().chmod(0o000)
    ret = utils.get_readable_files_in_glob('*.json', tmpdir.strpath)
    assert ret == [tmpdir.join('readable.json').strpath]


def test_get_readable_files_in_glob_is_recursive(tmpdir):
    a = tmpdir.join('a.json').ensure()
    b = tmpdir.join('b.json').ensure()
    c = tmpdir.join('subdir').ensure_dir().join('c.json').ensure()
    ret = utils.get_readable_files_in_glob('*.json', tmpdir.strpath)
    assert set(ret) == {a.strpath, b.strpath, c.strpath}


def test_load_system_paasta_config():
    json_load_return_value = {'foo': 'bar'}
    expected = utils.SystemPaastaConfig(json_load_return_value, '/some/fake/dir')
    file_mock = mock.mock_open()
    with mock.patch(
        'os.path.isdir', return_value=True, autospec=True,
    ), mock.patch(
        'os.access', return_value=True, autospec=True,
    ), mock.patch(
        'six.moves.builtins.open', file_mock, autospec=None,
    ) as open_file_patch, mock.patch(
        'paasta_tools.utils.get_readable_files_in_glob', autospec=True,
        return_value=['/some/fake/dir/some_file.json'],
    ), mock.patch(
        'paasta_tools.utils.json.load', autospec=True, return_value=json_load_return_value,
    ) as json_patch, mock.patch(
        'paasta_tools.utils.deep_merge_dictionaries', autospec=True, return_value=json_load_return_value,
    ) as mock_deep_merge:
        actual = utils.load_system_paasta_config()
        assert actual == expected
        # Kinda weird but without this load_system_paasta_config() can (and
        # did! during development) return a plain dict without the test
        # complaining.
        assert actual.__class__ == expected.__class__
        open_file_patch.assert_any_call('/some/fake/dir/some_file.json')
        json_patch.assert_any_call(file_mock.return_value.__enter__.return_value)
        assert json_patch.call_count == 1
        mock_deep_merge.assert_called_with(json_load_return_value, {})


def test_load_system_paasta_config_file_non_existent_dir():
    fake_path = '/var/dir_of_fake'
    with mock.patch('os.path.isdir', return_value=False, autospec=True):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = "Could not find system paasta configuration directory: %s" % fake_path
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_non_readable_dir():
    fake_path = '/var/dir_of_fake'
    with mock.patch(
        'os.path.isdir', return_value=True, autospec=True,
    ), mock.patch(
        'os.access', return_value=False, autospec=True,
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = "Could not read from system paasta configuration directory: %s" % fake_path
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_dne():
    fake_path = '/var/dir_of_fake'
    with mock.patch(
        'os.path.isdir', return_value=True, autospec=True,
    ), mock.patch(
        'os.access', return_value=True, autospec=True,
    ), mock.patch(
        'six.moves.builtins.open', side_effect=IOError(2, 'a', 'b'), autospec=None,
    ), mock.patch(
        'paasta_tools.utils.get_readable_files_in_glob', autospec=True, return_value=[fake_path],
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        assert str(excinfo.value) == "Could not load system paasta config file b: a"


def test_load_system_paasta_config_merge_lexographically():
    fake_file_a = {'foo': 'this value will be overriden', 'fake': 'fake_data'}
    fake_file_b = {'foo': 'overriding value'}
    expected = utils.SystemPaastaConfig({'foo': 'overriding value', 'fake': 'fake_data'}, '/some/fake/dir')
    file_mock = mock.mock_open()
    with mock.patch(
        'os.path.isdir', return_value=True, autospec=True,
    ), mock.patch(
        'os.access', return_value=True, autospec=True,
    ), mock.patch(
        'six.moves.builtins.open', file_mock, autospec=None,
    ), mock.patch(
        'paasta_tools.utils.get_readable_files_in_glob', autospec=True,
        return_value=['a', 'b'],
    ), mock.patch(
        'paasta_tools.utils.json.load', autospec=True, side_effect=[fake_file_a, fake_file_b],
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


def test_SystemPaastaConfig_get_sensu_host_default():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    actual = fake_config.get_sensu_host()
    expected = 'localhost'
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_host():
    fake_config = utils.SystemPaastaConfig({"sensu_host": "blurp"}, '/some/fake/dir')
    actual = fake_config.get_sensu_host()
    expected = 'blurp'
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_host_None():
    fake_config = utils.SystemPaastaConfig({"sensu_host": None}, '/some/fake/dir')
    actual = fake_config.get_sensu_host()
    expected = None
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_port_default():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    actual = fake_config.get_sensu_port()
    expected = 3030
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_port():
    fake_config = utils.SystemPaastaConfig({"sensu_port": 4040}, '/some/fake/dir')
    actual = fake_config.get_sensu_port()
    expected = 4040
    assert actual == expected


def test_SystemPaastaConfig_get_deployd_metrics_provider():
    fake_config = utils.SystemPaastaConfig({"deployd_metrics_provider": 'bar'}, '/some/fake/dir')
    actual = fake_config.get_deployd_metrics_provider()
    expected = 'bar'
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_fqdn_format_default():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    actual = fake_config.get_cluster_fqdn_format()
    expected = 'paasta-{cluster:s}.yelp'
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_fqdn_format():
    fake_config = utils.SystemPaastaConfig({"cluster_fqdn_format": "paasta-{cluster:s}.something"}, '/some/fake/dir')
    actual = fake_config.get_cluster_fqdn_format()
    expected = 'paasta-{cluster:s}.something'
    assert actual == expected


def test_SystemPaastaConfig_get_deployd_number_workers():
    fake_config = utils.SystemPaastaConfig({"deployd_number_workers": 3}, '/some/fake/dir')
    actual = fake_config.get_deployd_number_workers()
    expected = 3
    assert actual == expected


def test_SystemPaastaConfig_get_deployd_big_bounce_rate():
    fake_config = utils.SystemPaastaConfig({"deployd_big_bounce_rate": 3}, '/some/fake/dir')
    actual = fake_config.get_deployd_big_bounce_rate()
    expected = 3
    assert actual == expected


def test_SystemPaastaConfig_get_deployd_log_level():
    fake_config = utils.SystemPaastaConfig({"deployd_log_level": 'DEBUG'}, '/some/fake/dir')
    actual = fake_config.get_deployd_log_level()
    expected = 'DEBUG'
    assert actual == expected


@pytest.yield_fixture
def umask_022():
    old_umask = os.umask(0o022)
    yield
    os.umask(old_umask)


def test_atomic_file_write_itest(umask_022, tmpdir):
    target_file_name = tmpdir.join('test_atomic_file_write_itest.txt').strpath

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
    assert stat.S_IMODE(file_stat.st_mode) == 0o0644


def test_configure_log():
    fake_log_writer_config = {'driver': 'fake', 'options': {'fake_arg': 'something'}}
    with mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config().get_log_writer.return_value = fake_log_writer_config
        with mock.patch('paasta_tools.utils.get_log_writer_class', autospec=True) as mock_get_log_writer_class:
            utils.configure_log()
            mock_get_log_writer_class.assert_called_once_with('fake')
            mock_get_log_writer_class('fake').assert_called_once_with(fake_arg='something')


def test_compose_job_id_without_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    expected = "my_cool_service.main"
    actual = utils.compose_job_id(fake_service, fake_instance)
    assert actual == expected


def test_compose_job_id_with_git_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    with raises(utils.InvalidJobNameError):
        utils.compose_job_id(fake_service, fake_instance, git_hash=fake_git_hash)


def test_compose_job_id_with_config_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_config_hash = "config456def"
    with raises(utils.InvalidJobNameError):
        utils.compose_job_id(fake_service, fake_instance, config_hash=fake_config_hash)


def test_compose_job_id_with_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    fake_config_hash = "config456def"
    expected = "my_cool_service.main.git123abc.config456def"
    actual = utils.compose_job_id(fake_service, fake_instance, fake_git_hash, fake_config_hash)
    assert actual == expected


def test_decompose_job_id_too_short():
    with raises(utils.InvalidJobNameError):
        utils.decompose_job_id('foo')


def test_decompose_job_id_without_hashes():
    fake_job_id = "my_cool_service.main"
    expected = ("my_cool_service", "main", None, None)
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


def test_decompose_job_id_with_hashes():
    fake_job_id = "my_cool_service.main.git123abc.config456def"
    expected = ("my_cool_service", "main", "git123abc", "config456def")
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


@mock.patch('paasta_tools.utils.build_docker_image_name', autospec=True)
def test_build_docker_tag(mock_build_docker_image_name):
    upstream_job_name = 'foo'
    upstream_git_commit = 'bar'
    mock_build_docker_image_name.return_value = 'fake-registry/services-foo'
    expected = 'fake-registry/services-foo:paasta-%s' % (
        upstream_git_commit,
    )
    actual = utils.build_docker_tag(upstream_job_name, upstream_git_commit)
    assert actual == expected


@mock.patch('paasta_tools.utils.build_docker_image_name', autospec=True)
def test_check_docker_image_false(mock_build_docker_image_name):
    mock_build_docker_image_name.return_value = 'fake-registry/services-foo'
    fake_app = 'fake_app'
    fake_commit = 'fake_commit'
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch('paasta_tools.utils.get_docker_client', autospec=True) as mock_docker:
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


@mock.patch('paasta_tools.utils.build_docker_image_name', autospec=True)
def test_check_docker_image_true(mock_build_docker_image_name):
    fake_app = 'fake_app'
    fake_commit = 'fake_commit'
    mock_build_docker_image_name.return_value = 'fake-registry/services-foo'
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch('paasta_tools.utils.get_docker_client', autospec=True) as mock_docker:
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


def test_list_clusters_no_service_given_lists_all_of_them():
    fake_soa_dir = '/nail/etc/services'
    fake_cluster_configs = ['/nail/etc/services/service1/marathon-cluster1.yaml',
                            '/nail/etc/services/service2/chronos-cluster2.yaml']
    expected = ['cluster1', 'cluster2']
    with mock.patch(
        'os.path.join', autospec=True, return_value='%s/*' % fake_soa_dir,
    ) as mock_join_path, mock.patch(
        'glob.glob', autospec=True, return_value=fake_cluster_configs,
    ) as mock_glob:
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
    with mock.patch(
        'os.path.join', autospec=True, return_value='%s/%s' % (fake_soa_dir, fake_service),
    ) as mock_join_path, mock.patch(
        'glob.glob', autospec=True, return_value=fake_cluster_configs,
    ) as mock_glob:
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
    with mock.patch(
        'os.path.join', autospec=True, return_value='%s/%s' % (fake_soa_dir, fake_service),
    ), mock.patch(
        'glob.glob', autospec=True, return_value=fake_cluster_configs,
    ):
        actual = utils.list_clusters(service=fake_service)
        assert actual == expected


def test_list_all_instances_for_service():
    service = 'fake_service'
    clusters = ['fake_cluster']
    mock_instances = [(service, 'instance1'), (service, 'instance2')]
    expected = {'instance1', 'instance2'}
    with mock.patch(
        'paasta_tools.utils.list_clusters', autospec=True,
    ) as mock_list_clusters, mock.patch(
        'paasta_tools.utils.get_service_instance_list', autospec=True,
    ) as mock_service_instance_list:
        mock_list_clusters.return_value = clusters
        mock_service_instance_list.return_value = mock_instances
        actual = utils.list_all_instances_for_service(service)
        assert actual == expected
        mock_list_clusters.assert_called_once_with(service, soa_dir=mock.ANY)
        mock_service_instance_list.assert_called_once_with(service, clusters[0], None, soa_dir=mock.ANY)


def test_get_service_instance_list():
    fake_name = 'hint'
    fake_instance_1 = 'unsweet'
    fake_instance_2 = 'water'
    fake_cluster = '16floz'
    fake_dir = '/nail/home/hipster'
    fake_job_config = {fake_instance_1: {},
                       fake_instance_2: {}}
    expected = [
        (fake_name, fake_instance_1),
        (fake_name, fake_instance_1),
        (fake_name, fake_instance_1),
        (fake_name, fake_instance_1),
        (fake_name, fake_instance_2),
        (fake_name, fake_instance_2),
        (fake_name, fake_instance_2),
        (fake_name, fake_instance_2),
    ]
    with mock.patch(
        'paasta_tools.utils.service_configuration_lib.read_extra_service_information', autospec=True,
        return_value=fake_job_config,
    ) as read_extra_info_patch:
        actual = utils.get_service_instance_list(fake_name, fake_cluster, soa_dir=fake_dir)
        read_extra_info_patch.assert_any_call(fake_name, 'marathon-16floz', soa_dir=fake_dir)
        read_extra_info_patch.assert_any_call(fake_name, 'chronos-16floz', soa_dir=fake_dir)
        read_extra_info_patch.assert_any_call(fake_name, 'paasta_native-16floz', soa_dir=fake_dir)
        assert read_extra_info_patch.call_count == 4
        assert sorted(expected) == sorted(actual)


def test_get_services_for_cluster():
    cluster = 'honey_bunches_of_oats'
    soa_dir = 'completely_wholesome'
    instances = [['this_is_testing', 'all_the_things'], ['my_nerf_broke']]
    expected = ['my_nerf_broke', 'this_is_testing', 'all_the_things']
    with mock.patch(
        'os.path.abspath', autospec=True, return_value='chex_mix',
    ) as abspath_patch, mock.patch(
        'os.listdir', autospec=True, return_value=['dir1', 'dir2'],
    ) as listdir_patch, mock.patch(
        'paasta_tools.utils.get_service_instance_list',
        side_effect=lambda a, b, c, d: instances.pop(), autospec=True,
    ) as get_instances_patch:
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
    file_mock = mock.mock_open()
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
    with mock.patch(
        'six.moves.builtins.open', file_mock, autospec=None,
    ) as open_patch, mock.patch(
        'json.load', autospec=True, return_value=fake_json,
    ) as json_patch, mock.patch(
        'paasta_tools.utils.os.path.isfile', autospec=True, return_value=True,
    ):
        actual = utils.load_deployments_json('fake_service', fake_dir)
        open_patch.assert_called_once_with(fake_path)
        json_patch.assert_called_once_with(file_mock.return_value.__enter__.return_value)
        assert actual == fake_json['v1']


def test_get_docker_url_no_error():
    fake_registry = "im.a-real.vm"
    fake_image = "and-i-can-run:1.0"
    expected = "%s/%s" % (fake_registry, fake_image)
    assert utils.get_docker_url(fake_registry, fake_image) == expected


def test_get_docker_url_with_no_docker_image():
    with raises(utils.NoDockerImageError):
        utils.get_docker_url('fake_registry', None)


def test_get_running_mesos_docker_containers():

    fake_container_data = [
        {
            "Status": "Up 2 hours",
            "Names": ['/mesos-legit.e1ad42eb-3ed7-4c9b-8711-aff017ef55a5'],
            "Id": "05698f4156c4f30c8dcd747f7724b14c9af7771c9a4b96fdd6aa37d6419a12a3"
        },
        {
            "Status": "Up 3 days",
            "Names": ['/definitely_not_meeeeesos-.6d2fb3aa-2fef-4f98-8fed-df291481e91f'],
            "Id": "ae66e2c3fe3c4b2a7444212592afea5cc6a4d8ca70ee595036b19949e00a257c"
        }
    ]

    with mock.patch("paasta_tools.utils.get_docker_client", autospec=True) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.containers.return_value = fake_container_data
        assert len(utils.get_running_mesos_docker_containers()) == 1


def test_run_cancels_timer_thread_on_keyboard_interrupt():
    mock_process = mock.Mock()
    mock_timer_object = mock.Mock()
    with mock.patch(
        'paasta_tools.utils.Popen', autospec=True, return_value=mock_process,
    ), mock.patch(
        'paasta_tools.utils.threading.Timer', autospec=True, return_value=mock_timer_object,
    ):
        mock_process.stdout.readline.side_effect = KeyboardInterrupt
        with raises(KeyboardInterrupt):
            utils._run('sh echo foo', timeout=10)
        assert mock_timer_object.cancel.call_count == 1


def test_run_returns_when_popen_fails():
    fake_exception = OSError(1234, 'fake error')
    with mock.patch('paasta_tools.utils.Popen', autospec=True, side_effect=fake_exception):
        return_code, output = utils._run('nonexistant command', timeout=10)
    assert return_code == 1234
    assert 'fake error' in output


@pytest.mark.parametrize(
    ('dcts', 'expected'),
    (
        (
            [{'a': 'b'}, {'c': 'd'}],
            [{'a': 'b'}, {'c': 'd'}],
        ),
        (
            [{'c': 'd'}, {'a': 'b'}],
            [{'a': 'b'}, {'c': 'd'}],
        ),
        (
            [{'a': 'b', 'c': 'd'}, {'a': 'b'}],
            [{'a': 'b'}, {'a': 'b', 'c': 'd'}],
        ),
    ),
)
def test_sort_dcts(dcts, expected):
    assert utils.sort_dicts(dcts) == expected


class TestInstanceConfig:

    def test_get_monitoring(self):
        fake_info = 'fake_info'
        assert utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'monitoring': fake_info},
            branch_dict={},
        ).get_monitoring() == fake_info

    def test_get_cpus_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cpus': -5},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == -5

    def test_get_cpus_in_config_float(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cpus': .66},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == .66

    def test_get_cpus_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == .25

    def test_get_mem_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'mem': -999},
            branch_dict={},
        )
        assert fake_conf.get_mem() == -999

    def test_get_mem_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_mem() == 1024

    def test_zero_cpu_burst(self):
        fake_conf = utils.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'cpu_burst_pct': 0, 'cpus': 1},
            branch_dict={},
        )
        assert fake_conf.get_cpu_quota() == 100000

    def test_format_docker_parameters_default(self):
        fake_conf = utils.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={
                'cpus': 1,
                'mem': 1024,
            },
            branch_dict={},
        )
        assert fake_conf.format_docker_parameters() == [
            {"key": "memory-swap", "value": '1024m'},
            {"key": "cpu-period", "value": "100000"},
            {"key": "cpu-quota", "value": "1000000"},
            {"key": "label", "value": "paasta_service=fake_name"},
            {"key": "label", "value": "paasta_instance=fake_instance"},
        ]

    def test_format_docker_parameters_non_default(self):
        fake_conf = utils.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={
                'cpu_burst_pct': 200,
                'cfs_period_us': 200000,
                'cpus': 1,
                'mem': 1024,
                'ulimit': {
                    'nofile': {'soft': 1024, 'hard': 2048},
                    'nice': {'soft': 20},
                },
                'cap_add': ['IPC_LOCK', 'SYS_PTRACE'],
            },
            branch_dict={},
        )
        assert fake_conf.format_docker_parameters() == [
            {"key": "memory-swap", "value": '1024m'},
            {"key": "cpu-period", "value": "200000"},
            {"key": "cpu-quota", "value": "600000"},
            {"key": "label", "value": "paasta_service=fake_name"},
            {"key": "label", "value": "paasta_instance=fake_instance"},
            {"key": "ulimit", "value": "nice=20"},
            {"key": "ulimit", "value": "nofile=1024:2048"},
            {"key": "cap-add", "value": "IPC_LOCK"},
            {"key": "cap-add", "value": "SYS_PTRACE"},
        ]

    def test_full_cpu_burst(self):
        fake_conf = utils.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'cpu_burst_pct': 100, 'cpus': 1},
            branch_dict={},
        )
        assert fake_conf.get_cpu_quota() == 200000

    def test_get_mem_swap_int(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'mem': 50
            },
            branch_dict={},
        )
        assert fake_conf.get_mem_swap() == "50m"

    def test_get_mem_swap_float_rounds_up(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'mem': 50.4
            },
            branch_dict={},
        )
        assert fake_conf.get_mem_swap() == "51m"

    def test_get_disk_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'disk': -999},
            branch_dict={},
        )
        assert fake_conf.get_disk() == -999

    def test_get_disk_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_disk() == 1024

    def test_get_ulimit_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'ulimit': {
                    'nofile': {'soft': 1024, 'hard': 2048},
                    'nice': {'soft': 20},
                }
            },
            branch_dict={},
        )
        assert list(fake_conf.get_ulimit()) == [
            {"key": "ulimit", "value": "nice=20"},
            {"key": "ulimit", "value": "nofile=1024:2048"},
        ]

    def test_get_ulimit_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert list(fake_conf.get_ulimit()) == []

    def test_get_cap_add_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'cap_add': ['IPC_LOCK', 'SYS_PTRACE'],
            },
            branch_dict={},
        )
        assert list(fake_conf.get_cap_add()) == [
            {"key": "cap-add", "value": "IPC_LOCK"},
            {"key": "cap-add", "value": "SYS_PTRACE"},
        ]

    def test_get_cap_add_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert list(fake_conf.get_cap_add()) == []

    def test_deploy_group_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='fake_instance',
            cluster='fake_cluster',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'fake_cluster.fake_instance'

    def test_deploy_group_if_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'deploy_group': 'fake_deploy_group'},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'fake_deploy_group'

    def test_deploy_group_string_interpolation(self):
        fake_conf = utils.InstanceConfig(
            service='',
            instance='',
            cluster='fake_cluster',
            config_dict={'deploy_group': 'cluster_is_{cluster}'},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'cluster_is_fake_cluster'

    def test_get_cmd_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_cmd() is None

    def test_get_cmd_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cmd': 'FAKECMD'},
            branch_dict={},
        )
        assert fake_conf.get_cmd() == 'FAKECMD'

    def test_get_env_default(self):
        fake_conf = utils.InstanceConfig(
            service='fake_service',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_env() == {
            'PAASTA_SERVICE': 'fake_service',
            'PAASTA_INSTANCE': 'fake_instance',
            'PAASTA_CLUSTER': 'fake_cluster',
            'PAASTA_DOCKER_IMAGE': '',
        }

    def test_get_env_with_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'env': {'SPECIAL_ENV': 'TRUE'}},
            branch_dict={'docker_image': 'something'},
        )
        assert fake_conf.get_env() == {
            'SPECIAL_ENV': 'TRUE',
            'PAASTA_SERVICE': '',
            'PAASTA_INSTANCE': '',
            'PAASTA_CLUSTER': '',
            'PAASTA_DOCKER_IMAGE': 'something',
        }

    def test_get_args_default_no_cmd(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_args() == []

    def test_get_args_default_with_cmd(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cmd': 'FAKECMD'},
            branch_dict={},
        )
        assert fake_conf.get_args() is None

    def test_get_args_in_config(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'args': ['arg1', 'arg2']},
            branch_dict={},
        )
        assert fake_conf.get_args() == ['arg1', 'arg2']

    def test_get_args_in_config_with_cmd(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'args': ['A'], 'cmd': 'C'},
            branch_dict={},
        )
        fake_conf.get_cmd()
        with raises(utils.InvalidInstanceConfig):
            fake_conf.get_args()

    def test_get_force_bounce(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'force_bounce': 'blurp'},
        )
        assert fake_conf.get_force_bounce() == 'blurp'

    def test_get_desired_state(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'desired_state': 'stop'},
        )
        assert fake_conf.get_desired_state() == 'stop'

    def test_monitoring_blacklist_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_monitoring_blacklist(system_deploy_blacklist=[]) == []

    def test_monitoring_blacklist_defaults_to_deploy_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'deploy_blacklist': fake_deploy_blacklist},
            branch_dict={},
        )
        assert fake_conf.get_monitoring_blacklist(system_deploy_blacklist=[]) == fake_deploy_blacklist

    def test_deploy_blacklist_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_deploy_blacklist(system_deploy_blacklist=[]) == []

    def test_deploy_blacklist_reads_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'deploy_blacklist': fake_deploy_blacklist},
            branch_dict={},
        )
        assert fake_conf.get_deploy_blacklist(system_deploy_blacklist=[]) == fake_deploy_blacklist

    def test_extra_volumes_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_extra_volumes() == []

    def test_extra_volumes_normal(self):
        fake_extra_volumes = [
            {
                "containerPath": "/etc/a",
                "hostPath": "/var/data/a",
                "mode": "RO"
            },
        ]
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'extra_volumes': fake_extra_volumes},
            branch_dict={},
        )
        assert fake_conf.get_extra_volumes() == fake_extra_volumes

    def test_get_pool(self):
        pool = "poolname"
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'pool': pool},
            branch_dict={},
        )
        assert fake_conf.get_pool() == pool

    def test_get_pool_default(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_pool() == 'default'

    def test_get_volumes_dedupes_correctly_when_mode_differs_last_wins(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'extra_volumes': [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RW"},
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                ],
            },
            branch_dict={},
        )
        system_volumes = []
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
        ]

    def test_get_volumes_dedupes_respects_hostpath(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'extra_volumes': [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                    {"containerPath": "/a", "hostPath": "/other_a", "mode": "RO"},
                ],
            },
            branch_dict={},
        )
        system_volumes = [{"containerPath": "/a", "hostPath": "/a", "mode": "RO"}]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/a", "hostPath": "/other_a", "mode": "RO"},
        ]

    def test_get_volumes_handles_dupes_everywhere(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'extra_volumes': [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                    {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
                    {"containerPath": "/c", "hostPath": "/c", "mode": "RO"},
                ],
            },
            branch_dict={},
        )
        system_volumes = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
            {"containerPath": "/d", "hostPath": "/d", "mode": "RO"},
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
            {"containerPath": "/c", "hostPath": "/c", "mode": "RO"},
            {"containerPath": "/d", "hostPath": "/d", "mode": "RO"},
        ]

    def test_get_volumes_prefers_extra_volumes_over_system(self):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'extra_volumes': [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RW"},
                ],
            },
            branch_dict={},
        )
        system_volumes = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RW"},
        ]

    @pytest.mark.parametrize(('dependencies_reference', 'dependencies', 'expected'), [
        (None, None, None),
        ('aaa', None, None),
        ('aaa', {}, None),
        ('aaa', {"aaa": [{"foo": "bar"}]}, {"foo": "bar"}),
        ('aaa', {"bbb": [{"foo": "bar"}]}, None),
    ])
    def test_get_dependencies(self, dependencies_reference, dependencies, expected):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'dependencies_reference': dependencies_reference,
                'dependencies': dependencies
            },
            branch_dict={},
        )
        fake_conf.get_dependencies() == expected

    @pytest.mark.parametrize(('security', 'expected'), [
        ({}, None),
        (None, None),
        ({"outbound_firewall": "monitor"}, 'monitor'),
        ({"outbound_firewall": "foo"}, 'foo'),
    ])
    def test_get_outbound_firewall(self, security, expected):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'security': security},
            branch_dict={},
        )
        fake_conf.get_outbound_firewall() == expected

    @pytest.mark.parametrize(('security', 'expected'), [
        ({}, (True, '')),
        ({"outbound_firewall": "monitor"}, (True, '')),
        ({"outbound_firewall": "block"}, (True, '')),
        ({"outbound_firewall": "foo"}, (False, 'Unrecognized outbound_firewall value "foo"')),
        ({"outbound_firewall": "monitor", "foo": 1},
            (False, 'Unrecognized items in security dict of service config: "foo"')),
    ])
    def test_check_security(self, security, expected):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'security': security},
            branch_dict={},
        )
        assert fake_conf.check_security() == expected

    @pytest.mark.parametrize(('dependencies_reference', 'dependencies', 'expected'), [
        (None, None, (True, '')),
        ('aaa', {"aaa": []}, (True, '')),
        ('aaa', None, (False, 'dependencies_reference "aaa" declared but no dependencies found')),
        ('aaa', {"bbb": []}, (False, 'dependencies_reference "aaa" not found in dependencies dictionary')),
    ])
    def test_check_dependencies_reference(self, dependencies_reference, dependencies, expected):
        fake_conf = utils.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'dependencies_reference': dependencies_reference,
                'dependencies': dependencies
            },
            branch_dict={},
        )
        assert fake_conf.check_dependencies_reference() == expected


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


def test_deploy_blacklist_to_constraints():
    fake_deploy_blacklist = [["region", "useast1-prod"], ["habitat", "fake_habitat"]]
    expected_constraints = [["region", "UNLIKE", "useast1-prod"], ["habitat", "UNLIKE", "fake_habitat"]]
    actual = utils.deploy_blacklist_to_constraints(fake_deploy_blacklist)
    assert actual == expected_constraints


def test_validate_service_instance_valid_marathon():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    my_service = 'service1'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with mock.patch(
        'paasta_tools.utils.get_services_for_cluster',
        autospec=True,
        side_effect=[mock_marathon_services, mock_chronos_services],
    ) as get_services_for_cluster_patch:
        assert utils.validate_service_instance(
            my_service,
            my_instance,
            fake_cluster,
            fake_soa_dir,
        ) == 'marathon'
        assert mock.call(
            cluster=fake_cluster,
            instance_type='marathon',
            soa_dir=fake_soa_dir,
        ) in get_services_for_cluster_patch.call_args_list


def test_validate_service_instance_valid_chronos():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    my_service = 'service1'
    my_instance = 'worker'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with mock.patch(
        'paasta_tools.utils.get_services_for_cluster',
        autospec=True,
        side_effect=[mock_marathon_services, mock_chronos_services],
    ) as get_services_for_cluster_patch:
        assert utils.validate_service_instance(
            my_service,
            my_instance,
            fake_cluster,
            fake_soa_dir,
        ) == 'chronos'
        assert mock.call(
            cluster=fake_cluster,
            instance_type='chronos',
            soa_dir=fake_soa_dir,
        ) in get_services_for_cluster_patch.call_args_list


def test_validate_service_instance_invalid():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    mock_paasta_native_services = [('service1', 'main2'), ('service2', 'main2')]
    mock_adhoc_services = [('service1', 'interactive'), ('service2', 'interactive')]
    my_service = 'bad_service'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with mock.patch(
        'paasta_tools.utils.get_services_for_cluster',
        autospec=True,
        side_effect=[
            mock_marathon_services, mock_chronos_services,
            mock_paasta_native_services, mock_adhoc_services,
        ],
    ):
        with raises(utils.NoConfigurationForServiceError):
            utils.validate_service_instance(
                my_service,
                my_instance,
                fake_cluster,
                fake_soa_dir,
            )


def test_terminal_len():
    assert len('some text') == utils.terminal_len(utils.PaastaColors.red('some text'))


def test_format_table():
    actual = utils.format_table(
        [
            ['looooong', 'y', 'z'],
            ['a', 'looooong', 'c'],
            ['j', 'k', 'looooong']
        ]
    )
    expected = [
        'looooong  y         z',
        'a         looooong  c',
        'j         k         looooong',
    ]
    assert actual == expected
    assert ["a     b     c"] == utils.format_table([['a', 'b', 'c']], min_spacing=5)


def test_format_table_with_interjected_lines():
    actual = utils.format_table(
        [
            ['looooong', 'y', 'z'],
            'interjection',
            ['a', 'looooong', 'c'],
            'unicode interjection',
            ['j', 'k', 'looooong']
        ]
    )
    expected = [
        'looooong  y         z',
        'interjection',
        'a         looooong  c',
        'unicode interjection',
        'j         k         looooong',
    ]
    assert actual == expected


def test_format_table_all_strings():
    actual = utils.format_table(['foo', 'bar', 'baz'])
    expected = ['foo', 'bar', 'baz']
    assert actual == expected


def test_parse_timestamp():
    actual = utils.parse_timestamp('19700101T000000')
    expected = datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)
    assert actual == expected


def test_null_log_writer():
    """Basic smoke test for NullLogWriter"""
    lw = utils.NullLogWriter(driver='null')
    lw.log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')


class TestFileLogWriter:
    def test_smoke(self):
        """Smoke test for FileLogWriter"""
        fw = utils.FileLogWriter('/dev/null')
        fw.log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')

    def test_format_path(self):
        """Test the path formatting for FileLogWriter"""
        fw = utils.FileLogWriter("/logs/{service}/{component}/{level}/{cluster}/{instance}")
        expected = "/logs/a/b/c/d/e"
        assert expected == fw.format_path("a", "b", "c", "d", "e")

    def test_maybe_flock(self):
        """Make sure we flock and unflock when flock=True"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=True)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                mock_fcntl.flock.assert_called_once_with(mock_file, mock_fcntl.LOCK_EX)
                mock_fcntl.flock.reset_mock()

            mock_fcntl.flock.assert_called_once_with(mock_file, mock_fcntl.LOCK_UN)

    def test_maybe_flock_flock_false(self):
        """Make sure we don't flock/unflock when flock=False"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=False)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                assert mock_fcntl.flock.call_count == 0

            assert mock_fcntl.flock.call_count == 0

    def test_log_makes_exactly_one_write_call(self):
        """We want to make sure that log() makes exactly one call to write, since that's how we ensure atomicity."""
        fake_file = mock.Mock()
        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file,
            __exit__=lambda _self, t, v, tb: None
        )

        fake_line = "text" * 1000000

        with mock.patch("paasta_tools.utils.io.FileIO", return_value=fake_contextmgr, autospec=True) as mock_FileIO:
            fw = utils.FileLogWriter("/dev/null", flock=False)

            with mock.patch("paasta_tools.utils.format_log_line", return_value=fake_line, autospec=True) as fake_fll:
                fw.log("service", "line", "component", level="level", cluster="cluster", instance="instance")

            fake_fll.assert_called_once_with("level", "cluster", "service", "instance", "component", "line")

            mock_FileIO.assert_called_once_with("/dev/null", mode=fw.mode, closefd=True)
            fake_file.write.assert_called_once_with("{}\n".format(fake_line).encode('UTF-8'))


def test_deep_merge_dictionaries():
    overrides = {
        'common_key': 'value',
        'common_dict': {
            'subkey1': 1,
            'subkey2': 2,
            'subkey3': 3,
        },
        'just_in_overrides': 'value',
        'just_in_overrides_dict': {'key': 'value'},
        'overwriting_key': 'value',
        'overwriting_dict': {'test': 'value'},
    }
    defaults = {
        'common_key': 'overwritten_value',
        'common_dict': {
            'subkey1': 'overwritten_value',
            'subkey4': 4,
            'subkey5': 5,
        },
        'just_in_defaults': 'value',
        'just_in_defaults_dict': {'key': 'value'},
        'overwriting_key': {'overwritten-key', 'overwritten-value'},
        'overwriting_dict': 'overwritten-value',
    }
    expected = {
        'common_key': 'value',
        'common_dict': {
            'subkey1': 1,
            'subkey2': 2,
            'subkey3': 3,
            'subkey4': 4,
            'subkey5': 5,
        },
        'just_in_overrides': 'value',
        'just_in_overrides_dict': {'key': 'value'},
        'just_in_defaults': 'value',
        'just_in_defaults_dict': {'key': 'value'},
        'overwriting_key': 'value',
        'overwriting_dict': {'test': 'value'},
    }
    assert utils.deep_merge_dictionaries(overrides, defaults) == expected


def test_function_composition():
    def func_one(count):
        return count + 1

    def func_two(count):
        return count + 1

    composed_func = utils.compose(func_one, func_two)
    assert composed_func(0) == 2


def test_is_deploy_step():
    assert utils.is_deploy_step('prod.main')
    assert utils.is_deploy_step('thingy')

    assert not utils.is_deploy_step('itest')
    assert not utils.is_deploy_step('performance-check')
    assert not utils.is_deploy_step('command-thingy')


def test_long_job_id_to_short_job_id():
    assert utils.long_job_id_to_short_job_id('service.instance.git.config') == 'service.instance'


def test_mean():
    iterable = [1.0, 2.0, 3.0]
    assert utils.mean(iterable) == 2.0


def test_prompt_pick_one_happy():
    with mock.patch(
        'paasta_tools.utils.sys.stdin', autospec=True,
    ) as mock_stdin, mock.patch(
        'paasta_tools.utils.choice.Menu', autospec=True,
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value='choiceA'))
        assert utils.prompt_pick_one(['choiceA'], 'test') == 'choiceA'


def test_prompt_pick_one_quit():
    with mock.patch(
        'paasta_tools.utils.sys.stdin', autospec=True,
    ) as mock_stdin, mock.patch(
        'paasta_tools.utils.choice.Menu', autospec=True,
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value=(None, 'quit')))
        with raises(SystemExit):
            utils.prompt_pick_one(['choiceA', 'choiceB'], 'test')


def test_prompt_pick_one_keyboard_interrupt():
    with mock.patch(
        'paasta_tools.utils.sys.stdin', autospec=True,
    ) as mock_stdin, mock.patch(
        'paasta_tools.utils.choice.Menu', autospec=True,
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=KeyboardInterrupt))
        with raises(SystemExit):
            utils.prompt_pick_one(['choiceA', 'choiceB'], 'test')


def test_prompt_pick_one_eoferror():
    with mock.patch(
        'paasta_tools.utils.sys.stdin', autospec=True,
    ) as mock_stdin, mock.patch(
        'paasta_tools.utils.choice.Menu', autospec=True,
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=EOFError))
        with raises(SystemExit):
            utils.prompt_pick_one(['choiceA', 'choiceB'], 'test')


def test_prompt_pick_one_exits_no_tty():
    with mock.patch('paasta_tools.utils.sys.stdin', autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = False
        with raises(SystemExit):
            utils.prompt_pick_one(['choiceA', 'choiceB'], 'test')


def test_prompt_pick_one_exits_no_choices():
    with mock.patch('paasta_tools.utils.sys.stdin', autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = True
        with raises(SystemExit):
            utils.prompt_pick_one([], 'test')


def test_get_code_sha_from_dockerurl():
    fake_docker_url = 'docker-paasta.yelpcorp.com:443/services-cieye:paasta-93340779404579'
    actual = utils.get_code_sha_from_dockerurl(fake_docker_url)
    assert actual == 'git93340779'

    # Useful mostly for integration tests, where we run busybox a lot.
    assert utils.get_code_sha_from_dockerurl('docker.io/busybox') == 'gitbusybox'
