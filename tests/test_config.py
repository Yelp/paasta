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

import mock
import pytest
from pytest import raises

from paasta_tools import config
from paasta_tools import utils


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
    fake_config = utils.SystemPaastaConfig(
        {
            'cluster': 'peanut',
        }, '/some/fake/dir',
    )
    expected = 'peanut'
    actual = fake_config.get_cluster()
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_cluster()


def test_SystemPaastaConfig_get_volumes():
    fake_config = utils.SystemPaastaConfig(
        {
            'volumes': [{'fake_path': "fake_other_path"}],
        }, '/some/fake/dir',
    )
    expected = [{'fake_path': "fake_other_path"}]
    actual = fake_config.get_volumes()
    assert actual == expected


def test_SystemPaastaConfig_get_volumes_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_volumes()


def test_SystemPaastaConfig_get_zk():
    fake_config = utils.SystemPaastaConfig(
        {
            'zookeeper': 'zk://fake_zookeeper_host',
        }, '/some/fake/dir',
    )
    expected = 'fake_zookeeper_host'
    actual = fake_config.get_zk_hosts()
    assert actual == expected


def test_SystemPaastaConfig_get_zk_dne():
    fake_config = utils.SystemPaastaConfig({}, '/some/fake/dir')
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_zk_hosts()


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


class TestInstanceConfig:

    def test_get_monitoring(self):
        fake_info = 'fake_info'
        assert config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'monitoring': fake_info},
            branch_dict={},
        ).get_monitoring() == fake_info

    def test_get_cpus_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cpus': -5},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == -5

    def test_get_cpus_in_config_float(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cpus': .66},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == .66

    def test_get_cpus_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_cpus() == .25

    def test_get_mem_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'mem': -999},
            branch_dict={},
        )
        assert fake_conf.get_mem() == -999

    def test_get_mem_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_mem() == 1024

    def test_zero_cpu_burst(self):
        fake_conf = config.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'cpu_burst_pct': 0, 'cpus': 1},
            branch_dict={},
        )
        assert fake_conf.get_cpu_quota() == 100000

    def test_format_docker_parameters_default(self):
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'cpu_burst_pct': 100, 'cpus': 1},
            branch_dict={},
        )
        assert fake_conf.get_cpu_quota() == 200000

    def test_get_mem_swap_int(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'mem': 50,
            },
            branch_dict={},
        )
        assert fake_conf.get_mem_swap() == "50m"

    def test_get_mem_swap_float_rounds_up(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'mem': 50.4,
            },
            branch_dict={},
        )
        assert fake_conf.get_mem_swap() == "51m"

    def test_get_disk_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'disk': -999},
            branch_dict={},
        )
        assert fake_conf.get_disk() == -999

    def test_get_disk_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_disk() == 1024

    def test_get_ulimit_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={
                'ulimit': {
                    'nofile': {'soft': 1024, 'hard': 2048},
                    'nice': {'soft': 20},
                },
            },
            branch_dict={},
        )
        assert list(fake_conf.get_ulimit()) == [
            {"key": "ulimit", "value": "nice=20"},
            {"key": "ulimit", "value": "nofile=1024:2048"},
        ]

    def test_get_ulimit_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert list(fake_conf.get_ulimit()) == []

    def test_get_cap_add_in_config(self):
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={},
            branch_dict={},
        )
        assert list(fake_conf.get_cap_add()) == []

    def test_deploy_group_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='fake_instance',
            cluster='fake_cluster',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'fake_cluster.fake_instance'

    def test_deploy_group_if_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='',
            config_dict={'deploy_group': 'fake_deploy_group'},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'fake_deploy_group'

    def test_deploy_group_string_interpolation(self):
        fake_conf = config.InstanceConfig(
            service='',
            instance='',
            cluster='fake_cluster',
            config_dict={'deploy_group': 'cluster_is_{cluster}'},
            branch_dict={},
        )
        assert fake_conf.get_deploy_group() == 'cluster_is_fake_cluster'

    def test_get_cmd_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_cmd() is None

    def test_get_cmd_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cmd': 'FAKECMD'},
            branch_dict={},
        )
        assert fake_conf.get_cmd() == 'FAKECMD'

    def test_get_env_default(self):
        fake_conf = config.InstanceConfig(
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
            'PAASTA_DEPLOY_GROUP': 'fake_cluster.fake_instance',
            'PAASTA_DOCKER_IMAGE': '',
        }

    def test_get_env_with_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'env': {'SPECIAL_ENV': 'TRUE'}, 'deploy_group': 'fake_deploy_group'},
            branch_dict={'docker_image': 'something', 'deploy_group': 'nothing'},
        )
        assert fake_conf.get_env() == {
            'SPECIAL_ENV': 'TRUE',
            'PAASTA_SERVICE': '',
            'PAASTA_INSTANCE': '',
            'PAASTA_CLUSTER': '',
            'PAASTA_DEPLOY_GROUP': 'fake_deploy_group',
            'PAASTA_DOCKER_IMAGE': 'something',
        }

    def test_get_args_default_no_cmd(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_args() == []

    def test_get_args_default_with_cmd(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'cmd': 'FAKECMD'},
            branch_dict={},
        )
        assert fake_conf.get_args() is None

    def test_get_args_in_config(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'args': ['arg1', 'arg2']},
            branch_dict={},
        )
        assert fake_conf.get_args() == ['arg1', 'arg2']

    def test_get_args_in_config_with_cmd(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'args': ['A'], 'cmd': 'C'},
            branch_dict={},
        )
        fake_conf.get_cmd()
        with raises(config.InvalidInstanceConfig):
            fake_conf.get_args()

    def test_get_force_bounce(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'force_bounce': 'blurp'},
        )
        assert fake_conf.get_force_bounce() == 'blurp'

    def test_get_desired_state(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'desired_state': 'stop'},
        )
        assert fake_conf.get_desired_state() == 'stop'

    def test_monitoring_blacklist_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_monitoring_blacklist(system_deploy_blacklist=[]) == []

    def test_monitoring_blacklist_defaults_to_deploy_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'deploy_blacklist': fake_deploy_blacklist},
            branch_dict={},
        )
        assert fake_conf.get_monitoring_blacklist(system_deploy_blacklist=[]) == fake_deploy_blacklist

    def test_deploy_blacklist_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_deploy_blacklist(system_deploy_blacklist=[]) == []

    def test_deploy_blacklist_reads_blacklist(self):
        fake_deploy_blacklist = [["region", "fake_region"]]
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'deploy_blacklist': fake_deploy_blacklist},
            branch_dict={},
        )
        assert fake_conf.get_deploy_blacklist(system_deploy_blacklist=[]) == fake_deploy_blacklist

    def test_extra_volumes_default(self):
        fake_conf = config.InstanceConfig(
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
                "mode": "RO",
            },
        ]
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'extra_volumes': fake_extra_volumes},
            branch_dict={},
        )
        assert fake_conf.get_extra_volumes() == fake_extra_volumes

    def test_get_pool(self):
        pool = "poolname"
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'pool': pool},
            branch_dict={},
        )
        assert fake_conf.get_pool() == pool

    def test_get_pool_default(self):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_pool() == 'default'

    def test_get_volumes_dedupes_correctly_when_mode_differs_last_wins(self):
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
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
        fake_conf = config.InstanceConfig(
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

    def test_get_docker_url_no_error(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"

        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={},
        )

        with mock.patch(
            'paasta_tools.config.InstanceConfig.get_docker_registry', autospec=True,
            return_value=fake_registry,
        ), mock.patch(
            'paasta_tools.config.InstanceConfig.get_docker_image', autospec=True,
            return_value=fake_image,
        ):
            expected_url = "%s/%s" % (fake_registry, fake_image)
            assert fake_conf.get_docker_url() == expected_url

    @pytest.mark.parametrize(
        ('dependencies_reference', 'dependencies', 'expected'), [
            (None, None, None),
            ('aaa', None, None),
            ('aaa', {}, None),
            ('aaa', {"aaa": [{"foo": "bar"}]}, {"foo": "bar"}),
            ('aaa', {"bbb": [{"foo": "bar"}]}, None),
        ],
    )
    def test_get_dependencies(self, dependencies_reference, dependencies, expected):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'dependencies_reference': dependencies_reference,
                'dependencies': dependencies,
            },
            branch_dict={},
        )
        fake_conf.get_dependencies() == expected

    @pytest.mark.parametrize(
        ('security', 'expected'), [
            ({}, None),
            (None, None),
            ({"outbound_firewall": "monitor"}, 'monitor'),
            ({"outbound_firewall": "foo"}, 'foo'),
        ],
    )
    def test_get_outbound_firewall(self, security, expected):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'security': security},
            branch_dict={},
        )
        fake_conf.get_outbound_firewall() == expected

    @pytest.mark.parametrize(
        ('security', 'expected'), [
            ({}, (True, '')),
            ({"outbound_firewall": "monitor"}, (True, '')),
            ({"outbound_firewall": "block"}, (True, '')),
            ({"outbound_firewall": "foo"}, (False, 'Unrecognized outbound_firewall value "foo"')),
            (
                {"outbound_firewall": "monitor", "foo": 1},
                (False, 'Unrecognized items in security dict of service config: "foo"'),
            ),
        ],
    )
    def test_check_security(self, security, expected):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'security': security},
            branch_dict={},
        )
        assert fake_conf.check_security() == expected

    @pytest.mark.parametrize(
        ('dependencies_reference', 'dependencies', 'expected'), [
            (None, None, (True, '')),
            ('aaa', {"aaa": []}, (True, '')),
            ('aaa', None, (False, 'dependencies_reference "aaa" declared but no dependencies found')),
            ('aaa', {"bbb": []}, (False, 'dependencies_reference "aaa" not found in dependencies dictionary')),
        ],
    )
    def test_check_dependencies_reference(self, dependencies_reference, dependencies, expected):
        fake_conf = config.InstanceConfig(
            service='',
            cluster='',
            instance='',
            config_dict={
                'dependencies_reference': dependencies_reference,
                'dependencies': dependencies,
            },
            branch_dict={},
        )
        assert fake_conf.check_dependencies_reference() == expected
