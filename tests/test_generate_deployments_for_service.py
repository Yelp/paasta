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
import contextlib

import mock

from paasta_tools import generate_deployments_for_service
from paasta_tools.marathon_tools import MarathonServiceConfig


def test_get_deploy_group_mappings():
    fake_service = 'fake_service'
    fake_soa_dir = '/no/yes/maybe'

    fake_service_configs = [
        MarathonServiceConfig(
            service=fake_service,
            cluster='clusterA',
            instance='main',
            branch_dict={},
            config_dict={'deploy_group': 'no_thanks'},
        ),
        MarathonServiceConfig(
            service=fake_service,
            cluster='clusterB',
            instance='main',
            branch_dict={},
            config_dict={'deploy_group': 'try_me'},
        ),
    ]

    fake_remote_refs = {
        'refs/tags/paasta-try_me-20160308T053933-deploy': '123456',
        'refs/tags/paasta-clusterB.main-123-stop': '123456',
        'refs/tags/paasta-okay-20160308T053933-deploy': 'ijowarg',
        'refs/tags/paasta-no_thanks-20160308T053933-deploy': '789009',
        'refs/tags/paasta-nah-20160308T053933-deploy': 'j8yiomwer',
    }

    fake_old_mappings = ['']
    expected = {
        'fake_service:paasta-clusterA.main': {
            'docker_image': 'services-fake_service:paasta-789009',
            'desired_state': 'start',
            'force_bounce': None,
        },
        'fake_service:paasta-clusterB.main': {
            'docker_image': 'services-fake_service:paasta-123456',
            'desired_state': 'stop',
            'force_bounce': '123',
        },
    }
    with contextlib.nested(
        mock.patch('paasta_tools.generate_deployments_for_service.get_instance_config_for_service',
                   return_value=fake_service_configs),
        mock.patch('paasta_tools.remote_git.list_remote_refs',
                   return_value=fake_remote_refs),
    ) as (
        get_instance_config_for_service_patch,
        list_remote_refs_patch,
    ):
        actual = generate_deployments_for_service.get_deploy_group_mappings(fake_soa_dir,
                                                                            fake_service, fake_old_mappings)
        get_instance_config_for_service_patch.assert_called_once_with(soa_dir=fake_soa_dir, service=fake_service)
        assert list_remote_refs_patch.call_count == 1
        assert expected == actual


def test_get_cluster_instance_map_for_service():
    fake_service_configs = [
        MarathonServiceConfig(
            service='service1',
            cluster='clusterA',
            instance='main',
            branch_dict={},
            config_dict={'deploy_group': 'no_thanks'},
        ),
        MarathonServiceConfig(
            service='service1',
            cluster='clusterB',
            instance='main',
            branch_dict={},
            config_dict={'deploy_group': 'try_me'},
        ),
        MarathonServiceConfig(
            service='service1',
            cluster='clusterA',
            instance='canary',
            branch_dict={},
            config_dict={'deploy_group': 'try_me'},
        ),
    ]
    with contextlib.nested(
        mock.patch('paasta_tools.generate_deployments_for_service.get_instance_config_for_service',
                   return_value=fake_service_configs),
    ) as (
        mock_get_instance_config_for_service,
    ):
        ret = generate_deployments_for_service.get_cluster_instance_map_for_service('/nail/blah', 'service1', 'try_me')
        mock_get_instance_config_for_service.assert_called_with('/nail/blah', 'service1')
        expected = {'clusterA': {'instances': ['canary']}, 'clusterB': {'instances': ['main']}}
        assert ret == expected

        ret = generate_deployments_for_service.get_cluster_instance_map_for_service('/nail/blah', 'service1')
        expected = {'clusterA': {'instances': ['main', 'canary']}, 'clusterB': {'instances': ['main']}}
        assert ret == expected


def test_get_service_from_docker_image():
    mock_image = ('docker-paasta.yelpcorp.com:443/'
                  'services-example_service:paasta-591ae8a7b3224e3b3322370b858377dd6ef335b6')
    actual = generate_deployments_for_service.get_service_from_docker_image(mock_image)
    assert 'example_service' == actual


def test_main():
    fake_soa_dir = '/etc/true/null'
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('paasta_tools.generate_deployments_for_service.parse_args',
                   return_value=mock.Mock(verbose=False, soa_dir=fake_soa_dir, service='fake_service'),
                   autospec=True),
        mock.patch('os.path.abspath', return_value='ABSOLUTE', autospec=True),
        mock.patch(
            'paasta_tools.generate_deployments_for_service.get_deploy_group_mappings',
            return_value={'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}},
            autospec=True,
        ),
        mock.patch('os.path.join', return_value='JOIN', autospec=True),
        mock.patch('paasta_tools.generate_deployments_for_service.open', create=True, return_value=file_mock),
        mock.patch('json.dump', autospec=True),
        mock.patch('json.load', return_value={'OLD_MAP': 'PINGS'}, autospec=True),
        mock.patch('paasta_tools.generate_deployments_for_service.atomic_file_write', autospec=True),
    ) as (
        parse_patch,
        abspath_patch,
        mappings_patch,
        join_patch,
        open_patch,
        json_dump_patch,
        json_load_patch,
        atomic_file_write_patch,
    ):
        generate_deployments_for_service.main()
        parse_patch.assert_called_once_with()
        abspath_patch.assert_called_once_with(fake_soa_dir)
        mappings_patch.assert_called_once_with(
            soa_dir='ABSOLUTE',
            service='fake_service',
            old_mappings={'OLD_MAP': {'desired_state': 'start', 'docker_image': 'PINGS', 'force_bounce': None}},
        ),

        join_patch.assert_any_call('ABSOLUTE', 'fake_service', generate_deployments_for_service.TARGET_FILE),
        assert join_patch.call_count == 2

        atomic_file_write_patch.assert_called_once_with('JOIN')
        open_patch.assert_called_once_with('JOIN', 'r')
        json_dump_patch.assert_called_once_with(
            {
                'v1': {
                    'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}
                }
            },
            atomic_file_write_patch().__enter__()
        )
        json_load_patch.assert_called_once_with(file_mock.__enter__())


def test_get_deployments_dict():
    branch_mappings = {
        'app1': {
            'docker_image': 'image1',
            'desired_state': 'start',
            'force_bounce': '1418951213',
        },
        'app2': {
            'docker_image': 'image2',
            'desired_state': 'stop',
            'force_bounce': '1412345678',
        },
    }

    assert generate_deployments_for_service.get_deployments_dict_from_deploy_group_mappings(branch_mappings) == {
        'v1': branch_mappings,
    }


def test_get_desired_state_understands_tags():
    remote_refs = {
        'refs/heads/master': '7894E99E6805E9DC8C1D8EB26229E3E2243878C9',
        'refs/remotes/origin/HEAD': 'EE8796C4E4295B7D4087E3EB73662B99218DAD94',
        'refs/remotes/origin/master': '5F7C10B320A4EDBC4773C5FEFB1CD7B7A84FCB69',
        'refs/tags/paasta-paasta-cluster.instance-20150721T183905-start': '4EF01B5A574B519AB546309E89F72972A33B6B75',
        'refs/tags/paasta-paasta-cluster.instance-20151106T233211-stop': 'A5AB2A012DC238D4F6DD269C40A4BD3A99D52B1F',
        'refs/tags/paasta-cluster.instance-20160202T233805-start': 'BE68473F98F619F26FD7824B8F56F9A7ABAEB860',
        'refs/tags/paasta-cluster2.someinstance-20160202T233805-start': 'D6B9A0F86DC54A132FBB7747460F53F48C9AEEAD',
        'refs/tags/paasta-cluster2.someinstance-20160205T182601-stop': '9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711',
        'refs/tags/paasta-cluster.instance-20160308T053933-deploy': '4EF01B5A574B519AB546309E89F72972A33B6B75',
        'refs/tags/paasta-cluster2.someinstance-20160308T053933-deploy': '9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711',
    }
    branch = 'cluster2.someinstance'
    deploy_group = branch
    expected_desired_state = ('stop', '20160205T182601')
    actual = generate_deployments_for_service.get_desired_state(branch, remote_refs, deploy_group)

    assert actual == expected_desired_state


def test_get_desired_state_fails_gracefully_with_start():
    remote_refs = {
        'refs/heads/master': '7894E99E6805E9DC8C1D8EB26229E3E2243878C9',
        'refs/remotes/origin/HEAD': 'EE8796C4E4295B7D4087E3EB73662B99218DAD94',
        'refs/remotes/origin/master': '5F7C10B320A4EDBC4773C5FEFB1CD7B7A84FCB69',
        'refs/heads/paasta-cluster.instance': '4EF01B5A574B519AB546309E89F72972A33B6B75',
        'refs/heads/paasta-cluster2.someinstance': '9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711',
    }
    branch = 'cluster.instance'
    deploy_group = branch
    expected_desired_state = ('start', None)
    actual = generate_deployments_for_service.get_desired_state(branch, remote_refs, deploy_group)

    assert actual == expected_desired_state
