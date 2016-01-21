# Copyright 2015 Yelp Inc.
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

import generate_deployments_for_service
import mock
import contextlib
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.chronos_tools import ChronosJobConfig


def test_get_branches_for_service():
    fake_dir = '/mail/var/tea'
    fake_srv = 'boba'
    fake_branches = ['red', 'green', 'blue', 'orange', 'white', 'black']
    expected = set(['red', 'green', 'blue', 'orange', 'white', 'black', 'cluster_a.chronos_job',
                    'cluster_b.chronos_job', 'cluster_c.chronos_job'])
    with contextlib.nested(
        mock.patch('generate_deployments_for_service.list_clusters',
                   return_value=['cluster_a', 'cluster_b', 'cluster_c']),
        mock.patch('generate_deployments_for_service.get_service_instance_list',
                   side_effect=lambda service, cluster, instance_type:
                       [('', 'main_example'), ('', 'canary_example')] if instance_type == 'marathon'
                       else [('', 'chronos_job')]),
        mock.patch('generate_deployments_for_service.load_marathon_service_config',
                   side_effect=lambda service, instance, cluster, soa_dir: MarathonServiceConfig(
                       service=service,
                       cluster=cluster,
                       instance=instance,
                       config_dict={'deploy_group': fake_branches.pop()},
                       branch_dict={},
                   )),
        mock.patch('generate_deployments_for_service.load_chronos_job_config',
                   side_effect=lambda service, instance, cluster, soa_dir: ChronosJobConfig(
                       service=service,
                       cluster=cluster,
                       instance=instance,
                       config_dict={},
                       branch_dict={},
                   )),
    ) as (
        list_clusters_patch,
        get_service_instance_list_patch,
        load_marathon_config_patch,
        load_chronos_config_patch,
    ):
        actual = generate_deployments_for_service.get_branches_for_service(fake_dir, fake_srv)
        assert expected == actual
        # three clusters each having a canary and main marathon instance equals six instances total
        assert load_marathon_config_patch.call_count == 6
        # three clusters each having one chronos job equals six instances total
        assert load_chronos_config_patch.call_count == 3


def test_get_branch_mappings():
    fake_service = 'fake_service'
    fake_soa_dir = '/no/yes/maybe'
    fake_branches = ['try_me', 'no_thanks']

    fake_remote_refs = {
        'refs/heads/try_me': '123456',
        'refs/tags/paasta-try_me-123-stop': '123456',
        'refs/heads/okay': 'ijowarg',
        'refs/heads/no_thanks': '789009',
        'refs/heads/nah': 'j8yiomwer',
    }

    fake_old_mappings = ['']
    expected = {
        'fake_service:no_thanks': {
            'docker_image': 'services-fake_service:paasta-789009',
            'desired_state': 'start',
            'force_bounce': None,
        },
        'fake_service:try_me': {
            'docker_image': 'services-fake_service:paasta-123456',
            'desired_state': 'stop',
            'force_bounce': '123',
        },
    }
    with contextlib.nested(
        mock.patch('generate_deployments_for_service.get_branches_for_service',
                   return_value=fake_branches),
        mock.patch('paasta_tools.remote_git.list_remote_refs',
                   return_value=fake_remote_refs),
    ) as (
        get_branches_patch,
        list_remote_refs_patch,
    ):
        actual = generate_deployments_for_service.get_branch_mappings(fake_soa_dir, fake_service, fake_old_mappings)
        print actual
        print expected
        assert expected == actual
        get_branches_patch.assert_any_call(fake_soa_dir, fake_service)
        assert get_branches_patch.call_count == 1
        assert list_remote_refs_patch.call_count == 1


def test_get_service_from_docker_image():
    mock_image = ('docker-paasta.yelpcorp.com:443/'
                  'services-example_service:paasta-591ae8a7b3224e3b3322370b858377dd6ef335b6')
    actual = generate_deployments_for_service.get_service_from_docker_image(mock_image)
    assert 'example_service' == actual


def test_main():
    fake_soa_dir = '/etc/true/null'
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('generate_deployments_for_service.parse_args',
                   return_value=mock.Mock(verbose=False, soa_dir=fake_soa_dir, service='fake_service'),
                   autospec=True),
        mock.patch('os.path.abspath', return_value='ABSOLUTE', autospec=True),
        mock.patch(
            'generate_deployments_for_service.get_branch_mappings',
            return_value={'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}},
            autospec=True,
        ),
        mock.patch('os.path.join', return_value='JOIN', autospec=True),
        mock.patch('generate_deployments_for_service.open', create=True, return_value=file_mock),
        mock.patch('json.dump', autospec=True),
        mock.patch('json.load', return_value={'OLD_MAP': 'PINGS'}, autospec=True),
        mock.patch('generate_deployments_for_service.atomic_file_write', autospec=True),
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
            'ABSOLUTE',
            'fake_service',
            {'OLD_MAP': {'desired_state': 'start', 'docker_image': 'PINGS', 'force_bounce': None}},
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

    assert generate_deployments_for_service.get_deployments_dict_from_branch_mappings(branch_mappings) == {
        'v1': branch_mappings,
    }
