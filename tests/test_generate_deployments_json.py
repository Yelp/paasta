import generate_deployments_json
import mock
import contextlib


def test_get_branches_from_marathon_file():
    fake_dir = '/nail/etc/soa/dir'
    fake_fname = 'marathon-boston-devc-with-extra-dashes.yaml'
    fake_config = {'test1': {'branch': 'ranch'}, 'test2': {}}
    expected = set(['ranch', 'paasta-boston-devc-with-extra-dashes.test2'])
    with contextlib.nested(
        mock.patch('service_configuration_lib.read_service_information',
                   return_value=fake_config),
        mock.patch('os.path.join', return_value='internal_operations')
    ) as (
        read_info_patch,
        join_patch
    ):
        actual = generate_deployments_json.get_branches_from_marathon_file(fake_dir, fake_fname)
        assert expected == actual
        join_patch.assert_called_once_with(fake_dir, fake_fname)
        read_info_patch.assert_called_once_with('internal_operations')


def test_get_branches_for_service():
    fake_dir = '/mail/var/tea'
    fake_srv = 'boba'
    fake_fnames = ['marathon-quail', 'marathon-trac']
    fake_branches = [set(['red', 'green']), set(['blue', 'orange'])]
    expected = set(['red', 'green', 'blue', 'orange'])
    with contextlib.nested(
        mock.patch('os.path.join', return_value='no_sir'),
        mock.patch('os.listdir', return_value=fake_fnames),
        mock.patch('generate_deployments_json.get_branches_from_marathon_file',
                   side_effect=lambda a, b: fake_branches.pop())
    ) as (
        join_patch,
        listdir_patch,
        get_branches_patch
    ):
        actual = generate_deployments_json.get_branches_for_service(fake_dir, fake_srv)
        assert expected == actual
        join_patch.assert_called_once_with(fake_dir, fake_srv)
        listdir_patch.assert_called_once_with('no_sir')
        get_branches_patch.assert_any_call('no_sir', 'marathon-quail')
        get_branches_patch.assert_any_call('no_sir', 'marathon-trac')
        assert get_branches_patch.call_count == 2


def test_get_service_directories():
    fake_dir = '/red/version'
    fake_dir_entities = ['z', 'tier1', 't3', 'racks']
    expected = sorted(fake_dir_entities)
    fake_triple = (fake_dir, fake_dir_entities, 'nuuuuuuuuuu')
    fake_generator = mock.Mock(next=mock.Mock(return_value=fake_triple))
    with mock.patch('os.walk', return_value=fake_generator) as walk_patch:
        actual = generate_deployments_json.get_service_directories(fake_dir)
        assert expected == actual
        walk_patch.assert_called_once_with(fake_dir)
        fake_generator.next.assert_called_once_with()


def test_get_branch_mappings():
    fake_soa_dir = '/no/yes/maybe'
    fake_dirs = ['uno', 'dos']
    fake_branches = [['try_me'], ['no_thanks']]

    fake_remote_refs = [
        {
            'refs/heads/try_me': '123456',
            'refs/tags/paasta-try_me-123-stop': '123456',
            'refs/heads/okay': 'ijowarg',
        },
        {
            'refs/heads/no_thanks': '789009',
            'refs/heads/nah': 'j8yiomwer',
            # no start or stop tag.
        },
    ]

    fake_registry = 'super-docker'
    fake_old_mappings = ['']
    expected = {
        'uno:no_thanks': {
            'docker_image': 'services-uno:paasta-789009',
            'desired_state': 'start',
            'force_bounce': None,
        },
        'dos:try_me': {
            'docker_image': 'services-dos:paasta-123456',
            'desired_state': 'stop',
            'force_bounce': '123',
        },
    }
    with contextlib.nested(
        mock.patch('generate_deployments_json.get_service_directories',
                   return_value=fake_dirs),
        mock.patch('generate_deployments_json.get_branches_for_service',
                   side_effect=lambda a, b: fake_branches.pop()),
        mock.patch('paasta_tools.remote_git.list_remote_refs',
                   side_effect=lambda a: fake_remote_refs.pop()),
        mock.patch('paasta_tools.marathon_tools.get_docker_registry',
                   return_value=fake_registry),
        mock.patch('paasta_tools.marathon_tools.verify_docker_image',
                   return_value=True, autospec=True),
    ) as (
        get_dirs_patch,
        get_branches_patch,
        list_remote_refs_patch,
        registry_patch,
        verify_docker_image_patch
    ):
        actual = generate_deployments_json.get_branch_mappings(fake_soa_dir, fake_old_mappings)
        assert expected == actual
        registry_patch.assert_called_once_with()
        get_dirs_patch.assert_called_once_with(fake_soa_dir)
        get_branches_patch.assert_any_call(fake_soa_dir, 'uno')
        get_branches_patch.assert_any_call(fake_soa_dir, 'dos')
        assert get_branches_patch.call_count == 2

        # Each service should require exactly one call to list_remote_refs.
        assert list_remote_refs_patch.call_count == 2

        verify_docker_image_patch.assert_any_call(fake_registry, 'services-uno:paasta-789009')
        verify_docker_image_patch.assert_any_call(fake_registry, 'services-dos:paasta-123456')
        assert verify_docker_image_patch.call_count == 2


def test_main():
    fake_soa_dir = '/etc/true/null'
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('generate_deployments_json.parse_args',
                   return_value=mock.Mock(verbose=False, soa_dir=fake_soa_dir), autospec=True),
        mock.patch('os.path.abspath', return_value='ABSOLUTE', autospec=True),
        mock.patch('generate_deployments_json.get_branch_mappings', return_value={'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}}, autospec=True),
        mock.patch('os.path.join', return_value='JOIN', autospec=True),
        mock.patch('generate_deployments_json.open', create=True, return_value=file_mock),
        mock.patch('json.dump', autospec=True),
        mock.patch('json.load', return_value={'OLD_MAP': 'PINGS'}, autospec=True),
        mock.patch('generate_deployments_json.atomic_file_write', autospec=True),
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
        generate_deployments_json.main()
        parse_patch.assert_called_once_with()
        abspath_patch.assert_called_once_with(fake_soa_dir)
        mappings_patch.assert_called_once_with(
            'ABSOLUTE',
            {
                'OLD_MAP': {
                    'desired_state': 'start',
                    'docker_image': 'PINGS',
                    'force_bounce': None
                }
            }
        ),
        join_patch.assert_any_call(
            'ABSOLUTE',
            generate_deployments_json.TARGET_FILE
        ),
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

    assert generate_deployments_json.get_deployments_dict_from_branch_mappings(branch_mappings) == {
        'v1': branch_mappings,
    }
