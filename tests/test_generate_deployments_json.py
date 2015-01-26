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


def test_get_remote_refs_for_service():
    fake_remote_refs = {
        'refs/heads/foo': 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
        'refs/heads/bar': 'c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff',
    }

    with mock.patch('paasta_tools.remote_git.list_remote_refs',
                    return_value=fake_remote_refs):
        actual = generate_deployments_json.get_remote_refs_for_service('srv_fake')
        expected = [
            ('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef', 'foo'),
            ('c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff', 'bar'),
        ]
        assert actual == expected


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
    fake_remotes = [[('123456', 'try_me'), ('ijowarg', 'okay')],
                    [('789009', 'no_thanks'), ('j8yiomwer', 'nah')]]
    fake_registry = 'super-docker'
    fake_old_mappings = ['']
    fake_desired_states = {
        ('uno', 'no_thanks', '789009'): ('start', None),
        ('dos', 'try_me', '123456'): ('stop', '123'),
    }
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
        mock.patch('generate_deployments_json.get_remote_branches_for_service',
                   side_effect=lambda a: fake_remotes.pop()),
        mock.patch('paasta_tools.marathon_tools.get_docker_registry',
                   return_value=fake_registry),
        mock.patch('paasta_tools.marathon_tools.get_docker_url',
                   return_value="not empty"),
        mock.patch('generate_deployments_json.get_desired_state',
                   side_effect=lambda service, branch, sha: fake_desired_states.get((service, branch, sha))),
    ) as (
        get_dirs_patch,
        get_branches_patch,
        get_remotes_patch,
        registry_patch,
        docker_url_patch,
        get_desired_state_patch,
    ):
        actual = generate_deployments_json.get_branch_mappings(fake_soa_dir, fake_old_mappings)
        assert expected == actual
        registry_patch.assert_called_once_with()
        get_dirs_patch.assert_called_once_with(fake_soa_dir)
        get_branches_patch.assert_any_call(fake_soa_dir, 'uno')
        get_branches_patch.assert_any_call(fake_soa_dir, 'dos')
        assert get_branches_patch.call_count == 2
        get_remotes_patch.assert_any_call('uno')
        get_remotes_patch.assert_any_call('dos')
        assert get_remotes_patch.call_count == 2
        docker_url_patch.assert_any_call(fake_registry, 'services-uno:paasta-789009', verify=True)
        docker_url_patch.assert_any_call(fake_registry, 'services-dos:paasta-123456', verify=True)
        assert docker_url_patch.call_count == 2

        get_desired_state_patch.assert_any_call('uno', 'no_thanks', '789009')
        get_desired_state_patch.assert_any_call('dos', 'try_me', '123456')
        assert get_desired_state_patch.call_count == 2


def test_main():
    fake_soa_dir = '/etc/true/null'
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('generate_deployments_json.parse_args',
                   return_value=mock.Mock(verbose=False, soa_dir=fake_soa_dir)),
        mock.patch('os.path.abspath', return_value='ABSOLUTE'),
        mock.patch('generate_deployments_json.get_branch_mappings', return_value={'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}}),
        mock.patch('os.path.join', return_value='JOIN'),
        mock.patch('generate_deployments_json.open', create=True, return_value=file_mock),
        mock.patch('json.dump'),
        mock.patch('json.load', return_value={'OLD_MAP': 'PINGS'})
    ) as (
        parse_patch,
        abspath_patch,
        mappings_patch,
        join_patch,
        open_patch,
        json_dump_patch,
        json_load_patch
    ):
        generate_deployments_json.main()
        parse_patch.assert_called_once_with()
        abspath_patch.assert_called_once_with(fake_soa_dir)
        mappings_patch.assert_called_once_with('ABSOLUTE', {'OLD_MAP': {'desired_state': 'start', 'docker_image': 'PINGS', 'force_bounce': None}}),
        join_patch.assert_any_call('ABSOLUTE', generate_deployments_json.TARGET_FILE),
        assert join_patch.call_count == 2
        open_patch.assert_any_call('JOIN', 'w')
        open_patch.assert_any_call('JOIN', 'r')
        assert open_patch.call_count == 2
        json_dump_patch.assert_called_once_with({'v1': {'MAP': {'docker_image': 'PINGS', 'desired_state': 'start'}}}, file_mock.__enter__())
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


def test_get_remote_tags_for_service():
    fake_remote_refs = {
        'refs/tags/sometag': 'somehash',
        'refs/tags/othertag': 'otherhash',
    }
    fake_srv = 'srv_fake'
    expected = [('somehash', 'sometag'), ('otherhash', 'othertag')]

    with mock.patch('paasta_tools.remote_git.list_remote_refs',
                    return_value=fake_remote_refs):
        with mock.patch('generate_deployments_json.get_git_url', return_value='test_url') as url_patch:
            actual = generate_deployments_json.get_remote_tags_for_service(fake_srv)
            assert set(expected) == set(actual)
            url_patch.assert_called_once_with(fake_srv)


def test_get_desired_state():
    fake_remote_refs = {
        'refs/tags/paasta-prod-1-start': 'somehash',
        'refs/tags/paasta-prod-2-stop': 'somehash',
        'refs/tags/paasta-prod-3-start': 'somehash',
        'refs/tags/paasta-prod-4-start': 'diffhash',
        'refs/tags/paasta-stage-12345-stop': 'othersha',
    }
    fake_srv = 'srv_fake'
    with mock.patch('paasta_tools.remote_git.list_remote_refs',
                    return_value=fake_remote_refs):
        # Make sure that if there are no tags that say otherwise, we assume it should be started.
        assert ('start', None) == generate_deployments_json.get_desired_state(fake_srv, 'branchthatdoesntexist', 'c0ffee')

        # We should get status for a specific version, not other versions.
        assert ('start', '3') == generate_deployments_json.get_desired_state(fake_srv, 'prod', 'somehash')
        assert ('start', '4') == generate_deployments_json.get_desired_state(fake_srv, 'prod', 'diffhash')
        assert ('stop', '12345') == generate_deployments_json.get_desired_state(fake_srv, 'stage', 'othersha')
