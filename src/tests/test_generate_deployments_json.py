import generate_deployments_json
import mock
import contextlib


def test_get_git_url():
    service = 'giiiiiiiiiiit'
    expected = 'git@git.yelpcorp.com:services/%s.git' % service
    assert generate_deployments_json.get_git_url(service) == expected


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


def test_get_remote_branches_for_service():
    test_branches = 'b1\trefs/heads/1111112j34tirg\nb2\trefs/heads/22222223tiome'
    fake_srv = 'srv_fake'
    mygit = mock.Mock(ls_remote=mock.Mock(return_value=test_branches))
    expected = [('b1', '1111112j34tirg'), ('b2', '22222223tiome')]
    with mock.patch('generate_deployments_json.get_git_url', return_value='test_url') as url_patch:
        actual = generate_deployments_json.get_remote_branches_for_service(mygit, fake_srv)
        assert expected == actual
        url_patch.assert_called_once_with(fake_srv)
        mygit.ls_remote.assert_called_once_with('-h', 'test_url')


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
    fake_tmp_dir = '/44/33/22/11'
    fake_dirs = ['uno', 'dos']
    fake_branches = [['try_me'], ['no_thanks']]
    fake_remotes = [[('123456', 'try_me'), ('ijowarg', 'okay')],
                    [('789009', 'no_thanks'), ('j8yiomwer', 'nah')]]
    fake_registry = 'super-docker'
    fake_old_mappings = ['']
    fake_git = mock.Mock()
    expected = {'uno:no_thanks': 'services-uno:paasta-789009',
                'dos:try_me': 'services-dos:paasta-123456'}
    with contextlib.nested(
        mock.patch('tempfile.mkdtemp', return_value=fake_tmp_dir),
        mock.patch('git.Git', return_value=fake_git),
        mock.patch('generate_deployments_json.get_service_directories',
                   return_value=fake_dirs),
        mock.patch('generate_deployments_json.get_branches_for_service',
                   side_effect=lambda a, b: fake_branches.pop()),
        mock.patch('generate_deployments_json.get_remote_branches_for_service',
                   side_effect=lambda a, b: fake_remotes.pop()),
        mock.patch('service_deployment_tools.marathon_tools.get_docker_registry',
                   return_value=fake_registry),
        mock.patch('service_deployment_tools.marathon_tools.get_docker_url',
                   return_value="not empty"),
        mock.patch('os.rmdir')
    ) as (
        mkdir_patch,
        git_patch,
        get_dirs_patch,
        get_branches_patch,
        get_remotes_patch,
        registry_patch,
        docker_url_patch,
        rmdir_patch
    ):
        actual = generate_deployments_json.get_branch_mappings(fake_soa_dir, fake_old_mappings)
        assert expected == actual
        mkdir_patch.assert_called_once_with()
        registry_patch.assert_called_once_with()
        git_patch.assert_called_once_with(fake_tmp_dir)
        get_dirs_patch.assert_called_once_with(fake_soa_dir)
        get_branches_patch.assert_any_call(fake_soa_dir, 'uno')
        get_branches_patch.assert_any_call(fake_soa_dir, 'dos')
        assert get_branches_patch.call_count == 2
        get_remotes_patch.assert_any_call(fake_git, 'uno')
        get_remotes_patch.assert_any_call(fake_git, 'dos')
        assert get_remotes_patch.call_count == 2
        rmdir_patch.assert_called_once_with(fake_tmp_dir)
        docker_url_patch.assert_any_call(fake_registry, 'services-uno:paasta-789009', verify=True)
        docker_url_patch.assert_any_call(fake_registry, 'services-dos:paasta-123456', verify=True)
        assert docker_url_patch.call_count == 2


def test_main():
    fake_soa_dir = '/etc/true/null'
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('generate_deployments_json.parse_args',
                   return_value=mock.Mock(verbose=False, soa_dir=fake_soa_dir)),
        mock.patch('os.path.abspath', return_value='ABSOLUTE'),
        mock.patch('generate_deployments_json.get_branch_mappings', return_value='MAPPINGS'),
        mock.patch('os.path.join', return_value='JOIN'),
        mock.patch('generate_deployments_json.open', create=True, return_value=file_mock),
        mock.patch('json.dump'),
        mock.patch('json.load', return_value='LOAAAAADIN')
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
        mappings_patch.assert_called_once_with('ABSOLUTE', 'LOAAAAADIN'),
        join_patch.assert_any_call('ABSOLUTE', generate_deployments_json.TARGET_FILE),
        assert join_patch.call_count == 2
        open_patch.assert_any_call('JOIN', 'w')
        open_patch.assert_any_call('JOIN', 'r')
        assert open_patch.call_count == 2
        json_dump_patch.assert_called_once_with('MAPPINGS', file_mock.__enter__())
        json_load_patch.assert_called_once_with(file_mock.__enter__())
