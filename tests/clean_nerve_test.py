import mock

from paasta_tools import clean_nerve


def test_parse_args():
    mock_argv = ['clean_nerve']
    with mock.patch('sys.argv', mock_argv):
        args = clean_nerve.parse_args()

    assert not args.simulate
    assert args.cluster_type == 'infrastructure'


def test_parse_args_simulate():
    mock_argv = ['clean_nerve', '--simulate']
    with mock.patch('sys.argv', mock_argv):
        args = clean_nerve.parse_args()

    assert args.simulate


def test_parse_args_cluster_type():
    mock_argv = ['clean_nerve', '--cluster-type', 'bar']
    with mock.patch('sys.argv', mock_argv):
        args = clean_nerve.parse_args()

    assert args.cluster_type == 'bar'


def create_mock_zk():
    """Create a mock ZK Kazoo client."""

    def mock_get_children(path):
        if path == '/nerve':
            return ['region:alpha', 'region:beta']
        if path == '/nerve/region:alpha':
            return ['foo', 'bar']
        if path == '/nerve/region:alpha/foo':
            return ['good_service_node', 'orphaned_service_node']
        if path == '/nerve/region:alpha/bar':
            return []
        if path == '/nerve/region:beta':
            return []
        raise ValueError('Unexpected path: %s' % path)

    def mock_get(path):
        if path == '/nerve/region:alpha/foo/good_service_node':
            # This is a regular service instance node that should be kept
            return ('some data', mock.Mock(ephemeralOwner=1, numChildren=0))
        if path == '/nerve/region:alpha/foo/orphaned_service_node':
            # This is an orphaned node that should be removed
            return ('', mock.Mock(ephemeralOwner=0, numChildren=0))
        raise ValueError('Unexpected path: %s' % path)

    mock_zk = mock.Mock()
    mock_zk.get_children.side_effect = mock_get_children
    mock_zk.get.side_effect = mock_get
    return mock_zk


def test_clean():
    mock_zk = create_mock_zk()
    assert clean_nerve.clean(simulate=False, zk=mock_zk) == 1
    expected_calls = [mock.call('/nerve/region:alpha/foo/orphaned_service_node')]
    mock_zk.delete.assert_has_calls(expected_calls)


def test_clean_simulate():
    mock_zk = create_mock_zk()
    assert clean_nerve.clean(simulate=True, zk=mock_zk) == 1
    mock_zk.delete.assert_has_calls([])


@mock.patch('paasta_tools.clean_nerve.glob.glob')
@mock.patch('paasta_tools.clean_nerve.os.path.islink')
def test_get_zk_cluster_locations(mock_islink, mock_glob):
    mock_glob.return_value = [
        '/nail/etc/zookeeper_discovery/ty/foo.yaml',
        '/nail/etc/zookeeper_discovery/ty/bar.yaml',
    ]

    # bar is a symlink, so should not be returned
    mock_islink.side_effect = \
        lambda path: path == '/nail/etc/zookeeper_discovery/ty/bar.yaml'

    result = clean_nerve.get_zk_cluster_locations('ty')
    assert list(result) == ['foo']
    mock_glob.assert_has_calls([
        mock.call('/nail/etc/zookeeper_discovery/ty/*.yaml')])
