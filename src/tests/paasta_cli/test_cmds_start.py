import mock
from paasta_tools.paasta_cli.cmds import start
from pytest import raises


def test_match_branches():
    # simple case
    assert start.match_branches(['foo'], ['foo']) == set(['foo'])

    assert start.match_branches(
        ['foo', 'bar', 'baz'],
        ['b*']
    ) == set(['bar', 'baz'])

    with raises(start.NoBranchesMatchException):
        start.match_branches(['foo'], ['bar'])

    with raises(start.NoBranchesMatchException):
        start.match_branches(['foo'], ['foo', 'bar'])


def test_format_tag():
    expected = 'refs/tags/paasta-BRANCHNAME-TIMESTAMP-stop'
    actual = start.format_tag(
        branch='BRANCHNAME',
        force_bounce='TIMESTAMP',
        desired_state='stop'
    )
    assert actual == expected


@mock.patch('paasta_tools.utils.get_git_url', autospec=True)
@mock.patch('dulwich.client.get_transport_and_path', autospec=True)
def test_issue_start_for_branches(get_transport_and_path, get_git_url):
    fake_git_url = 'BLOORGRGRGRGR'
    fake_path = 'somepath'

    get_git_url.return_value = fake_git_url

    mock_git_client = mock.Mock()
    get_transport_and_path.return_value = (mock_git_client, fake_path)

    start.issue_start_for_branches('fake_service', ['branch1', 'branch2'], '0')

    get_transport_and_path.assert_called_once_with(fake_git_url)
    mock_git_client.send_pack.assert_called_once_with(fake_path, mock.ANY,
                                                      mock.ANY)


def test_make_mutate_refs_func():

    mutate_refs = start.make_mutate_refs_func(
        branches=['a', 'b'],
        force_bounce='FORCEBOUNCE',
        desired_state='stop',
    )

    old_refs = {
        'refs/heads/a': 'hash_for_a',
        'refs/heads/b': 'hash_for_b',
        'refs/heads/c': 'hash_for_c',
        'refs/heads/d': 'hash_for_d',
    }

    expected = dict(old_refs)
    expected.update({
        'refs/tags/paasta-a-FORCEBOUNCE-stop': 'hash_for_a',
        'refs/tags/paasta-b-FORCEBOUNCE-stop': 'hash_for_b',
    })

    actual = mutate_refs(old_refs)
    assert actual == expected
