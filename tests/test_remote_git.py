from paasta_tools import remote_git


def test_make_determine_wants_func():
    refs = {
        'refs/heads/foo': 'abcde',
        'refs/tags/blah': '12345',
    }
    # nothing changed, so nothing should change
    determine_wants = remote_git._make_determine_wants_func(lambda x: x)
    assert determine_wants(refs) == refs

    # don't delete anything.
    determine_wants = remote_git._make_determine_wants_func(lambda x: {})
    assert determine_wants(refs) == refs

    # don't modify anything existing.
    determine_wants = remote_git._make_determine_wants_func(
        lambda x: dict((k, v[::-1]) for k, v in x.items())
    )
    assert determine_wants(refs) == refs

    # only allow new things
    determine_wants = remote_git._make_determine_wants_func(
        lambda x: {'foo': 'bar'}
    )
    actual = determine_wants(refs)
    expected = dict(refs.items() + [('foo', 'bar')])
    assert actual == expected
