# Unit tests for soa_collector.sanitize()
from paasta_tools.diamond_collector.soa_collector import sanitize


def test_spaces():
    assert 'a_b_c_d' == sanitize("a b c d")


def test_dots():
    assert 'a_b_c_d' == sanitize("a.b.c.d")


def test_noop():
    assert 'abcd' == sanitize('abcd')
