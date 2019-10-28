from clusterman.common.sfx import _make_filter_string


def test_make_filter_string():
    assert _make_filter_string([('foo', 'bar'), ('fizz', 'buzz')]) == 'filter("foo", "bar") and filter("fizz", "buzz")'
