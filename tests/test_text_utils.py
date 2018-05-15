from paasta_tools import text_utils


def test_function_composition():
    def func_one(count):
        return count + 1

    def func_two(count):
        return count + 1

    composed_func = text_utils.compose(func_one, func_two)
    assert composed_func(0) == 2


def test_remove_ansi_escape_sequences():
    plain_string = 'blackandwhite'
    colored_string = '\033[34m' + plain_string + '\033[0m'
    assert text_utils.remove_ansi_escape_sequences(colored_string) == plain_string


def test_color_text():
    expected = "%shi%s" % (text_utils.PaastaColors.RED, text_utils.PaastaColors.DEFAULT)
    actual = text_utils.PaastaColors.color_text(text_utils.PaastaColors.RED, "hi")
    assert actual == expected


def test_color_text_nested():
    expected = "%sred%sblue%sred%s" % (
        text_utils.PaastaColors.RED,
        text_utils.PaastaColors.BLUE,
        text_utils.PaastaColors.DEFAULT + text_utils.PaastaColors.RED,
        text_utils.PaastaColors.DEFAULT,
    )
    actual = text_utils.PaastaColors.color_text(
        text_utils.PaastaColors.RED,
        "red%sred" % text_utils.PaastaColors.blue("blue"),
    )
    assert actual == expected


def test_terminal_len():
    assert len('some text') == text_utils.terminal_len(text_utils.PaastaColors.red('some text'))


def test_format_table():
    actual = text_utils.format_table(
        [
            ['looooong', 'y', 'z'],
            ['a', 'looooong', 'c'],
            ['j', 'k', 'looooong'],
        ],
    )
    expected = [
        'looooong  y         z',
        'a         looooong  c',
        'j         k         looooong',
    ]
    assert actual == expected
    assert ["a     b     c"] == text_utils.format_table([['a', 'b', 'c']], min_spacing=5)


def test_format_table_with_interjected_lines():
    actual = text_utils.format_table(
        [
            ['looooong', 'y', 'z'],
            'interjection',
            ['a', 'looooong', 'c'],
            'unicode interjection',
            ['j', 'k', 'looooong'],
        ],
    )
    expected = [
        'looooong  y         z',
        'interjection',
        'a         looooong  c',
        'unicode interjection',
        'j         k         looooong',
    ]
    assert actual == expected


def test_format_table_all_strings():
    actual = text_utils.format_table(['foo', 'bar', 'baz'])
    expected = ['foo', 'bar', 'baz']
    assert actual == expected
