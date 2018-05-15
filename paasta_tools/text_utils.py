import re
import sys
from typing import Any
from typing import Callable
from typing import cast
from typing import Iterable
from typing import List
from typing import Sequence
from typing import TypeVar
from typing import Union


no_escape = re.compile('\x1B\[[0-9;]*[mK]')


def remove_ansi_escape_sequences(line: str) -> str:
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub('', line)


def terminal_len(text: str) -> int:
    """Return the number of characters that text will take up on a terminal. """
    return len(remove_ansi_escape_sequences(text))


TableRow = Union[str, Sequence[str]]


def format_table(rows: Iterable[TableRow], min_spacing: int=2) -> List[str]:
    """Formats a table for use on the command line.

    :param rows: List of rows, each of which can either be a tuple of strings containing the row's values, or a string
                 to be inserted verbatim. Each row (except literal strings) should be the same number of elements as
                 all the others.
    :returns: A string containing rows formatted as a table.
    """

    list_rows = [r for r in rows if not isinstance(r, str)]

    # If all of the rows are strings, we have nothing to do, so short-circuit.
    if not list_rows:
        return cast(List[str], rows)

    widths = []
    for i in range(len(list_rows[0])):
        widths.append(max(terminal_len(r[i]) for r in list_rows))

    expanded_rows = []
    for row in rows:
        if isinstance(row, str):
            expanded_rows.append([row])
        else:
            expanded_row = []
            for i, cell in enumerate(row):
                if i == len(row) - 1:
                    padding = ''
                else:
                    padding = ' ' * (widths[i] - terminal_len(cell))
                expanded_row.append(cell + padding)
            expanded_rows.append(expanded_row)

    return [(' ' * min_spacing).join(r) for r in expanded_rows]


_ComposeRetT = TypeVar('_ComposeRetT')
_ComposeInnerRetT = TypeVar('_ComposeInnerRetT')


def compose(
    func_one: Callable[[_ComposeInnerRetT], _ComposeRetT],
    func_two: Callable[..., _ComposeInnerRetT],
) -> Callable[..., _ComposeRetT]:
    def composed(*args: Any, **kwargs: Any) -> _ComposeRetT:
        return func_one(func_two(*args, **kwargs))
    return composed


class PaastaColors:

    """Collection of static variables and methods to assist in coloring text."""
    # ANSI colour codes
    BLUE = '\033[34m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    DEFAULT = '\033[0m'
    GREEN = '\033[32m'
    GREY = '\033[38;5;242m'
    MAGENTA = '\033[35m'
    RED = '\033[31m'
    YELLOW = '\033[33m'

    @staticmethod
    def bold(text: str) -> str:
        """Return bolded text.

        :param text: a string
        :return: text colour coded with ANSI bold
        """
        return PaastaColors.color_text(PaastaColors.BOLD, text)

    @staticmethod
    def blue(text: str) -> str:
        """Return text that can be printed blue.

        :param text: a string
        :return: text colour coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text: str) -> str:
        """Return text that can be printed green.

        :param text: a string
        :return: text colour coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text: str) -> str:
        """Return text that can be printed red.

        :param text: a string
        :return: text colour coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def magenta(text: str) -> str:
        """Return text that can be printed magenta.

        :param text: a string
        :return: text colour coded with ANSI magenta"""
        return PaastaColors.color_text(PaastaColors.MAGENTA, text)

    @staticmethod
    def color_text(color: str, text: str) -> str:
        """Return text that can be printed color.

        :param color: ANSI colour code
        :param text: a string
        :return: a string with ANSI colour encoding"""
        # any time text returns to default, we want to insert our color.
        replaced = text.replace(PaastaColors.DEFAULT, PaastaColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + PaastaColors.DEFAULT

    @staticmethod
    def cyan(text: str) -> str:
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI cyan"""
        return PaastaColors.color_text(PaastaColors.CYAN, text)

    @staticmethod
    def yellow(text: str) -> str:
        """Return text that can be printed yellow.

        :param text: a string
        :return: text colour coded with ANSI yellow"""
        return PaastaColors.color_text(PaastaColors.YELLOW, text)

    @staticmethod
    def grey(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.GREY, text)

    @staticmethod
    def default(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.DEFAULT, text)


def print_with_indent(line: str, indent: int=2) -> None:
    """Print a line with a given indent level"""
    paasta_print(" " * indent + line)


def to_bytes(obj: Any) -> bytes:
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode('UTF-8')
    else:
        return str(obj).encode('UTF-8')


def paasta_print(*args: Any, **kwargs: Any) -> None:
    f = kwargs.pop('file', sys.stdout)
    f = getattr(f, 'buffer', f)
    end = to_bytes(kwargs.pop('end', '\n'))
    sep = to_bytes(kwargs.pop('sep', ' '))
    assert not kwargs, kwargs
    to_print = sep.join(to_bytes(x) for x in args) + end
    f.write(to_print)
    f.flush()
