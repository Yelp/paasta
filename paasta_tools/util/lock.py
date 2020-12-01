import contextlib
import fcntl
import io
import sys
from typing import IO
from typing import Iterator
from typing import Union

from paasta_tools.util.timeout import Timeout


_AnyIO = Union[io.IOBase, IO]


@contextlib.contextmanager
def flock(fd: _AnyIO) -> Iterator[None]:
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def timed_flock(fd: _AnyIO, seconds: int = 1) -> Iterator[None]:
    """ Attempt to grab an exclusive flock with a timeout. Uses Timeout, so will
    raise a TimeoutError if `seconds` elapses before the flock can be obtained
    """
    # We don't want to wrap the user code in the timeout, just the flock grab
    flock_context = flock(fd)
    with Timeout(seconds=seconds):
        flock_context.__enter__()
    try:
        yield
    finally:
        flock_context.__exit__(*sys.exc_info())
