import time

import mock
import pytest

from paasta_tools.util import lock
from paasta_tools.util.timeout import TimeoutError


@mock.patch("paasta_tools.util.lock.fcntl.flock", autospec=True, wraps=lock.fcntl.flock)
def test_flock(mock_flock, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with lock.flock(f):
            mock_flock.assert_called_once_with(f.fileno(), lock.fcntl.LOCK_EX)
            mock_flock.reset_mock()

        mock_flock.assert_called_once_with(f.fileno(), lock.fcntl.LOCK_UN)


@mock.patch("paasta_tools.util.lock.Timeout", autospec=True)
@mock.patch("paasta_tools.util.lock.fcntl.flock", autospec=True, wraps=lock.fcntl.flock)
def test_timed_flock_ok(mock_flock, mock_timeout, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with lock.timed_flock(f, seconds=mock.sentinel.seconds):
            mock_timeout.assert_called_once_with(seconds=mock.sentinel.seconds)
            mock_flock.assert_called_once_with(f.fileno(), lock.fcntl.LOCK_EX)
            mock_flock.reset_mock()

        mock_flock.assert_called_once_with(f.fileno(), lock.fcntl.LOCK_UN)


@mock.patch(
    "paasta_tools.util.lock.Timeout",
    autospec=True,
    side_effect=TimeoutError("Oh noes"),
)
@mock.patch("paasta_tools.util.lock.fcntl.flock", autospec=True, wraps=lock.fcntl.flock)
def test_timed_flock_timeout(mock_flock, mock_timeout, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with pytest.raises(TimeoutError):
            with lock.timed_flock(f):
                assert False  # pragma: no cover
        assert mock_flock.mock_calls == []


@mock.patch("paasta_tools.util.lock.fcntl.flock", autospec=True, wraps=lock.fcntl.flock)
def test_timed_flock_inner_timeout_ok(mock_flock, tmpdir):
    # Doing something slow inside the 'with' context of timed_flock doesn't cause a timeout
    # (the timeout should only apply to the flock operation itself)
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with lock.timed_flock(f, seconds=1):
            time.true_slow_sleep(0.1)  # type: ignore
        assert mock_flock.mock_calls == [
            mock.call(f.fileno(), lock.fcntl.LOCK_EX),
            mock.call(f.fileno(), lock.fcntl.LOCK_UN),
        ]
