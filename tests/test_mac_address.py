import contextlib
import fcntl
import itertools
import os
import signal
import sys
import time

import mock
import pytest

from paasta_tools import mac_address


skip_if_osx = pytest.mark.skipif(
    sys.platform == "darwin", reason="Flock is not present on OS X"
)


def test_simple(tmpdir):
    mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
    with contextlib.closing(lock_file):
        assert lock_file is not None
        assert mac == "02:52:00:00:00:00"
        assert tmpdir.join(mac).check()


def test_dir_not_exist(tmpdir):
    with pytest.raises(IOError):
        mac_address.reserve_unique_mac_address(str(tmpdir.join("nonexistent")))


def test_file_exists_no_flock(tmpdir):
    tmpdir.join("02:52:00:00:00:00").ensure()
    mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
    with contextlib.closing(lock_file):
        assert lock_file is not None
        assert mac == "02:52:00:00:00:00"


def _flock_process(path):
    # fork a subprocess that holds an flock.
    r, w = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:  # pragma: no cover
        os.close(r)
        fd = os.open(path, os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        os.write(w, b"ok")
        os.close(w)
        time.sleep(60 * 60 * 24)  # sleep for some approximation of infinity
        sys.exit(0)  # never returns

    os.close(w)
    # wait for something to be printed so we know the flock has occurred
    assert os.read(r, 2) == b"ok"
    os.close(r)
    return child_pid


@skip_if_osx
def test_file_exists_flock(tmpdir):
    # it doesn't count if this process has the flock, so we need to spawn a different one to hold it
    flock_process = _flock_process(str(tmpdir.join("02:52:00:00:00:00")))
    try:
        mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
        with contextlib.closing(lock_file):
            assert lock_file is not None
            assert mac == "02:52:00:00:00:01"
    finally:
        os.kill(flock_process, signal.SIGKILL)


@pytest.fixture(autouse=True)
def mock_randbits():
    # make getrandbits() reliably return an incrementing counter starting at 0
    class counter(itertools.count):
        def __call__(self, _):
            return next(self)

    with mock.patch.object(mac_address.random, "getrandbits", side_effect=counter()):
        yield
