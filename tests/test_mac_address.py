# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import itertools
import subprocess

import mock
import pytest

from paasta_tools import mac_address


def test_simple(tmpdir):
    mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
    with contextlib.closing(lock_file):
        assert lock_file is not None
        assert mac == '02:52:00:00:00:00'
        assert tmpdir.join(mac).check()


def test_dir_not_exist(tmpdir):
    with pytest.raises(IOError):
        mac_address.reserve_unique_mac_address(str(tmpdir.join('nonexistent')))


def test_file_exists_no_flock(tmpdir):
    tmpdir.join('02:52:00:00:00:00').ensure()
    mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
    with contextlib.closing(lock_file):
        assert lock_file is not None
        assert mac == '02:52:00:00:00:00'


def _flock_process(path):
    # spawn a subprocess that holds an flock.
    proc = subprocess.Popen(
        ['flock', path, 'bash', '-c', 'echo -n ok && sleep infinity'],
        stdout=subprocess.PIPE)
    # wait for something to be printed so we know the flock has occurred
    assert proc.stdout.read(2) == 'ok'
    return proc


def test_file_exists_flock(tmpdir):
    # it doesn't count if this process has the flock, so we need to spawn a different one to hold it
    flock_process = _flock_process(str(tmpdir.join('02:52:00:00:00:00')))
    try:
        mac, lock_file = mac_address.reserve_unique_mac_address(str(tmpdir))
        with contextlib.closing(lock_file):
            assert lock_file is not None
            assert mac == '02:52:00:00:00:01'
    finally:
        flock_process.kill()


def test_file_exists_exhaustion(tmpdir):
    flock_processes = []
    try:
        for x in range(100):
            flock_process = _flock_process(str(tmpdir.join('02:52:00:00:00:{:02x}'.format(x))))
            flock_processes.append(flock_process)

        with pytest.raises(mac_address.MacAddressException):
            mac_address.reserve_unique_mac_address(str(tmpdir))
    finally:
        [p.kill() for p in flock_processes]


@pytest.yield_fixture(autouse=True)
def mock_randbits():
    # make getrandbits() reliably return an incrementing counter starting at 0
    class counter(itertools.count):
        def __call__(self, _):
            return self.next()

    with mock.patch.object(mac_address.random, 'getrandbits', side_effect=counter()):
        yield
