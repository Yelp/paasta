import io

from contextlib import contextmanager

from paasta_tools.utils import timed_flock

DEFAULT_IP_FLOCK_PATH = '/var/lib/paasta/ip.flock'
DEFAULT_IP_FLOCK_TIMEOUT_SECS = 2

@contextmanager
def ip_flock(flock_path=DEFAULT_IP_FLOCK_PATH):
    """ Grab an exclusive flock to avoid concurrent ip allocations
    """
    with io.FileIO(flock_path, 'w') as f:
        with timed_flock(f, seconds=DEFAULT_IP_FLOCK_TIMEOUT_SECS):
            yield
