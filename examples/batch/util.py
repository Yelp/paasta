import inspect
import os
from contextlib import contextmanager

import botocore.exceptions
import colorlog

logger = colorlog.getLogger(__name__)


class BatchRunningSentinelMixin:  # pragma: no cover
    def make_running_sentinel(self):
        batch_name, ext = os.path.splitext(os.path.basename(inspect.getfile(self.__class__)))
        sentinel_file = f'/tmp/{batch_name}.running'
        with open(sentinel_file, 'w') as f:
            f.write(str(os.getpid()))


@contextmanager
def suppress_request_limit_exceeded():
    try:
        yield
    except botocore.exceptions.ClientError as e:
        if e.response.get('Error', {}).get('Code') == 'RequestLimitExceeded':
            logger.warning(e)
        else:
            raise
