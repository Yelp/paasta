import inspect
import os
from contextlib import contextmanager

import botocore.exceptions
import colorlog
from yelp_batch.batch import batch_context

from clusterman.monitoring_lib import get_monitoring_client

RLE_COUNTER_NAME = 'clusterman.request_limit_exceeded'
logger = colorlog.getLogger(__name__)


class BatchLoggingMixin:  # pragma: no cover
    @batch_context
    def setup_watchers(self):
        self.logger.info('Starting batch {name}; watching {watched_files} for changes'.format(
            name=type(self).__name__,
            watched_files=[watcher.filename for watcher in self.version_checker.watchers],
        ))
        yield
        self.logger.info('Batch {name} complete'.format(name=type(self).__name__))


class BatchRunningSentinelMixin:  # pragma: no cover
    @batch_context
    def make_running_sentinel(self):
        batch_name, ext = os.path.splitext(os.path.basename(inspect.getfile(self.__class__)))
        sentinel_file = f'/tmp/{batch_name}.running'
        with open(sentinel_file, 'w') as f:
            f.write(str(os.getpid()))
        yield


@contextmanager
def suppress_request_limit_exceeded():
    try:
        yield
    except botocore.exceptions.ClientError as e:
        if e.response.get('Error', {}).get('Code') == 'RequestLimitExceeded':
            logger.warning(e)
            rle_counter = get_monitoring_client().create_counter(RLE_COUNTER_NAME)
            rle_counter.count()
        else:
            raise
