# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
