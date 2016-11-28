# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import concurrent.futures

from . import exceptions


@contextlib.contextmanager
def execute(max_workers):
    try:
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers)
        yield executor
    except KeyboardInterrupt:
        # Threads in the ThreadPoolExecutor are created with
        # daemon=True. There is, therefore, an atexit function registered
        # that allows all the currently running threads to stop before
        # allowing the interpreter to stop. Because we don't care whether
        # the worker threads exit cleanly or not, we force shutdown to be
        # immediate.
        concurrent.futures.thread._threads_queues.clear()
        raise
    finally:
        executor.shutdown(wait=False)


def stream(fn, elements, workers):
    """Yield the results of fn as jobs complete."""
    jobs = []

    with execute(workers) as executor:
        for elem in elements:
            jobs.append(executor.submit(fn, elem))

        for job in concurrent.futures.as_completed(jobs):
            try:
                yield job.result()
            except exceptions.SkipResult:
                pass
