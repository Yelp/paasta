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
from __future__ import print_function

import contextlib
import itertools

import concurrent.futures

from . import exceptions
from .cfg import CURRENT as CFG


@contextlib.contextmanager
def execute():
    try:
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=CFG["max_workers"])
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


def stream(fn, elements):
    """Yield the results of fn as jobs complete."""
    jobs = []

    with execute() as executor:
        for elem in elements:
            jobs.append(executor.submit(fn, elem))

        for job in concurrent.futures.as_completed(jobs):
            try:
                yield job.result()
            except exceptions.SkipResult:
                pass


def by_fn(keyfn, fn, items):
    """Call fn in parallel across items based on keyfn.

    Extensive caching/memoization is utilized when fetching data.
    When you run a function against tasks in a completely parallel way, the
    caching is skipped and there is the possibility that your endpoint will
    receive multiple requests. For most use cases, this significantly slows
    the result down (instead of speeding it up).

    The solution to this predicament is to execute fn in parallel but only
    across a specific partition function (slave ids in this example).
    """

    # itertools.groupby returns a list of (key, generator) tuples. A job
    # is submitted and then the local execution context continues. The
    # partitioned generator is destroyed and you only end up executing fn
    # over a small subset of the desired partition. Therefore, the list()
    # conversion when submitting the partition for execution is very
    # important.
    for result in stream(
            lambda (k, part): [fn(i) for i in list(part)],
            itertools.groupby(items, keyfn)):
        for l in result:
            yield l


def by_slave(fn, tasks):
    """Execute a function against tasks partitioned by slave."""

    def keyfn(x):
        return x.slave["id"]
    tasks = sorted(tasks, key=keyfn)
    return by_fn(keyfn, fn, tasks)
