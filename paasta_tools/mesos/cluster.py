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
import asyncio
import itertools
from typing import Set

from . import exceptions
from paasta_tools.utils import paasta_print

missing_slave: Set[str] = set()


async def get_files_for_tasks(task_list, file_list, max_workers):
    no_files_found = True

    async def process(task_fname):
        task, fname = task_fname
        try:
            fobj = await task.file(fname)
        except exceptions.SlaveDoesNotExist:
            if task["id"] not in missing_slave:
                paasta_print("{}:{}".format(task["id"], fname))
                paasta_print("Slave no longer exists.")

            missing_slave.add(task["id"])
            raise exceptions.SkipResult

        if await fobj.exists():
            return fobj

    elements = itertools.chain(
        *[[(task, fname) for fname in file_list] for task in task_list],
    )

    futures = [asyncio.ensure_future(process(element)) for element in elements]

    if futures:
        for result in asyncio.as_completed(futures):
            try:
                result = await result
                if result:
                    no_files_found = False
                    yield result
            except exceptions.SkipResult:
                pass

    if no_files_found:
        raise exceptions.FileNotFoundForTaskException(
            "None of the tasks in {} contain the files in list {}".format(
                ",".join([task["id"] for task in task_list]),
                ",".join(file_list),
            ),
        )
