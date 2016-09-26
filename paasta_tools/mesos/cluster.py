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

import itertools

from . import exceptions
from . import parallel

dne = True
missing_slave = set([])


def get_files_for_tasks(task_list, file_list, max_workers, fail=True):
    global dne

    dne = True

    def process((task, fname)):
        global dne

        try:
            fobj = task.file(fname)
        except exceptions.SlaveDoesNotExist:
            if task["id"] not in missing_slave:
                print("%s:%s" % (task["id"], fname))
                print("Slave no longer exists.")

            missing_slave.add(task["id"])
            raise exceptions.SkipResult

        if fobj.exists():
            dne = False
            return fobj

    elements = itertools.chain(
        *[[(task, fname) for fname in file_list] for task in task_list])

    for result in parallel.stream(process, elements, max_workers):
        if not result:
            continue
        yield result

    if dne and fail:
        raise exceptions.FileNotFoundForTaskException(
            "None of the tasks in %s contin the files in list %s" % (
                ",".join([task["id"] for task in task]),
                ",".join(file_list)
            )
        )
