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
import os

from . import exceptions
from .master import CURRENT as MASTER


def task(prefix, parsed_args, **kwargs):
    return [x["id"] for x in MASTER.tasks(prefix)]


def slave(prefix, parsed_args, **kwargs):
    return [s["id"] for s in MASTER.slaves(prefix)]


def file(prefix, parsed_args, **kwargs):
    files = set([])
    split = prefix.rsplit("/", 1)
    base = ""
    if len(split) == 2:
        base = split[0]
    pattern = split[-1]

    for task in MASTER.tasks(parsed_args.task):
        # It is possible for the master to have completed tasks that no longer
        # have files and/or executors
        try:
            for file_meta in task.file_list(base):
                rel = os.path.relpath(file_meta["path"], task.directory)
                if rel.rsplit("/", 1)[-1].startswith(pattern):
                    if file_meta["mode"].startswith("d"):
                        rel += "/"
                    files.add(rel)
        except exceptions.MissingExecutor:
            pass
    return files
