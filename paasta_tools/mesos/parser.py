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
import argparse

from . import completion_helpers


class ArgumentParser(argparse.ArgumentParser):

    def enable_print_header(self):
        self.add_argument(
            '-q', action='store_true',
            help="Suppresses printing of headers when multiple tasks are " +
                 "being examined"
        )

    def task_argument(self, optional=False):
        kwargs = {
            "default": "",
            "type": str,
            "help": "ID of the task. May match multiple tasks (or all)"
        }
        if optional:
            kwargs["nargs"] = "?"

        self.add_argument('task', **kwargs).completer = completion_helpers.task

    def file_argument(self):
        self.add_argument(
            'file', nargs="*", default=["stdout"],
            help="Path to the file inside the task's sandbox."
        ).completer = completion_helpers.file

    def path_argument(self):
        self.add_argument(
            'path', type=str, nargs="?", default="",
            help="""Path to view."""
        ).completer = completion_helpers.file
