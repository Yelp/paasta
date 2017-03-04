#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.frameworks.native_scheduler import NativeScheduler


class AdhocScheduler(NativeScheduler):
    def __init__(self, *args, **kwargs):
        self.dry_run = kwargs.pop('dry_run')
        self.task_started = False
        super(AdhocScheduler, self).__init__(*args, **kwargs)

    def need_more_tasks(self, *args, **kwargs):
        # One of pre-conditions in start_task for launching tasks, returning
        # True doesn't actually guarantee the task was launched.
        return len(self.tasks_with_flags) == 0

    def need_to_stop(self):
        # Is used to decide whether to stop the driver or try to start more tasks.
        return self.task_started and self.need_more_tasks()

    def statusUpdate(self, driver, update):
        super(AdhocScheduler, self).statusUpdate(driver, update)
        # Stop if task ran and finished
        if self.need_to_stop():
            driver.stop()

    def start_task(self, *args, **kwargs):
        # Possibly statusUpdate already removed the task from tasks_with_flags
        # but didn't stop() the driver yet. We don't want to launch one more
        # task in this case.
        if self.need_to_stop():
            return None

        tasks = super(AdhocScheduler, self).start_task(*args, **kwargs)

        # Task was launched, tell driver to stop after it finishes.
        if not self.need_more_tasks():
            self.task_started = True

        return tasks

    def kill_tasks_if_necessary(self, *args, **kwargs):
        return
