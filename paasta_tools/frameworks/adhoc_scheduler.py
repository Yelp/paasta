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

from paasta_tools.frameworks.native_scheduler import LIVE_TASK_STATES
from paasta_tools.frameworks.native_scheduler import NativeScheduler
from paasta_tools.utils import paasta_print


class AdhocScheduler(NativeScheduler):
    def __init__(self, *args, **kwargs):
        self.dry_run = kwargs.pop('dry_run')

        if kwargs.get('service_config_overrides') is None:
            kwargs['service_config_overrides'] = {}
        kwargs['service_config_overrides']['instances'] = 1

        super(AdhocScheduler, self).__init__(*args, **kwargs)

    def need_to_stop(self):
        # Is used to decide whether to stop the driver or try to start more tasks.
        for task, params in self.tasks_with_flags.items():
            if params.mesos_task_state not in LIVE_TASK_STATES:
                return True
        return False

    def statusUpdate(self, driver, update):
        super(AdhocScheduler, self).statusUpdate(driver, update)
        # Stop if task ran and finished
        if self.need_to_stop():
            driver.stop()

    def tasks_for_offer(self, driver, offer):
        # In dry run satisfy exit-conditions after we got the offer
        if self.dry_run or self.need_to_stop():
            if self.dry_run:
                tasks = super(AdhocScheduler, self).tasks_for_offer(driver, offer)
                paasta_print("Would have launched: ", tasks)
            driver.stop()
            return None

        return super(AdhocScheduler, self).tasks_for_offer(driver, offer)

    def kill_tasks_if_necessary(self, *args, **kwargs):
        return
