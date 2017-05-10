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
from paasta_tools.frameworks.native_service_config import UnknownNativeServiceError
from paasta_tools.utils import paasta_print


class AdhocScheduler(NativeScheduler):
    def __init__(self, *args, **kwargs):
        self.dry_run = kwargs.pop('dry_run')

        if kwargs.get('service_config_overrides') is None:
            kwargs['service_config_overrides'] = {}
        kwargs['service_config_overrides'].setdefault('instances', 1)
        self.finished_countdown = kwargs['service_config_overrides']['instances']

        super(AdhocScheduler, self).__init__(*args, **kwargs)

    def need_to_stop(self):
        # Is used to decide whether to stop the driver or try to start more tasks.
        return self.finished_countdown == 0

    def statusUpdate(self, driver, update):
        super(AdhocScheduler, self).statusUpdate(driver, update)

        if update.state not in LIVE_TASK_STATES:
            self.finished_countdown -= 1

        # Stop if task ran and finished
        if self.need_to_stop():
            driver.stop()

    def tasks_and_state_for_offer(self, driver, offer, state):
        # In dry run satisfy exit-conditions after we got the offer
        if self.dry_run or self.need_to_stop():
            if self.dry_run:
                tasks, _ = super(AdhocScheduler, self). \
                    tasks_and_state_for_offer(driver, offer, state)
                paasta_print("Would have launched: ", tasks)
            driver.stop()
            return [], state

        return super(AdhocScheduler, self). \
            tasks_and_state_for_offer(driver, offer, state)

    def kill_tasks_if_necessary(self, *args, **kwargs):
        return

    def validate_config(self):
        if self.service_config.get_cmd() is None:
            raise UnknownNativeServiceError("missing cmd in service config")
