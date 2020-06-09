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
from typing import Dict
from typing import List
from typing import Tuple

from pymesos import MesosSchedulerDriver

from paasta_tools.frameworks.constraints import ConstraintState
from paasta_tools.frameworks.native_scheduler import LIVE_TASK_STATES
from paasta_tools.frameworks.native_scheduler import NativeScheduler
from paasta_tools.frameworks.native_service_config import TaskInfo
from paasta_tools.frameworks.native_service_config import UnknownNativeServiceError


class AdhocScheduler(NativeScheduler):
    def __init__(self, *args, **kwargs):
        self.dry_run = kwargs.pop("dry_run")

        if kwargs.get("service_config_overrides") is None:
            kwargs["service_config_overrides"] = {}
        kwargs["service_config_overrides"].setdefault("instances", 1)
        self.finished_countdown = kwargs["service_config_overrides"]["instances"]

        super().__init__(*args, **kwargs)

    def need_to_stop(self):
        # Is used to decide whether to stop the driver or try to start more tasks.
        return self.finished_countdown == 0

    def statusUpdate(self, driver: MesosSchedulerDriver, update: Dict):
        super().statusUpdate(driver, update)

        if update["state"] not in LIVE_TASK_STATES:
            self.finished_countdown -= 1

        # Stop if task ran and finished
        if self.need_to_stop():
            driver.stop()

    def tasks_and_state_for_offer(
        self, driver: MesosSchedulerDriver, offer, state: ConstraintState
    ) -> Tuple[List[TaskInfo], ConstraintState]:
        # In dry run satisfy exit-conditions after we got the offer
        if self.dry_run or self.need_to_stop():
            if self.dry_run:
                tasks, _ = super().tasks_and_state_for_offer(driver, offer, state)
                print("Would have launched: ", tasks)
            driver.stop()
            return [], state

        return super().tasks_and_state_for_offer(driver, offer, state)

    def kill_tasks_if_necessary(self, *args, **kwargs):
        return

    def validate_config(self):
        if self.service_config.get_cmd() is None:
            raise UnknownNativeServiceError("missing cmd in service config")
