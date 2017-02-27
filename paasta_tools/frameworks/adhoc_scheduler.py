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

sys.path.insert(0, '/usr/lib/python2.7/site-packages/mesos')
import mesos
from mesos.interface import Scheduler
from mesos.interface import mesos_pb2

from paasta_tools.utils import paasta_print

class PaastaAdhocScheduler(Scheduler):

    def __init__ (self, command, service_config, system_paasta_config, dry_run):
        self.command = command
        self.service_config = service_config
        self.system_paasta_config = system_paasta_config
        self.dry_run = dry_run
        self.launched = False
        self.status = None


    def registered(self, driver, framework_id, master_info):
        paasta_print("Registered with framework id: {}".format(framework_id))


    def resourceOffers(self, driver, offers):
        if self.launched:
            return

        paasta_print("Recieved resource offers: {}".format([o.id.value for o in offers]))
        offer = offers[0]
        driver.suppressOffers()
        for i in offers[1:]:
            driver.declineOffer(offers[i].id)
        task = self.new_task(offer)
        paasta_print("Launching task {task} "
                     "using offer {offer}.".format(task=task.task_id.value,
                                                   offer=offer.id.value))

        self.launched = True
        if self.dry_run:
            paasta_print("Not doing anything in dry-run mode."
                         "Would have launched: {task}".format(task=task))
            self.status = 0
            driver.stop()
        else:
            driver.launchTasks(offer.id, [task])


    def statusUpdate(self, driver, update):
        paasta_print("Mesos Scheduler: task %s is in state %d" % (update.task_id.value, update.state))

        if update.state == mesos_pb2.TASK_FINISHED:
            self.status = 0
        elif update.state == mesos_pb2.TASK_FAILED or update.state == mesos_pb2.TASK_ERROR:
            paasta_print("Task failed: %s" % update.message)
            self.status = 1
        elif update.state == mesos_pb2.TASK_LOST or update.state == mesos_pb2.TASK_KILLED:
            self.status = 1
            # self.launched = False

        if self.status != None:
            paasta_print("Task %s is finished." % update.task_id.value)
            # for stream in mesos.cli.cluster.files(flist=['stdout','stderr'], fltr=update.task_id.value):
            #     print("Printing %s for task %s" % (stream[0].path, update.task_id.value))
            #     for line in stream[0].readlines():
            #         print line
            driver.stop()


    def new_task(self, offer):
        task = self.service_config.base_task(self.system_paasta_config, portMappings=False)
        id = uuid.uuid4()
        task.task_id.value = str(id)
        task.slave_id.value = offer.slave_id.value

        if self.command:
            task.command.value = self.command

        return task
