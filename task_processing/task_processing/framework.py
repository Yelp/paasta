# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2


log = logging.getLogger(__name__)


class MesosScheduler(mesos.interface.Scheduler):
    # TODO: Decide which options to move to config.
    def __init__(
        self,
        task_config, total_tasks_to_run=1,
        framework_checkpointing=False,
        name="Remote run",
        retries=0,
        paasta_pool=None,
        offer_backoff=240
    ):
        self.task_config = task_config
        self.framework_id = None
        self.tasks_launched = 0
        self.total_tasks_to_run = total_tasks_to_run
        self.name = name
        self.offer_backoff = offer_backoff
        self.tasks_finished = 0
        self.framework_checkpointing = framework_checkpointing
        self.framework_info = self.build_framework_info()
        self.offer_decline_filter = self.build_decline_offer_filter()

    def build_framework_info(self):
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ""  # Have Mesos fill in the current user.
        framework.name = self.name
        framework.checkpoint = self.framework_checkpointing
        return framework

    def registered(self, driver, frameworkId, masterInfo):
        log.info("Registered with framework ID {id} to mesos masters {masterInfo}".format(
            id=frameworkId.value,
            masterInfo=str(masterInfo)
        )
        )
        self.framework_id = frameworkId.value

    def reregistered(self, driver, masterInfo):
        log.info("Re-registered with framework ID {id} to mesos masters {masterInfo}".format(
            id=self.framework_id,
            masterInfo=str(masterInfo)
        )
        )

    def slaveLost(self, drive, slaveId):
        log.error("Slave lost: {id}".format(id=str(slaveId)))

    def build_decline_offer_filter(self):
        f = mesos_pb2.Filters()
        f.refuse_seconds = self.offer_backoff
        return f

    def get_available_ports(self, resource):
        i = 0
        ports = []
        while True:
            try:
                ports = ports + range(resource.ranges.range[i].begin, resource.ranges.range[i].end)
                i += 1
            except Exception:
                break
        return ports

    def is_offer_valid(self, offer):
        offer_cpus = 0
        offer_mem = 0
        offer_disk = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                offer_cpus += resource.scalar.value
            elif resource.name == "mem":
                offer_mem += resource.scalar.value
            elif resource.name == "disk":
                offer_disk += resource.scalar.value
            elif resource.name == "ports":
                # TODO: Validate if the ports available > ports require
                self.get_available_ports(resource)

        log.info("Received offer {id} with cpus: {cpu} and mem: {mem}".format(
            id=offer.id.value,
            cpu=offer_cpus,
            mem=offer_mem
        )
        )

        if ((offer_cpus >= self.task_config.cpus and
             offer_mem >= self.task_config.mem and
             offer_disk >= self.task_config.disk)):
            return True

        return False

    def resourceOffers(self, driver, offers):
        for offer in offers:
            # Check if the offer is valid or not
            if not self.is_offer_valid(offer):
                log.info("Declining offer {id} because it does not match our requirements.".format(id=offer.id.value))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            if self.tasks_launched + self.tasks_finished >= self.total_tasks_to_run:
                log.info("Declining offer {id} because all the tasks have been launched already.".format(
                    id=offer.id.value
                )
                )
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            log.info("Launching a new docker task using offer {id}".format(id=offer.id.value))
            offer_tasks = []
            task = self.create_new_docker_task(offer, self.tasks_launched)
            offer_tasks.append(task)
            self.tasks_launched += 1
            driver.launchTasks(offer.id, offer_tasks)

    def statusUpdate(self, driver, update):
        log.info("Task update {update} received for task {task}".format(
            update=mesos_pb2.TaskState.Name(update.state),
            task=update.task_id.value
        )
        )

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasks_finished += 1
            if self.tasks_finished == self.total_tasks_to_run:
                log.info("All tasks are done!!!")
                # TODO: Decide whether to supress the offers or not.
                driver.suppressOffers()

        if update.state in (mesos_pb2.TASK_LOST, mesos_pb2.TASK_KILLED, mesos_pb2.TASK_FAILED):
            self.tasks_launched = - 1

        # We have to do this because we are not using implicit acknowledgements.
        driver.acknowledgeStatusUpdate(update)

    def create_new_docker_task(self, offer, task_id):

        task = mesos_pb2.TaskInfo()

        container = mesos_pb2.ContainerInfo()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER

        command = mesos_pb2.CommandInfo()
        command.value = self.task_config.cmd

        task.command.MergeFrom(command)
        task.task_id.value = str(task_id)
        task.slave_id.value = offer.slave_id.value
        # TODO: We can add action names here
        task.name = "executor-{id}".format(id=task_id)

        # CPUs
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.task_config.cpus

        # mem
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.task_config.mem

        # disk
        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        disk.scalar.value = self.task_config.disk

        # Volumes
        for mode in self.task_config.volumes:
            for container_path, host_path in self.task_config.volumes[mode]:
                volume = container.volumes.add()
                volume.container_path = container_path
                volume.host_path = host_path
                """
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                """
                volume.mode = 1 if mode == "RW" else 2

        # Container info
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = self.task_config.image
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = True

        available_ports = []
        for resource in offer.resources:
            if resource.name == "ports":
                available_ports = self.get_available_ports(resource)

        port_to_use = available_ports[0]

        mesos_ports = task.resources.add()
        mesos_ports.name = "ports"
        mesos_ports.type = mesos_pb2.Value.RANGES
        port_range = mesos_ports.ranges.range.add()

        port_range.begin = port_to_use
        port_range.end = port_to_use
        docker_port = docker.port_mappings.add()
        docker_port.host_port = port_to_use
        docker_port.container_port = 8888

        # Set docker info in container.docker
        container.docker.MergeFrom(docker)
        # Set docker container in task.container
        task.container.MergeFrom(container)

        return task
