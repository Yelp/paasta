# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import threading

import mesos.native
from framework import MesosScheduler
from mesos.interface import mesos_pb2


class MesosTaskExecution(object):
    """ Interface for task execution."""

    def __init__(self, docker_configs):
        """
        Constructs the instance of a task execution, encapsulating all state required to run,
        monitor and stop the job.

        :param dict executor_arguments: The task specification which is required to run the task on
            the particular implementation of the execution platform.
        """
        # Get creds for mesos
        credential = mesos_pb2.Credential()
        credential.principal = "mesos_slave"
        credential.secret = "bee5aeJibee5aeJibee5aeJi"

        mesos_scheduler = MesosScheduler(docker_configs)

        # TODO: Get mesos master ips from smartstack
        self.driver = mesos.native.MesosSchedulerDriver(
            mesos_scheduler,
            mesos_scheduler.framework_info,
            "10.40.1.17:5050",
            False,
            credential
        )

    def run_mesos_driver(self):
        status = 0 if self.driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        # Ensure that the driver process terminates.
        self.driver.stop()
        exit(status)

    def run(self):
        """
        Runs the task (or tasks) registered on this object. Creates underlying executor(s) and
        runs it on the execution platform.
        After a task is completed, it calls task_status callback which provides the status of
        execution to the caller. The exact interface of these callbacks is tbd.

        :param: function task_status: This function will be called after the task is executed
            on the execution platform. The arguments and return values depend on the executor
            itself
        """
        thread = threading.Thread(target=self.run_mesos_driver, args=())
        thread.daemon = True
        thread.start()

    def kill(self, task_status=None):
        """ Stop the instance of the task """
        pass

    def status(self):
        """ Get the status of this instance of the task.
            What return type here?? we do not know yet
        """
        pass

    def cleanup(self):
        """ Cleanup any state before shutting down, can internally call kill """
        pass

    def supported_extra_methods(self):
        """
        :return [list of strings] of extensions that this executor provides

        For example, if this implementation supports stdout or stdin
        """
