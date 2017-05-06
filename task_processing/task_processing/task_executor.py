# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import threading
from collections import namedtuple

import mesos.native
from execution_framework import ExecutionFramework
from mesos.interface import mesos_pb2
TaskConfig = namedtuple(
    'TaskConfig',
    ['image', 'cmd', 'cpus', 'mem', 'disk', 'volumes', 'ports'],
)


class TaskExecutor(object):
    """ Interface for task execution."""

    def __init__(self, credentials, *args, **kwargs):
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        :param dict credentials: Mesos principal and secret.
        """

        # Get creds for mesos
        credential = mesos_pb2.Credential()
        credential.principal = credentials["principal"]
        credential.secret = credentials["secret"]

        self.execution_framework = ExecutionFramework(
            # framework-specific options go here: parallelization,
            # retry policy, etc
        )

        # TODO: Get mesos master ips from smartstack
        self.driver = mesos.native.MesosSchedulerDriver(
            self.execution_framework,
            self.execution_framework.framework_info,
            "10.40.1.17:5050",
            False,
            credential
        )

        # start driver thread immediately
        self.driver_thread = threading.Thread(target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()

    def run(self, task_config):
        """ Schedule task for execution and wait until it's finished. Return
            task id and exit status.
        """
        task_id = self.execution_framework.enqueue(task_config)
        status = self.execution_framework.wait_for(task_id)
        return task_id, status

    def run_promise(self, task_config):
        """ Schedule task for execution and return it's id and a function that
            blocks until the task is finished. Promise returns task's exit
            status.
        """
        task_id = self.execution_framework.enqueue(task_config)

        def promise(): return self.execution_framework.wait_for(task_id)
        return task_id, promise

    def run_async(self, task_config, success=None, failure=None, status=None):
        """ Schedule task for execution and return it's id. Optionally
            subscribe for status updates for this task:

            success: run when task finished successfully
            failure: run when task failed
            status: run on every status update (status object is passed to the
              callback)
        """
        task_id = self.execution_framework.enqueue(task_config)

        if success is not None:
            self.execution_framework.on_success(task_id, success)

        if failure is not None:
            self.execution_framework.on_failure(task_id, failure)

        if status is not None:
            self.execution_framework.on_status(task_id, status)

        return task_id

    def kill(self, task_id=None):
        """ Stop the instance of the task """
        pass

    def status(self, task_id=None):
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

    def wait_until_done(self):
        self.driver_thread.join()
