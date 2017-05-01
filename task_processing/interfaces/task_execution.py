# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class TaskExecution(object):
    """ Interface for task execution."""

    def __init__(self, **executor_arguments):
        """
        Constructs the instance of a task execution, encapsulating all state required to run,
        monitor and stop the job.

        :param dict executor_arguments: The task specification which is required to run the task on
            the particular implementation of the execution platform.
        """
        pass

    def run(self, task_status=None):
        """
        Runs the task (or tasks) registered on this object. Creates underlying executor(s) and
        runs it on the execution platform.
        After a task is completed, it calls task_status callback which provides the status of
        execution to the caller. The exact interface of these callbacks is tbd.

        :param: function task_status: This function will be called after the task is executed
            on the execution platform. The arguments and return values depend on the executor
            itself
        """
        pass

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
