# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import abc
from collections import namedtuple

import six

TaskConfig = namedtuple(
    'TaskConfig',
    ['image', 'cmd', 'cpus', 'mem', 'disk', 'volumes', 'ports', 'executor'],
)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id=None):
        """ Stop the instance of the task """
        pass

    @abc.abstractmethod
    def status(self, task_id=None):
        """ Get the status of this instance of the task.
            What return type here?? we do not know yet
        """
        pass

    @abc.abstractmethod
    def cleanup(self):
        """ Cleanup any state before shutting down, can internally call kill """
        pass

    @abc.abstractmethod
    def supported_extra_methods(self):
        pass

    @abc.abstractmethod
    def wait_until_done(self):
        pass


@six.add_metaclass(abc.ABCMeta)
class Promiseable(object):

    @abc.abstractmethod
    def run_promise(self, task_config):
        pass


@six.add_metaclass(abc.ABCMeta)
class Asyncable(object):

    @abc.abstractmethod
    def run_async(self, task_config, success=None, failure=None, status=None):
        pass


@six.add_metaclass(abc.ABCMeta)
class Subscribable(object):

    @abc.abstractmethod
    def subscribe(self, queue):
        pass
