# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import abc
from collections import namedtuple

import six

TaskConfig = namedtuple(
    'TaskConfig',
    ['image', 'cmd', 'cpus', 'mem', 'disk', 'volumes', 'ports', 'cap_add', 'ulimit', 'docker_parameters'],
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
