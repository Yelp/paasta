# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import abc
import uuid
from collections import namedtuple

import six

TaskConfig = namedtuple(
    'TaskConfig',
    ['task_id', 'name', 'image', 'cmd', 'cpus', 'mem', 'disk', 'volumes',
     'ports', 'cap_add', 'ulimit', 'docker_parameters'],
)


def make_task_config(task_id=None, name='foo', image="ubuntu:xenial",
                     cmd="/bin/true", cpus=0.1, mem=32, disk=10, volumes=None,
                     ports=None, cap_add=None, ulimit=None,
                     docker_parameters=None):
    if task_id is None:
        task_id = "%s.%s" % (name, uuid.uuid4().hex)
    if volumes is None:
        volumes = {}
    if ports is None:
        ports = []
    if cap_add is None:
        cap_add = []
    if ulimit is None:
        ulimit = []
    if docker_parameters is None:
        docker_parameters = []

    return TaskConfig(task_id, name, image, cmd, cpus, mem, disk, volumes,
                      ports, cap_add, ulimit, docker_parameters)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
