# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import abc
import uuid
from collections import namedtuple

import six

TaskConfig = namedtuple(
    'TaskConfig',
    ['task_id', 'name', 'image', 'cmd', 'cpus', 'mem', 'disk', 'volumes', 'ports', 'cap_add',
     'ulimit', 'docker_parameters'],
)

# TODO: reimplement TaskConfig using attrs or precord


def make_task_config(image="ubuntu:xenial", cmd="/bin/true", cpus=0.1,
                     mem=32, disk=10, volumes=None, ports=[], cap_add=[],
                     ulimit=[], docker_parameters=[], task_id=None, name=None):
    if task_id is None:
        if name is None:
            task_id = str(uuid.uuid4())
        else:
            task_id = "{}.{}".format(name, str(uuid.uuid4()))

    if name is None:
        name = str(uuid.uuid4())

    if volumes is None:
        volumes = {}
    if ports is None:
        ports = []
    if cap_add is None:
        cap_add = []
    if ulimit is None:
        ulimit = []
    if docker_parameters:
        docker_parameters = []

    return TaskConfig(task_id, name, image, cmd, cpus, mem, disk, volumes, ports, cap_add,
                      ulimit, docker_parameters)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
