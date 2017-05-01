# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class DockerTaskConfigs(object):
    def __init__(self, image, cmd, cpus, mem, disk, volumes, ports):
        self.image = image
        self.cmd = cmd
        self.cpus = cpus
        self.mem = mem
        self.disk = disk
        self.volumes = volumes
        self.ports = ports
