# Copyright 2015-2016 Yelp Inc.
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
import re
import time

import requests


_drain_methods = {}


def register_drain_method(name):
    """Returns a decorator that registers a DrainMethod subclass at a given name
    so get_drain_method/list_drain_methods can find it."""
    def outer(drain_method):
        _drain_methods[name] = drain_method
        return drain_method
    return outer


def get_drain_method(name, service, instance, nerve_ns, drain_method_params=None):
    return _drain_methods[name](service, instance, nerve_ns, **(drain_method_params or {}))


def list_drain_methods():
    return sorted(_drain_methods.keys())


class DrainMethod(object):
    """A drain method is a way of stopping new traffic to tasks without killing them. For example, you might take a task
    out of a load balancer by causing its healthchecks to fail.

    A drain method must have the following methods:
     - drain(task): Begin draining traffic from a task. This should be idempotent.
     - stop_draining(task): Stop draining traffic from a task. This should be idempotent.
     - is_draining(task): Whether a task has already been marked as downed. Note that this state should be stored out of
                          process, because a bounce may take multiple runs of setup_marathon_job to complete.
     - is_safe_to_kill(task): Return True if this task is safe to kill, False otherwise.

    When implementing a drain method, be sure to decorate with @register_drain_method(name).
    """

    def __init__(self, service, instance, nerve_ns, **kwargs):
        self.service = service
        self.instance = instance
        self.nerve_ns = nerve_ns

    def drain(self, task):
        """Make a task stop receiving new traffic."""
        raise NotImplementedError()

    def stop_draining(self, task):
        """Make a task that has previously been downed start receiving traffic again."""
        raise NotImplementedError()

    def is_draining(self, task):
        """Return whether a task is being drained."""
        raise NotImplementedError()

    def is_safe_to_kill(self, task):
        """Return True if a task is drained and ready to be killed, or False if we should wait."""
        raise NotImplementedError()


@register_drain_method('noop')
class NoopDrainMethod(DrainMethod):
    """This drain policy does nothing and assumes every task is safe to kill."""

    def drain(self, task):
        pass

    def stop_draining(self, task):
        pass

    def is_draining(self, task):
        return False

    def is_safe_to_kill(self, task):
        return True


@register_drain_method('test')
class TestDrainMethod(DrainMethod):
    """This drain policy is meant for integration testing. Do not use."""

    # These are variables on the class for ease of use in testing.
    downed_task_ids = set()
    safe_to_kill_task_ids = set()

    def drain(self, task):
        self.downed_task_ids.add(task.id)

    def stop_draining(self, task):
        self.downed_task_ids -= set([task.id])
        self.safe_to_kill_task_ids -= set([task.id])

    def is_draining(self, task):
        return task.id in (self.downed_task_ids | self.safe_to_kill_task_ids)

    def is_safe_to_kill(self, task):
        return task.id in self.safe_to_kill_task_ids

    @classmethod
    def mark_arbitrary_task_as_safe_to_kill(cls):
        cls.safe_to_kill_task_ids.add(cls.downed_task_ids.pop())


@register_drain_method('hacheck')
class HacheckDrainMethod(DrainMethod):
    """This drain policy issues a POST to hacheck's /spool/{service}/{port}/status endpoint to cause healthchecks to
    fail. It considers tasks safe to kill if they've been down in hacheck for more than a specified delay."""

    def __init__(self, service, instance, nerve_ns, delay=120, hacheck_port=6666, expiration=0, **kwargs):
        super(HacheckDrainMethod, self).__init__(service, instance, nerve_ns)
        self.delay = float(delay)
        self.hacheck_port = hacheck_port
        self.expiration = float(expiration) or float(delay) * 10

    def spool_url(self, task):
        return 'http://%(task_host)s:%(hacheck_port)d/spool/%(service)s.%(nerve_ns)s/%(task_port)d/status' % {
            'task_host': task.host,
            'task_port': task.ports[0],
            'hacheck_port': self.hacheck_port,
            'service': self.service,
            'nerve_ns': self.nerve_ns,
        }

    def post_spool(self, task, status):
        resp = requests.post(
            self.spool_url(task),
            data={
                'status': status,
                'expiration': time.time() + self.expiration,
                'reason': 'Drained by Paasta',
            },
        )
        resp.raise_for_status()

    def get_spool(self, task):
        """Query hacheck for the state of a task, and parse the result into a dictionary."""
        response = requests.get(self.spool_url(task))
        if response.status_code == 200:
            return {
                'state': 'up',
            }

        regex = ''.join([
            "^",
            r"Service (?P<service>.+)",
            r" in (?P<state>.+) state",
            r"(?: since (?P<since>[0-9.]+))?",
            r"(?: until (?P<until>[0-9.]+))?",
            r"(?:: (?P<reason>.*))?",
            "$"
        ])
        match = re.match(regex, response.text)
        groupdict = match.groupdict()
        info = {}
        info['service'] = groupdict['service']
        info['state'] = groupdict['state']
        if 'since' in groupdict:
            info['since'] = float(groupdict['since'] or 0)
        if 'until' in groupdict:
            info['until'] = float(groupdict['until'] or 0)
        if 'reason' in groupdict:
            info['reason'] = groupdict['reason']
        return info

    def drain(self, task):
        self.post_spool(task, 'down')

    def stop_draining(self, task):
        self.post_spool(task, 'up')

    def is_draining(self, task):
        info = self.get_spool(task)
        if info["state"] == "up":
            return False
        else:
            return True

    def is_safe_to_kill(self, task):
        info = self.get_spool(task)
        if info["state"] == "up":
            return False
        else:
            return info.get("since", 0) < (time.time() - self.delay)
