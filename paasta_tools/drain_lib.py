import re
import requests
import time


_drain_methods = {}


def register_drain_method(name):
    """Returns a decorator that registers that bounce function at a given name
    so get_bounce_method_func can find it."""
    def outer(drain_method):
        _drain_methods[name] = drain_method
        return drain_method
    return outer


def get_drain_method(name, service_name, instance_name, nerve_ns, drain_method_params=None):
    return _drain_methods[name](service_name, instance_name, nerve_ns, **(drain_method_params or {}))


class DrainMethod(object):
    """A drain method is a way of stopping new traffic to tasks without killing them. For example, you might take a task
    out of a load balancer by causing its healthchecks to fail.

    A drain method must have the following methods:
     - down(task): Begin draining traffic from a task. This should be idempotent.
     - up(task): Stop draining traffic from a task. This should be idempotent.
     - is_downed(task): Whether a task has already been marked as downed. Note that this state should be stored out of
                        process, because a bounce may take multiple runs of setup_marathon_job to complete.
     - safe_to_kill(task): Return True if this task is safe to kill, False otherwise.

    When implementing a drain method, be sure to decorate with @register_drain_method(name).
    """

    def __init__(self, service_name, instance_name, nerve_ns):
        self.service_name = service_name
        self.instance_name = instance_name
        self.nerve_ns = nerve_ns

    def down(self, task):
        """Make a task stop receiving new traffic."""
        raise NotImplementedError()

    def up(self, task):
        """Make a task that has previously been downed start receiving traffic again."""
        raise NotImplementedError()

    def is_downed(self, task):
        """Return whether a task is being drained."""
        raise NotImplementedError()

    def safe_to_kill(self, task):
        """Return True if a task is drained and ready to be killed, or False if we should wait."""
        raise NotImplementedError()


@register_drain_method('noop')
class NoopDrainMethod(DrainMethod):
    """This drain policy does nothing and assumes every task is safe to kill."""
    def down(self, task):
        pass

    def up(self, task):
        pass

    def is_downed(self, task):
        return False

    def safe_to_kill(self, task):
        return True


@register_drain_method('test')
class TestDrainMethod(DrainMethod):
    """This drain policy is meant for integration testing. Do not use."""

    # These are variables on the class for ease of use in testing.
    downed_task_ids = set()
    safe_to_kill_task_ids = set()

    def down(self, task):
        self.downed_task_ids.add(task.id)

    def up(self, task):
        self.downed_task_ids.remove(task.id)
        self.safe_to_kill_task_ids.remove(task.id)

    def is_downed(self, task):
        return task.id in (self.downed_task_ids | self.safe_to_kill_task_ids)

    def safe_to_kill(self, task):
        return task.id in self.safe_to_kill_task_ids

    @classmethod
    def mark_a_task_as_safe_to_kill(cls):
        cls.safe_to_kill_task_ids.add(cls.downed_task_ids.pop())


@register_drain_method('hacheck')
class HacheckDrainMethod(DrainMethod):
    """This drain policy does nothing and assumes every task is safe to kill."""
    def __init__(self, service_name, instance_name, nerve_ns, delay=120, hacheck_port=6666, expiration=0):
        super(HacheckDrainMethod, self).__init__(service_name, instance_name, nerve_ns)
        self.delay = float(delay)
        self.hacheck_port = hacheck_port
        self.expiration = float(expiration) or float(delay) * 10

    def spool_url(self, task):
        return 'http://%(task_host)s:%(hacheck_port)d/spool/%(service_name)s/%(task_port)d/status' % {
            'task_host': task.host,
            'task_port': task.ports[0],
            'hacheck_port': self.hacheck_port,
            'service_name': self.service_name,
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
        response = requests.get(self.spool_url(task))
        if response.status_code == 200:
            return False

        regex = ''.join([
            "^",
            r"Service (?P<service_name>.*)",
            r" in (?P<state>.*) state",
            r"(?: since (?P<since>[0-9.]+))?",
            r"(?: until (?P<until>[0-9.]+))?",
            r"(?:: (?P<reason>.*))?",
            "$"
        ])
        match = re.match(regex, response.text)
        groupdict = match.groupdict()
        info = {}
        if 'since' in groupdict:
            info['since'] = float(groupdict['since'] or 0)
        if 'until' in groupdict:
            info['until'] = float(groupdict['until'] or 0)
        if 'reason' in groupdict:
            info['reason'] = groupdict['reason']
        if 'service_name' in groupdict:
            info['service_name'] = groupdict['service_name']
        if 'state' in groupdict:
            info['state'] = groupdict['state']
        return info

    def down(self, task):
        self.post_spool(task, 'down')

    def up(self, task):
        self.post_spool(task, 'up')

    def is_downed(self, task):
        info = self.get_spool(task)
        if info["state"] == "up":
            return False
        else:
            return True

    def safe_to_kill(self, task):
        info = self.get_spool(task)
        if info["state"] == "up":
            return False
        else:
            return info.get("since", 0) < (time.time() - self.delay)
