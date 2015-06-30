class DrainMethod(object):
    """A drain method is a way of stopping new traffic to tasks without killing them. For example, you might take a task
    out of a load balancer by causing its healthchecks to fail.

    A drain method must have the following methods:
     - down(task): Begin draining traffic from a task. This should be idempotent.
     - up(task): Stop draining traffic from a task. This should be idempotent.
     - is_downed(task): Whether a task has already been marked as downed. Note that this state should be stored out of
                        process, because a bounce may take multiple runs of setup_marathon_job to complete.
     - safe_to_kill(task): Return True if this task is safe to kill, False otherwise.

    When implementing a drain method, be sure to decorate with @DrainMethod.register(name).
    """
    _drain_methods = {}

    @staticmethod
    def register(name):
        """Returns a decorator that registers that bounce function at a given name
        so get_bounce_method_func can find it."""
        def outer(drain_method):
            DrainMethod._drain_methods[name] = drain_method
            return drain_method
        return outer

    @staticmethod
    def get_drain_method(name, drain_method_params=None):
        return DrainMethod._drain_methods[name](**(drain_method_params or {}))

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


@DrainMethod.register('noop')
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


@DrainMethod.register('test')
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
