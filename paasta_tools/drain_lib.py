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
    def down(cls, task):
        pass

    def up(cls, task):
        pass

    def is_downed(cls, task):
        return False

    def safe_to_kill(cls, task):
        return True
