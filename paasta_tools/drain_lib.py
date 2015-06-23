class DrainMethod(object):
    """A drain method is a way of stopping new traffic to tasks without killing them. For example, you might take a task
    out of a load balancer by causing its healthchecks to fail.

    When implementing a drain method, be sure to decorate with @DrainMethod.register(name)
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
    def get_drain_method(name):
        return DrainMethod._drain_methods[name]

    @classmethod
    def down(self, task):
        """Make a task stop receiving new traffic."""
        raise NotImplementedError()

    @classmethod
    def up(self, task):
        """Make a task that has previously been downed start receiving traffic again."""
        raise NotImplementedError()

    @classmethod
    def is_downed(self, task):
        """Return False if a task is not downed, or a number of seconds that the task has been downed for."""
        raise NotImplementedError()


class DrainPolicy(object):
    """A drain policy decides whether a task can be safely killed. When implementing a drain policy, be sure to decorate
    with @DrainPolicy.register(name)."""

    _drain_policies = {}

    @staticmethod
    def register(name):
        """Returns a decorator that registers that bounce function at a given name
        so get_bounce_method_func can find it."""
        def outer(drain_policy):
            DrainPolicy._drain_policies[name] = drain_policy
            return drain_policy
        return outer

    @staticmethod
    def get_drain_policy(name):
        return DrainPolicy._drain_policies[name]

    @classmethod
    def safe_to_kill(cls, task):
        raise NotImplementedError()


@DrainPolicy.register('brutal')
class BrutalDrainPolicy(DrainPolicy):
    """This drain policy assumes every task is safe to kill."""
    @classmethod
    def safe_to_kill(self, task):
        return True


@DrainMethod.register('noop')
class NoopDrainMethod(DrainMethod):
    @classmethod
    def down(cls, task):
        pass

    @classmethod
    def up(cls, task):
        pass

    @classmethod
    def is_downed(cls, task):
        return False
