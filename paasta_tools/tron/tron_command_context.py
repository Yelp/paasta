"""Command Context is how we construct the command line for a command which may
have variables that need to be rendered.
This is a COPY of https://github.com/Yelp/Tron/blob/master/tron/command_context.py.
"""
import operator

from paasta_tools.tron import tron_timeutils


class CommandContext:
    """A CommandContext object is a wrapper around any object which has values
    to be used to render a command for execution.  It looks up values by name.

    It's lookup order is:
        base[name],
        base.__getattr__(name),
        next[name],
        next.__getattr__(name)
    """

    def __init__(self, base=None, next=None):
        """
          base - Object to look for attributes in
          next - Next place to look for more pieces of context
                 Generally this will be another instance of CommandContext
        """
        self.base = base or {}
        self.next = next or {}

    def get(self, name, default=None):
        try:
            return self.__getitem__(name)
        except KeyError:
            return default

    def __getitem__(self, name):
        getters = [operator.itemgetter(name), operator.attrgetter(name)]
        for target in [self.base, self.next]:
            for getter in getters:
                try:
                    return getter(target)
                except (KeyError, TypeError, AttributeError):
                    pass

        raise KeyError(name)

    def __eq__(self, other):
        return self.base == other.base and self.next == other.next

    def __ne__(self, other):
        return not self == other


class JobRunContext:

    def __init__(self, job_run):
        self.job_run = job_run

    def __getitem__(self, name):
        """Attempt to parse date arithmetic syntax and apply to run_time."""
        run_time = self.job_run.run_time
        time_value = tron_timeutils.DateArithmetic.parse(name, run_time)
        if time_value:
            return time_value

        raise KeyError(name)
