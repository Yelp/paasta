"""Command Context is how we construct the command line for a command which may
have variables that need to be rendered.
This is a COPY of https://github.com/Yelp/Tron/blob/master/tron/command_context.py.
"""
import functools
import operator

from paasta_tools.tron import tron_timeutils


def build_context(object, parent):
    """Construct a CommandContext for object. object must have a property
    'context_class'.
    """
    return CommandContext(object.context_class(object), parent)


def build_filled_context(*context_objects):
    """Create a CommandContext chain from context_objects, using a Filler
    object to pass to each CommandContext. Can be used to validate a format
    string.
    """
    if not context_objects:
        return CommandContext()

    filler = Filler()

    def build(current, next):
        return CommandContext(next(filler), current)

    return functools.reduce(build, context_objects, None)


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


class JobContext:
    """A class which exposes properties for rendering commands."""

    def __init__(self, job):
        self.job = job

    @property
    def name(self):
        return self.job.name

    def __getitem__(self, item):
        date_name, date_spec = self._get_date_spec_parts(item)
        if not date_spec:
            raise KeyError(item)

        if date_name == "last_success":
            last_success = self.job.runs.last_success
            last_success = last_success.run_time if last_success else None

            time_value = tron_timeutils.DateArithmetic.parse(date_spec, last_success)
            if time_value:
                return time_value

        raise KeyError(item)

    def _get_date_spec_parts(self, name):
        parts = name.rsplit(":", 1)
        if len(parts) != 2:
            return name, None
        return parts


class JobRunContext:
    def __init__(self, job_run):
        self.job_run = job_run

    @property
    def runid(self):
        return self.job_run.id

    @property
    def cleanup_job_status(self):
        """Provide 'SUCCESS' or 'FAILURE' to a cleanup action context based on
        the status of the other steps
        """
        if self.job_run.action_runs.is_failed:
            return "FAILURE"
        elif self.job_run.action_runs.is_complete_without_cleanup:
            return "SUCCESS"
        return "UNKNOWN"

    def __getitem__(self, name):
        """Attempt to parse date arithmetic syntax and apply to run_time."""
        run_time = self.job_run.run_time
        time_value = tron_timeutils.DateArithmetic.parse(name, run_time)
        if time_value:
            return time_value

        raise KeyError(name)


class ActionRunContext:
    """Context object that gives us access to data about the action run."""

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def actionname(self):
        return self.action_run.action_name

    @property
    def node(self):
        return self.action_run.node.hostname


class ServiceInstancePidContext:
    def __init__(self, service_instance):
        self.service_instance = service_instance

    @property
    def instance_number(self):
        return self.service_instance.instance_number

    @property
    def node(self):
        return self.service_instance.node.hostname

    @property
    def name(self):
        return self.service_instance.config.name


class ServiceInstanceContext(ServiceInstancePidContext):
    @property
    def pid_file(self):
        context = CommandContext(self, self.service_instance.parent_context)
        return self.service_instance.config.pid_file % context


class Filler:
    """Filler object for using CommandContext during config parsing. This class
    is used as a substitute for objects that would be passed to Context objects.
    This allows the Context objects to be used directly for config validation.
    """

    def __getattr__(self, _):
        return self

    def __str__(self):
        return "%(...)s"

    def __mod__(self, _):
        return self

    def __nonzero__(self):
        return False
