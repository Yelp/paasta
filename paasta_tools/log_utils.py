import contextlib
import datetime
import io
import json
import sys
from collections import OrderedDict
from typing import Any
from typing import Callable
from typing import ContextManager
from typing import Iterable
from typing import Iterator
from typing import Type
from typing import TypeVar

from paasta_tools.text_utils import compose
from paasta_tools.text_utils import paasta_print
from paasta_tools.text_utils import PaastaColors
from paasta_tools.text_utils import remove_ansi_escape_sequences
from paasta_tools.utils import _AnyIO
from paasta_tools.utils import flock
from paasta_tools.utils import load_system_paasta_config

# Default values for _log
ANY_CLUSTER = 'N/A'
ANY_INSTANCE = 'N/A'
DEFAULT_LOGLEVEL = 'event'


class NoSuchLogLevel(Exception):
    pass


# The active log writer.
_log_writer = None
# The map of name -> LogWriter subclasses, used by configure_log.
_log_writer_classes = {}


class LogWriter(object):
    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()


_LogWriterTypeT = TypeVar('_LogWriterTypeT', bound=Type[LogWriter])


def register_log_writer(name: str) -> Callable[[_LogWriterTypeT], _LogWriterTypeT]:
    """Returns a decorator that registers that log writer class at a given name
    so get_log_writer_class can find it."""
    def outer(log_writer_class: _LogWriterTypeT) -> _LogWriterTypeT:
        _log_writer_classes[name] = log_writer_class
        return log_writer_class
    return outer


def get_log_writer_class(name: str) -> Type[LogWriter]:
    return _log_writer_classes[name]


def list_log_writers() -> Iterable[str]:
    return _log_writer_classes.keys()


def configure_log() -> None:
    """We will log to the yocalhost binded scribe."""
    log_writer_config = load_system_paasta_config().get_log_writer()
    global _log_writer
    LogWriterClass = get_log_writer_class(log_writer_config['driver'])
    _log_writer = LogWriterClass(**log_writer_config.get('options', {}))


def _log(
    service: str,
    line: str,
    component: str,
    level: str=DEFAULT_LOGLEVEL,
    cluster: str=ANY_CLUSTER,
    instance: str=ANY_INSTANCE,
) -> None:
    if _log_writer is None:
        configure_log()
    return _log_writer.log(
        service=service,
        line=line,
        component=component,
        level=level,
        cluster=cluster,
        instance=instance,
    )


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def format_log_line(
    level: str,
    cluster: str,
    service: str,
    instance: str,
    component: str,
    line: str,
    timestamp: str=None,
) -> str:
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    validate_log_component(component)
    if not timestamp:
        timestamp = _now()
    line = remove_ansi_escape_sequences(line)
    message = json.dumps(
        {
            'timestamp': timestamp,
            'level': level,
            'cluster': cluster,
            'service': service,
            'instance': instance,
            'component': component,
            'message': line,
        }, sort_keys=True,
    )
    return message


def get_log_name_for_service(service: str, prefix: str=None) -> str:
    if prefix:
        return 'stream_paasta_%s_%s' % (prefix, service)
    return 'stream_paasta_%s' % service


@register_log_writer('scribe')
class ScribeLogWriter(LogWriter):
    def __init__(
        self,
        scribe_host: str='169.254.255.254',
        scribe_port: int=1463,
        scribe_disable: bool=False,
        **kwargs: Any,
    ) -> None:
        self.clog = __import__('clog')
        self.clog.config.configure(scribe_host=scribe_host, scribe_port=scribe_port, scribe_disable=scribe_disable)

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        """This expects someone (currently the paasta cli main()) to have already
        configured the log object. We'll just write things to it.
        """
        if level == 'event':
            paasta_print("[service %s] %s" % (service, line), file=sys.stdout)
        elif level == 'debug':
            paasta_print("[service %s] %s" % (service, line), file=sys.stderr)
        else:
            raise NoSuchLogLevel
        log_name = get_log_name_for_service(service)
        formatted_line = format_log_line(level, cluster, service, instance, component, line)
        self.clog.log_line(log_name, formatted_line)


@register_log_writer('null')
class NullLogWriter(LogWriter):
    """A LogWriter class that doesn't do anything. Primarily useful for integration tests where we don't care about
    logs."""

    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        pass


@contextlib.contextmanager
def _empty_context() -> Iterator[None]:
    yield


@register_log_writer('file')
class FileLogWriter(LogWriter):
    def __init__(
        self,
        path_format: str,
        mode: str='a+',
        line_delimeter: str='\n',
        flock: bool=False,
    ) -> None:
        self.path_format = path_format
        self.mode = mode
        self.flock = flock
        self.line_delimeter = line_delimeter

    def maybe_flock(self, fd: _AnyIO) -> ContextManager:
        if self.flock:
            # https://github.com/python/typeshed/issues/1548
            return flock(fd)
        else:
            return _empty_context()

    def format_path(self, service: str, component: str, level: str, cluster: str, instance: str) -> str:
        return self.path_format.format(
            service=service,
            component=component,
            level=level,
            cluster=cluster,
            instance=instance,
        )

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        path = self.format_path(service, component, level, cluster, instance)

        # We use io.FileIO here because it guarantees that write() is implemented with a single write syscall,
        # and on Linux, writes to O_APPEND files with a single write syscall are atomic.
        #
        # https://docs.python.org/2/library/io.html#io.FileIO
        # http://article.gmane.org/gmane.linux.kernel/43445

        to_write = "%s%s" % (format_log_line(level, cluster, service, instance, component, line), self.line_delimeter)

        try:
            with io.FileIO(path, mode=self.mode, closefd=True) as f:
                with self.maybe_flock(f):
                    # remove type ignore comment below once https://github.com/python/typeshed/pull/1541 is merged.
                    f.write(to_write.encode('UTF-8'))  # type: ignore
        except IOError as e:
            paasta_print(
                "Could not log to %s: %s: %s -- would have logged: %s" % (path, type(e).__name__, str(e), to_write),
                file=sys.stderr,
            )


LOG_COMPONENTS = OrderedDict([
    (
        'build', {
            'color': PaastaColors.blue,
            'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
            'source_env': 'devc',
        },
    ),
    (
        'deploy', {
            'color': PaastaColors.cyan,
            'help': 'Output from the paasta deploy code. (setup_marathon_job, bounces, etc)',
            'additional_source_envs': ['devc'],
        },
    ),
    (
        'monitoring', {
            'color': PaastaColors.green,
            'help': 'Logs from Sensu checks for the service',
        },
    ),
    (
        'marathon', {
            'color': PaastaColors.magenta,
            'help': 'Logs from Marathon for the service',
        },
    ),
    (
        'chronos', {
            'color': PaastaColors.red,
            'help': 'Logs from Chronos for the service',
        },
    ),
    (
        'app_output', {
            'color': compose(PaastaColors.yellow, PaastaColors.bold),
            'help': 'Stderr and stdout of the actual process spawned by Mesos. '
                    'Convenience alias for both the stdout and stderr components',
        },
    ),
    (
        'stdout', {
            'color': PaastaColors.yellow,
            'help': 'Stdout from the process spawned by Mesos.',
        },
    ),
    (
        'stderr', {
            'color': PaastaColors.yellow,
            'help': 'Stderr from the process spawned by Mesos.',
        },
    ),
    (
        'security', {
            'color': PaastaColors.red,
            'help': 'Logs from security-related services such as firewall monitoring',
        },
    ),
    (
        'oom', {
            'color': PaastaColors.red,
            'help': 'Kernel OOM events.',
        },
    ),
    # I'm leaving these planned components here since they provide some hints
    # about where we want to go. See PAASTA-78.
    #
    # But I'm commenting them out so they don't delude users into believing we
    # can expose logs that we cannot actually expose. See PAASTA-927.
    #
    # ('app_request', {
    #     'color': PaastaColors.bold,
    #     'help': 'The request log for the service. Defaults to "service_NAME_requests"',
    #     'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
    # }),
    # ('app_errors', {
    #     'color': PaastaColors.red,
    #     'help': 'Application error log, defaults to "stream_service_NAME_errors"',
    #     'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
    # }),
    # ('lb_requests', {
    #     'color': PaastaColors.bold,
    #     'help': 'All requests from Smartstack haproxy',
    #     'command': 'NA - TODO: SRV-1130',
    # }),
    # ('lb_errors', {
    #     'color': PaastaColors.red,
    #     'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
    #     'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
    # }),
])


class NoSuchLogComponent(Exception):
    pass


def validate_log_component(component: str) -> bool:
    if component in LOG_COMPONENTS.keys():
        return True
    else:
        raise NoSuchLogComponent
