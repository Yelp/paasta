# Copyright 2015-2017 Yelp Inc.
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
import contextlib
import datetime
import getpass
import hashlib
import io
import json
import logging
import os
import pwd
import re
import shlex
import signal
import socket
import sys
import tempfile
import threading
import warnings
from collections import OrderedDict
from enum import Enum
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT
from types import FrameType
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import ContextManager
from typing import Dict
from typing import IO
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

import choice
import dateutil.tz
import requests_cache
import service_configuration_lib
from docker import Client
from docker.utils import kwargs_from_env

import paasta_tools.cli.fsm
from paasta_tools.util.config_loading import build_docker_image_name
from paasta_tools.util.config_loading import build_docker_tag
from paasta_tools.util.config_loading import load_system_paasta_config
from paasta_tools.util.const import DEFAULT_SOA_DIR
from paasta_tools.util.lock import _AnyIO
from paasta_tools.util.lock import flock
from paasta_tools.util.timeout import _timeout


# Default values for _log
ANY_CLUSTER = "N/A"
ANY_INSTANCE = "N/A"
DEFAULT_LOGLEVEL = "event"
no_escape = re.compile(r"\x1B\[[0-9;]*[mK]")

# instead of the convention of using underscores in this scribe channel name,
# the audit log uses dashes to prevent collisions with a service that might be
# named 'audit_log'
AUDIT_LOG_STREAM = "stream_paasta-audit-log"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class RollbackTypes(Enum):
    AUTOMATIC_SLO_ROLLBACK = "automatic_slo_rollback"
    USER_INITIATED_ROLLBACK = "user_initiated_rollback"


_ComposeRetT = TypeVar("_ComposeRetT")
_ComposeInnerRetT = TypeVar("_ComposeInnerRetT")


def compose(
    func_one: Callable[[_ComposeInnerRetT], _ComposeRetT],
    func_two: Callable[..., _ComposeInnerRetT],
) -> Callable[..., _ComposeRetT]:
    def composed(*args: Any, **kwargs: Any) -> _ComposeRetT:
        return func_one(func_two(*args, **kwargs))

    return composed


class PaastaColors:

    """Collection of static variables and methods to assist in coloring text."""

    # ANSI color codes
    BLUE = "\033[34m"
    BOLD = "\033[1m"
    CYAN = "\033[36m"
    DEFAULT = "\033[0m"
    GREEN = "\033[32m"
    GREY = "\033[38;5;242m"
    MAGENTA = "\033[35m"
    RED = "\033[31m"
    YELLOW = "\033[33m"

    @staticmethod
    def bold(text: str) -> str:
        """Return bolded text.

        :param text: a string
        :return: text color coded with ANSI bold
        """
        return PaastaColors.color_text(PaastaColors.BOLD, text)

    @staticmethod
    def blue(text: str) -> str:
        """Return text that can be printed blue.

        :param text: a string
        :return: text color coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text: str) -> str:
        """Return text that can be printed green.

        :param text: a string
        :return: text color coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text: str) -> str:
        """Return text that can be printed red.

        :param text: a string
        :return: text color coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def magenta(text: str) -> str:
        """Return text that can be printed magenta.

        :param text: a string
        :return: text color coded with ANSI magenta"""
        return PaastaColors.color_text(PaastaColors.MAGENTA, text)

    @staticmethod
    def color_text(color: str, text: str) -> str:
        """Return text that can be printed color.

        :param color: ANSI color code
        :param text: a string
        :return: a string with ANSI color encoding"""
        # any time text returns to default, we want to insert our color.
        replaced = text.replace(PaastaColors.DEFAULT, PaastaColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + PaastaColors.DEFAULT

    @staticmethod
    def cyan(text: str) -> str:
        """Return text that can be printed cyan.

        :param text: a string
        :return: text color coded with ANSI cyan"""
        return PaastaColors.color_text(PaastaColors.CYAN, text)

    @staticmethod
    def yellow(text: str) -> str:
        """Return text that can be printed yellow.

        :param text: a string
        :return: text color coded with ANSI yellow"""
        return PaastaColors.color_text(PaastaColors.YELLOW, text)

    @staticmethod
    def grey(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.GREY, text)

    @staticmethod
    def default(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.DEFAULT, text)


LOG_COMPONENTS: Mapping[str, Mapping[str, Any]] = OrderedDict(
    [
        (
            "build",
            {
                "color": PaastaColors.blue,
                "help": "Jenkins build jobs output, like the itest, promotion, security checks, etc.",
                "source_env": "devc",
            },
        ),
        (
            "deploy",
            {
                "color": PaastaColors.cyan,
                "help": "Output from the paasta deploy code. (setup_marathon_job, bounces, etc)",
                "additional_source_envs": ["devc"],
            },
        ),
        (
            "monitoring",
            {
                "color": PaastaColors.green,
                "help": "Logs from Sensu checks for the service",
            },
        ),
        (
            "marathon",
            {
                "color": PaastaColors.magenta,
                "help": "Logs from Marathon for the service",
            },
        ),
        (
            "app_output",
            {
                "color": compose(PaastaColors.yellow, PaastaColors.bold),
                "help": "Stderr and stdout of the actual process spawned by Mesos. "
                "Convenience alias for both the stdout and stderr components",
            },
        ),
        (
            "stdout",
            {
                "color": PaastaColors.yellow,
                "help": "Stdout from the process spawned by Mesos.",
            },
        ),
        (
            "stderr",
            {
                "color": PaastaColors.yellow,
                "help": "Stderr from the process spawned by Mesos.",
            },
        ),
        (
            "security",
            {
                "color": PaastaColors.red,
                "help": "Logs from security-related services such as firewall monitoring",
            },
        ),
        ("oom", {"color": PaastaColors.red, "help": "Kernel OOM events."}),
        (
            "task_lifecycle",
            {
                "color": PaastaColors.bold,
                "help": "Logs that tell you about task startup, failures, healthchecks, etc.",
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
    ]
)


class NoSuchLogComponent(Exception):
    pass


def validate_log_component(component: str) -> bool:
    if component in LOG_COMPONENTS.keys():
        return True
    else:
        raise NoSuchLogComponent


def get_git_url(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> str:
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@github.yelpcorp.com:services/test.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    # TODO: PAASTA-16927: get this from system config `.git_config`
    default_location = format_git_url(
        "git", "github.yelpcorp.com", f"services/{service}"
    )
    return general_config.get("git_url", default_location)


def format_git_url(git_user: str, git_server: str, repo_name: str) -> str:
    return f"{git_user}@{git_server}:{repo_name}"


class NoSuchLogLevel(Exception):
    pass


# The active log writer.
_log_writer = None
# The map of name -> LogWriter subclasses, used by configure_log.
_log_writer_classes = {}


class LogWriter:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()


_LogWriterTypeT = TypeVar("_LogWriterTypeT", bound=Type[LogWriter])


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
    LogWriterClass = get_log_writer_class(log_writer_config["driver"])
    _log_writer = LogWriterClass(**log_writer_config.get("options", {}))


def _log(
    service: str,
    line: str,
    component: str,
    level: str = DEFAULT_LOGLEVEL,
    cluster: str = ANY_CLUSTER,
    instance: str = ANY_INSTANCE,
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


def _log_audit(
    action: str,
    action_details: dict = None,
    service: str = None,
    cluster: str = ANY_CLUSTER,
    instance: str = ANY_INSTANCE,
) -> None:
    if _log_writer is None:
        configure_log()

    user = get_username()
    host = get_hostname()

    return _log_writer.log_audit(
        user=user,
        host=host,
        action=action,
        action_details=action_details,
        service=service,
        cluster=cluster,
        instance=instance,
    )


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def remove_ansi_escape_sequences(line: str) -> str:
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub("", line)


def format_log_line(
    level: str,
    cluster: str,
    service: str,
    instance: str,
    component: str,
    line: str,
    timestamp: str = None,
) -> str:
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    validate_log_component(component)
    if not timestamp:
        timestamp = _now()
    line = remove_ansi_escape_sequences(line.strip())
    message = json.dumps(
        {
            "timestamp": timestamp,
            "level": level,
            "cluster": cluster,
            "service": service,
            "instance": instance,
            "component": component,
            "message": line,
        },
        sort_keys=True,
    )
    return message


def format_audit_log_line(
    cluster: str,
    instance: str,
    user: str,
    host: str,
    action: str,
    action_details: dict = None,
    service: str = None,
    timestamp: str = None,
) -> str:
    """Accepts:

        * a string 'user' describing the user that initiated the action
        * a string 'host' describing the server where the user initiated the action
        * a string 'action' describing an action performed by paasta_tools
        * a dict 'action_details' optional information about the action

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains details about an action performed on
    a service/instance.
    """
    if not timestamp:
        timestamp = _now()
    if not action_details:
        action_details = {}

    message = json.dumps(
        {
            "timestamp": timestamp,
            "cluster": cluster,
            "service": service,
            "instance": instance,
            "user": user,
            "host": host,
            "action": action,
            "action_details": action_details,
        },
        sort_keys=True,
    )
    return message


def get_log_name_for_service(service: str, prefix: str = None) -> str:
    if prefix:
        return f"stream_paasta_{prefix}_{service}"
    return "stream_paasta_%s" % service


try:
    import clog

    # Somehow clog turns on DeprecationWarnings, so we need to disable them
    # again after importing it.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    class CLogWriter(LogWriter):
        def __init__(self, **kwargs: Any):
            clog.config.configure(**kwargs)

        def log(
            self,
            service: str,
            line: str,
            component: str,
            level: str = DEFAULT_LOGLEVEL,
            cluster: str = ANY_CLUSTER,
            instance: str = ANY_INSTANCE,
        ) -> None:
            """This expects someone (currently the paasta cli main()) to have already
            configured the log object. We'll just write things to it.
            """
            if level == "event":
                print(f"[service {service}] {line}", file=sys.stdout)
            elif level == "debug":
                print(f"[service {service}] {line}", file=sys.stderr)
            else:
                raise NoSuchLogLevel
            log_name = get_log_name_for_service(service)
            formatted_line = format_log_line(
                level, cluster, service, instance, component, line
            )
            clog.log_line(log_name, formatted_line)

        def log_audit(
            self,
            user: str,
            host: str,
            action: str,
            action_details: dict = None,
            service: str = None,
            cluster: str = ANY_CLUSTER,
            instance: str = ANY_INSTANCE,
        ) -> None:
            log_name = AUDIT_LOG_STREAM
            formatted_line = format_audit_log_line(
                user=user,
                host=host,
                action=action,
                action_details=action_details,
                service=service,
                cluster=cluster,
                instance=instance,
            )
            clog.log_line(log_name, formatted_line)

    @register_log_writer("monk")
    class MonkLogWriter(CLogWriter):
        def __init__(
            self,
            monk_host: str = "169.254.255.254",
            monk_port: int = 1473,
            monk_disable: bool = False,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                monk_host=monk_host, monk_port=monk_port, monk_disable=monk_disable,
            )

    @register_log_writer("scribe")
    class ScribeLogWriter(CLogWriter):
        def __init__(
            self,
            scribe_host: str = "169.254.255.254",
            scribe_port: int = 1463,
            scribe_disable: bool = False,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                scribe_host=scribe_host,
                scribe_port=scribe_port,
                scribe_disable=scribe_disable,
            )


except ImportError:
    warnings.warn("clog is unavailable")


@register_log_writer("null")
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
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        pass

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        pass


@contextlib.contextmanager
def _empty_context() -> Iterator[None]:
    yield


@register_log_writer("file")
class FileLogWriter(LogWriter):
    def __init__(
        self,
        path_format: str,
        mode: str = "a+",
        line_delimiter: str = "\n",
        flock: bool = False,
    ) -> None:
        self.path_format = path_format
        self.mode = mode
        self.flock = flock
        self.line_delimiter = line_delimiter

    def maybe_flock(self, fd: _AnyIO) -> ContextManager:
        if self.flock:
            # https://github.com/python/typeshed/issues/1548
            return flock(fd)
        else:
            return _empty_context()

    def format_path(
        self, service: str, component: str, level: str, cluster: str, instance: str
    ) -> str:
        return self.path_format.format(
            service=service,
            component=component,
            level=level,
            cluster=cluster,
            instance=instance,
        )

    def _log_message(self, path: str, message: str) -> None:
        # We use io.FileIO here because it guarantees that write() is implemented with a single write syscall,
        # and on Linux, writes to O_APPEND files with a single write syscall are atomic.
        #
        # https://docs.python.org/2/library/io.html#io.FileIO
        # http://article.gmane.org/gmane.linux.kernel/43445

        try:
            with io.FileIO(path, mode=self.mode, closefd=True) as f:
                with self.maybe_flock(f):
                    f.write(message.encode("UTF-8"))
        except IOError as e:
            print(
                "Could not log to {}: {}: {} -- would have logged: {}".format(
                    path, type(e).__name__, str(e), message
                ),
                file=sys.stderr,
            )

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        path = self.format_path(service, component, level, cluster, instance)
        to_write = "{}{}".format(
            format_log_line(level, cluster, service, instance, component, line),
            self.line_delimiter,
        )

        self._log_message(path, to_write)

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        path = self.format_path(AUDIT_LOG_STREAM, "", "", cluster, instance)
        formatted_line = format_audit_log_line(
            user=user,
            host=host,
            action=action,
            action_details=action_details,
            service=service,
            cluster=cluster,
            instance=instance,
        )

        to_write = f"{formatted_line}{self.line_delimiter}"

        self._log_message(path, to_write)


def _run(
    command: Union[str, List[str]],
    env: Mapping[str, str] = os.environ,
    timeout: float = None,
    log: bool = False,
    stream: bool = False,
    stdin: Any = None,
    stdin_interrupt: bool = False,
    popen_kwargs: Dict = {},
    **kwargs: Any,
) -> Tuple[int, str]:
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timeout
        seconds.
    :param log: If True, the _log will be handled by _run. If set, it is mandatory
        to pass at least a :service: and a :component: parameter. Optionally you
        can pass :cluster:, :instance: and :loglevel: parameters for logging.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    output: List[str] = []
    if log:
        service = kwargs["service"]
        component = kwargs["component"]
        cluster = kwargs.get("cluster", ANY_CLUSTER)
        instance = kwargs.get("instance", ANY_INSTANCE)
        loglevel = kwargs.get("loglevel", DEFAULT_LOGLEVEL)
    try:
        if not isinstance(command, list):
            command = shlex.split(command)
        popen_kwargs["stdout"] = PIPE
        popen_kwargs["stderr"] = STDOUT
        popen_kwargs["stdin"] = stdin
        popen_kwargs["env"] = env
        process = Popen(command, **popen_kwargs)

        if stdin_interrupt:

            def signal_handler(signum: int, frame: FrameType) -> None:
                process.stdin.write("\n".encode("utf-8"))
                process.stdin.flush()
                process.wait()

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, [process])
            proctimer.start()

        outfn: Any = print if stream else output.append
        for linebytes in iter(process.stdout.readline, b""):
            line = linebytes.decode("utf-8", errors="replace").rstrip("\n")
            outfn(line)

            if log:
                _log(
                    service=service,
                    line=line,
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
        # when finished, get the exit code
        process.wait()
        returncode = process.returncode
    except OSError as e:
        if log:
            _log(
                service=service,
                line=e.strerror.rstrip("\n"),
                component=component,
                level=loglevel,
                cluster=cluster,
                instance=instance,
            )
        output.append(e.strerror.rstrip("\n"))
        returncode = e.errno
    except (KeyboardInterrupt, SystemExit):
        # need to clean up the timing thread here
        if timeout:
            proctimer.cancel()
        raise
    else:
        # Stop the timer
        if timeout:
            proctimer.cancel()
    if returncode == -9:
        output.append(f"Command '{command}' timed out (longer than {timeout}s)")
    return returncode, "\n".join(output)


def get_umask() -> int:
    """Get the current umask for this process. NOT THREAD SAFE."""
    old_umask = os.umask(0o0022)
    os.umask(old_umask)
    return old_umask


def get_user_agent() -> str:
    base_name = os.path.basename(sys.argv[0])
    if base_name == "gunicorn":
        return f"{sys.argv[-1]} {paasta_tools.__version__}"
    elif len(sys.argv) >= 1:
        return f"{base_name} {paasta_tools.__version__}"
    else:
        return f"PaaSTA Tools {paasta_tools.__version__}"


@contextlib.contextmanager
def atomic_file_write(target_path: str) -> Iterator[IO]:
    dirname = os.path.dirname(target_path)
    basename = os.path.basename(target_path)

    if target_path == "-":
        yield sys.stdout
    else:
        with tempfile.NamedTemporaryFile(
            dir=dirname, prefix=(".%s-" % basename), delete=False, mode="w"
        ) as f:
            temp_target_path = f.name
            yield f

        mode = 0o0666 & (~get_umask())
        os.chmod(temp_target_path, mode)
        os.rename(temp_target_path, target_path)


def check_docker_image(service: str, tag: str) -> bool:
    """Checks whether the given image for :service: with :tag: exists.

    :raises: ValueError if more than one docker image with :tag: found.
    :returns: True if there is exactly one matching image found.
    """
    docker_client = get_docker_client()
    image_name = build_docker_image_name(service)
    docker_tag = build_docker_tag(service, tag)
    images = docker_client.images(name=image_name)
    # image['RepoTags'] may be None
    # Fixed upstream but only in docker-py 2.
    # https://github.com/docker/docker-py/issues/1401
    result = [image for image in images if docker_tag in (image["RepoTags"] or [])]
    if len(result) > 1:
        raise ValueError(
            f"More than one docker image found with tag {docker_tag}\n{result}"
        )
    return len(result) == 1


def datetime_from_utc_to_local(utc_datetime: datetime.datetime) -> datetime.datetime:
    return datetime_convert_timezone(
        utc_datetime, dateutil.tz.tzutc(), dateutil.tz.tzlocal()
    )


def datetime_convert_timezone(
    dt: datetime.datetime, from_zone: datetime.tzinfo, to_zone: datetime.tzinfo
) -> datetime.datetime:
    dt = dt.replace(tzinfo=from_zone)
    converted_datetime = dt.astimezone(to_zone)
    converted_datetime = converted_datetime.replace(tzinfo=None)
    return converted_datetime


def get_username() -> str:
    """Returns the current username in a portable way. Will use the SUDO_USER
    environment variable if present.
    http://stackoverflow.com/a/2899055
    """
    return os.environ.get("SUDO_USER", pwd.getpwuid(os.getuid())[0])


def get_hostname() -> str:
    """Returns the fully-qualified domain name of the server this code is
    running on.
    """
    return socket.getfqdn()


def get_docker_host() -> str:
    return os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock")


def get_docker_client() -> Client:
    client_opts = kwargs_from_env(assert_hostname=False)
    if "base_url" in client_opts:
        return Client(**client_opts)
    else:
        return Client(base_url=get_docker_host(), **client_opts)


def get_running_mesos_docker_containers() -> List[Dict]:
    client = get_docker_client()
    running_containers = client.containers()
    return [
        container
        for container in running_containers
        if "mesos-" in container["Names"][0]
    ]


def print_with_indent(line: str, indent: int = 2) -> None:
    """Print a line with a given indent level"""
    print(" " * indent + line)


def parse_timestamp(tstamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(tstamp, "%Y%m%dT%H%M%S")


def format_timestamp(dt: datetime.datetime = None) -> str:
    if dt is None:
        dt = datetime.datetime.utcnow()
    return dt.strftime("%Y%m%dT%H%M%S")


def get_paasta_tag_from_deploy_group(identifier: str, desired_state: str) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return f"paasta-{identifier}-{timestamp}-{desired_state}"


def get_paasta_tag(cluster: str, instance: str, desired_state: str) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return f"paasta-{cluster}.{instance}-{timestamp}-{desired_state}"


def format_tag(tag: str) -> str:
    return "refs/tags/%s" % tag


def get_config_hash(config: Any, force_bounce: str = None) -> str:
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything really, so long as str(config) works. Returns
    the first 8 characters so things are not really long.

    :param config: The configuration to hash
    :param force_bounce: a timestamp (in the form of a string) that is appended before hashing
                         that can be used to force a hash change
    :returns: A MD5 hash of str(config)
    """
    hasher = hashlib.md5()
    hasher.update(
        json.dumps(config, sort_keys=True).encode("UTF-8")
        + (force_bounce or "").encode("UTF-8")
    )
    return "config%s" % hasher.hexdigest()[:8]


def is_under_replicated(
    num_available: int, expected_count: int, crit_threshold: int
) -> Tuple[bool, float]:
    """Calculates if something is under replicated

    :param num_available: How many things are up
    :param expected_count: How many things you think should be up
    :param crit_threshold: Int from 0-100
    :returns: Tuple of (bool, ratio)
    """
    if expected_count == 0:
        ratio = 100.0
    else:
        ratio = (num_available / float(expected_count)) * 100

    if ratio < int(crit_threshold):
        return (True, ratio)
    else:
        return (False, ratio)


def terminal_len(text: str) -> int:
    """Return the number of characters that text will take up on a terminal. """
    return len(remove_ansi_escape_sequences(text))


def format_table(
    rows: Iterable[Union[str, Sequence[str]]], min_spacing: int = 2
) -> List[str]:
    """Formats a table for use on the command line.

    :param rows: List of rows, each of which can either be a tuple of strings containing the row's values, or a string
                 to be inserted verbatim. Each row (except literal strings) should be the same number of elements as
                 all the others.
    :returns: A string containing rows formatted as a table.
    """

    list_rows = [r for r in rows if not isinstance(r, str)]

    # If all of the rows are strings, we have nothing to do, so short-circuit.
    if not list_rows:
        return cast(List[str], rows)

    widths = []
    for i in range(len(list_rows[0])):
        widths.append(max(terminal_len(r[i]) for r in list_rows))

    expanded_rows = []
    for row in rows:
        if isinstance(row, str):
            expanded_rows.append([row])
        else:
            expanded_row = []
            for i, cell in enumerate(row):
                if i == len(row) - 1:
                    padding = ""
                else:
                    padding = " " * (widths[i] - terminal_len(cell))
                expanded_row.append(cell + padding)
            expanded_rows.append(expanded_row)

    return [(" " * min_spacing).join(r) for r in expanded_rows]


def calculate_tail_lines(verbose_level: int) -> int:
    if verbose_level <= 1:
        return 0
    else:
        return 10 ** (verbose_level - 1)


_UseRequestsCacheFuncT = TypeVar("_UseRequestsCacheFuncT", bound=Callable)


def use_requests_cache(
    cache_name: str, backend: str = "memory", **kwargs: Any
) -> Callable[[_UseRequestsCacheFuncT], _UseRequestsCacheFuncT]:
    def wrap(fun: _UseRequestsCacheFuncT) -> _UseRequestsCacheFuncT:
        def fun_with_cache(*args: Any, **kwargs: Any) -> Any:
            requests_cache.install_cache(cache_name, backend=backend, **kwargs)
            result = fun(*args, **kwargs)
            requests_cache.uninstall_cache()
            return result

        return cast(_UseRequestsCacheFuncT, fun_with_cache)

    return wrap


def mean(iterable: Collection[float]) -> float:
    """
    Returns the average value of an iterable
    """
    return sum(iterable) / len(iterable)


def prompt_pick_one(sequence: Collection[str], choosing: str) -> str:
    if not sys.stdin.isatty():
        print(
            "No {choosing} specified and no TTY present to ask."
            "Please specify a {choosing} using the cli.".format(choosing=choosing),
            file=sys.stderr,
        )
        sys.exit(1)

    if not sequence:
        print(
            f"PaaSTA needs to pick a {choosing} but none were found.", file=sys.stderr
        )
        sys.exit(1)

    global_actions = [str("quit")]
    choices = [(item, item) for item in sequence]

    if len(choices) == 1:
        return choices[0][0]

    chooser = choice.Menu(choices=choices, global_actions=global_actions)
    chooser.title = 'Please pick a {choosing} from the choices below (or "quit" to quit):'.format(
        choosing=str(choosing)
    )
    try:
        result = chooser.ask()
    except (KeyboardInterrupt, EOFError):
        print("")
        sys.exit(1)

    if isinstance(result, tuple) and result[1] == str("quit"):
        sys.exit(1)
    else:
        return result


def to_bytes(obj: Any) -> bytes:
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode("UTF-8")
    else:
        return str(obj).encode("UTF-8")


def get_possible_launched_by_user_variable_from_env() -> str:
    return os.getenv("SUDO_USER") or getpass.getuser()
