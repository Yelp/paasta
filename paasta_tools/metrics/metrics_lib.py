import functools
import logging
import os
import socket
import sys
import time
from abc import ABC
from abc import abstractmethod
from contextvars import ContextVar
from inspect import currentframe
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast

from typing_extensions import Protocol

from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)

try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None

_metrics_interfaces: Dict[str, Type["BaseMetrics"]] = {}

# extra dimensions for the active @timed_entry_point timer; None outside of one
_timer_extra_dimensions: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
    "_timer_extra_dimensions", default=None
)

_EntryPoint = TypeVar("_EntryPoint", bound=Callable[..., Any])

# result recorded for uncaught exceptions; real exit codes are always 0-255
CRASHED_RESULT = -1


class TimerProtocol(Protocol):
    def __enter__(self) -> "TimerProtocol":
        raise NotImplementedError()

    def __exit__(
        self,
        err_type: Optional[type],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError()

    def __call__(
        self,
    ) -> Optional[float]:
        raise NotImplementedError()

    def start(self) -> None:
        raise NotImplementedError()

    def stop(self, **kwargs: Any) -> None:
        raise NotImplementedError()

    def record(self, value: float, **kwargs: Any) -> None:
        raise NotImplementedError()


class GaugeProtocol(Protocol):
    def set(self, value: Union[int, float]) -> None:
        raise NotImplementedError()


class CounterProtocol(Protocol):
    def count(self) -> None:
        raise NotImplementedError()


class BaseMetrics(ABC):
    def __init__(self, base_name: str) -> None:
        self.base_name = base_name

    @abstractmethod
    def create_timer(self, name: str, **kwargs: Any) -> TimerProtocol:
        raise NotImplementedError()

    @abstractmethod
    def create_gauge(self, name: str, **kwargs: Any) -> GaugeProtocol:
        raise NotImplementedError()

    @abstractmethod
    def create_counter(self, name: str, **kwargs: Any) -> CounterProtocol:
        raise NotImplementedError()

    @abstractmethod
    def emit_event(self, name: str, **kwargs: Any) -> bool:
        raise NotImplementedError()


def get_metrics_interface(base_name: str) -> BaseMetrics:
    metrics_provider = load_system_paasta_config().get_metrics_provider()
    return _metrics_interfaces[metrics_provider](base_name)


def register_metrics_interface(
    name: Optional[str],
) -> Callable[[Type[BaseMetrics]], Type[BaseMetrics]]:
    def outer(func: Type[BaseMetrics]) -> Type[BaseMetrics]:
        _metrics_interfaces[name] = func
        return func

    return outer


@register_metrics_interface("meteorite")
class MeteoriteMetrics(BaseMetrics):
    def __init__(self, base_name: str) -> None:
        self.base_name = base_name
        if yelp_meteorite is None:
            raise ImportError(
                "yelp_meteorite not imported, please try another metrics provider"
            )

    def create_timer(self, name: str, **kwargs: Any) -> TimerProtocol:
        # yelp_meteorite returns an EmptyMetric object if the timer is misconfigured
        # ...but that doesn't have the same interface ;_;
        return cast(
            Timer, yelp_meteorite.create_timer(self.base_name + "." + name, **kwargs)
        )

    def create_gauge(self, name: str, **kwargs: Any) -> GaugeProtocol:
        # yelp_meteorite returns an EmptyMetric object if the gauge is misconfigured
        # ...but that doesn't have the same interface ;_;
        return cast(
            Gauge, yelp_meteorite.create_gauge(self.base_name + "." + name, **kwargs)
        )

    def create_counter(self, name: str, **kwargs: Any) -> CounterProtocol:
        return yelp_meteorite.create_counter(self.base_name + "." + name, **kwargs)

    def emit_event(self, name: str, **kwargs: Any) -> bool:
        return yelp_meteorite.emit_event(self.base_name + "." + name, **kwargs)


class Timer(TimerProtocol):
    def __init__(self, name: str) -> None:
        self.name = name
        self.elapsed_ms: Optional[float] = None

    def __enter__(self) -> TimerProtocol:
        self.start()
        return self

    def __exit__(
        self,
        err_type: Optional[type],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if not err_type:
            self.stop()

    def __call__(self) -> Optional[float]:
        if self.elapsed_ms is None:
            self.stop()
        return self.elapsed_ms

    def start(self) -> None:
        log.debug("timer {} start at {}".format(self.name, time.time()))

    def stop(self, **kwargs: Any) -> None:
        log.debug("timer {} stop at {}".format(self.name, time.time()))

    def record(self, value: float, **kwargs: Any) -> None:
        log.debug(f"timer {self.name} record value {value}")


class Gauge(GaugeProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

    def set(self, value: Union[int, float]) -> None:
        log.debug(f"gauge {self.name} set to {value}")


class Counter(CounterProtocol):
    def __init__(self, name: str) -> None:
        self.name = name
        self.counter = 0

    def count(self) -> None:
        self.counter += 1
        log.debug(f"counter {self.name} incremented to {self.counter}")


@register_metrics_interface(None)
class NoMetrics(BaseMetrics):
    def __init__(self, base_name: str) -> None:
        self.base_name = base_name

    def create_timer(self, name: str, **kwargs: Any) -> Timer:
        return Timer(self.base_name + "." + name)

    def create_gauge(self, name: str, **kwargs: Any) -> Gauge:
        return Gauge(self.base_name + "." + name)

    def create_counter(self, name: str, **kwargs: Any) -> Counter:
        return Counter(self.base_name + "." + name)

    def emit_event(self, name: str, **kwargs: Any) -> bool:
        log.debug(f"event {name} occurred with properties: {kwargs}")
        return True


def _parse_metric_labels_env() -> Dict[str, str]:
    """Parse PAASTA_METRICS_LABELS envvar (comma-separated KEY=VALUE pairs) into a dict."""
    raw = os.environ.get("PAASTA_METRICS_LABELS", "")
    if not raw:
        return {}
    result: Dict[str, str] = {}
    for token in raw.split(","):
        token = token.strip()
        if "=" not in token:
            continue
        key, _, value = token.partition("=")
        result[key.strip()] = value.strip()
    return result


def _get_host_cluster() -> Optional[str]:
    try:
        return load_system_paasta_config().get_cluster()
    except PaastaNotConfiguredError:
        return None


def system_timer(
    name: str = "system_process_duration", dimensions: Dict[str, Any] = {}
) -> TimerProtocol:
    metrics_interface = get_metrics_interface("paasta")
    parent_pid = os.getppid()
    pid = os.getpid()
    hostname = socket.gethostname()
    # set our parent caller as the path dimension, e.g. /path/to/setup_kubernetes_job
    current = currentframe()
    parent = current.f_back
    path = parent.f_globals.get("__file__", "unknown")
    # Note any of these dimensions may be overridden by caller
    default_dimensions = {
        "path": path,
        "host": hostname,
        # ppid is included given some system processes are called multiple times in parallel for a single 'run'
        "parent_pid": parent_pid,
        "pid": pid,
    }
    cluster = _get_host_cluster()
    if cluster is not None:
        default_dimensions["cluster"] = cluster
    default_dimensions.update(_parse_metric_labels_env())
    default_dimensions.update(dimensions)
    return metrics_interface.create_timer(
        name=name, default_dimensions=default_dimensions
    )


def add_timer_dimensions(**dims: Any) -> None:
    """Add dimensions to the timer of the enclosing @timed_entry_point."""
    current = _timer_extra_dimensions.get()
    if current is None:
        log.debug(f"No active timed_entry_point, ignoring timer dimensions {dims}")
        return
    current.update(dims)


def _exit_code_from_system_exit(e: SystemExit) -> int:
    # mirrors how the interpreter and OS map SystemExit.code to a process exit code
    if e.code is None:
        return 0
    if isinstance(e.code, int):
        return e.code & 0xFF
    return 1


def timed_entry_point(fn: _EntryPoint) -> _EntryPoint:
    """Emit a system_timer metric around main(); result is the exit code, or -1 on crash."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # system_timer derives path from its caller's frame, which would be this module
        module = sys.modules.get(fn.__module__)
        path = getattr(module, "__file__", None) or "unknown"
        token = _timer_extra_dimensions.set({})
        timer: Optional[TimerProtocol] = None
        try:
            timer = system_timer(dimensions={"path": path})
            timer.start()
        except Exception:
            # metrics should never prevent a script from running
            log.exception(f"Unable to start timer for {path}")
            timer = None

        exit_code = 0
        try:
            return fn(*args, **kwargs)
        except SystemExit as e:
            exit_code = _exit_code_from_system_exit(e)
            raise
        except BaseException:
            exit_code = CRASHED_RESULT
            raise
        finally:
            extra_dimensions = _timer_extra_dimensions.get() or {}
            _timer_extra_dimensions.reset(token)
            if timer is not None:
                try:
                    timer.stop(tmp_dimensions={**extra_dimensions, "result": exit_code})
                    log.info(
                        f"Stopping timer for {path} {extra_dimensions} with result {exit_code}: {timer()}ms elapsed"
                    )
                except Exception:
                    log.exception(f"Unable to stop timer for {path}")

    return cast(_EntryPoint, wrapper)
