import logging
import time
from abc import ABC
from abc import abstractmethod
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Type
from typing import Union

from typing_extensions import Protocol

from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)

try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None

_metrics_interfaces: Dict[str, Type["BaseMetrics"]] = {}


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
        return yelp_meteorite.create_timer(self.base_name + "." + name, **kwargs)

    def create_gauge(self, name: str, **kwargs: Any) -> GaugeProtocol:
        return yelp_meteorite.create_gauge(self.base_name + "." + name, **kwargs)

    def create_counter(self, name: str, **kwargs: Any) -> CounterProtocol:
        return yelp_meteorite.create_counter(self.base_name + "." + name, **kwargs)

    def emit_event(self, name: str, **kwargs: Any) -> bool:
        return yelp_meteorite.emit_event(self.base_name + "." + name, **kwargs)


class Timer(TimerProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

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


class Counter(GaugeProtocol):
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
