import logging
import time
from typing import Any
from typing import Union

from paasta_tools.metrics.metrics_lib import BaseMetrics
from paasta_tools.metrics.metrics_lib import GaugeProtocol
from paasta_tools.metrics.metrics_lib import register_metrics_interface
from paasta_tools.metrics.metrics_lib import TimerProtocol

log = logging.getLogger(__name__)


class Timer(TimerProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

    def start(self) -> None:
        log.debug("timer {} start at {}".format(self.name, time.time()))

    def stop(self) -> None:
        log.debug("timer {} stop at {}".format(self.name, time.time()))


class Gauge(GaugeProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

    def set(self, value: Union[int, float]) -> None:
        log.debug("gauge {} set to {}".format(self.name, value))


@register_metrics_interface(None)
class NoMetrics(BaseMetrics):
    def __init__(self, base_name: str) -> None:
        self.base_name = base_name

    def create_timer(self, name: str, **kwargs: Any) -> Timer:
        return Timer(self.base_name + '.' + name)

    def create_gauge(self, name: str, **kwargs: Any) -> Gauge:
        return Gauge(self.base_name + '.' + name)
