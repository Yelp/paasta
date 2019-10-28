# Copyright 2019 Yelp Inc.
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
import time
from abc import ABCMeta
from abc import abstractmethod
from typing import Any
from typing import Type
from typing import Union

import colorlog
import staticconf
from typing_extensions import Protocol

try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None

logger = colorlog.getLogger(__name__)


class CounterProtocol(Protocol):
    def count(self, *args: Any, **kwargs: Any) -> None: ...


class GaugeProtocol(Protocol):
    def set(self, value: Union[int, float], *args: Any, **kwargs: Any) -> None: ...


class TimerProtocol(Protocol):
    def start(self, *args: Any, **kwargs: Any) -> None: ...
    def stop(self, *args: Any, **kwargs: Any) -> None: ...


class MonitoringClient(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def create_counter(name: str, *args: Any, **kwargs: Any) -> CounterProtocol:  # pragma: no cover
        pass

    @staticmethod
    @abstractmethod
    def create_gauge(name: str, *args: Any, **kwargs: Any) -> GaugeProtocol:  # pragma: no cover
        pass

    @staticmethod
    @abstractmethod
    def create_timer(name: str, *args: Any, **kwargs: Any) -> TimerProtocol:  # pragma: no cover
        pass


def get_monitoring_client() -> Type[MonitoringClient]:
    default_monitoring_client = 'SignalFXMonitoringClient' if yelp_meteorite else 'LogMonitoringClient'
    client_class = staticconf.read('monitoring_client', default=default_monitoring_client)
    return _clients[client_class]


class SignalFXMonitoringClient(MonitoringClient):
    @staticmethod
    def create_counter(name: str, *args: Any, **kwargs: Any) -> CounterProtocol:
        return yelp_meteorite.create_counter(name, *args, **kwargs)

    @staticmethod
    def create_gauge(name: str, *args: Any, **kwargs: Any) -> GaugeProtocol:
        return yelp_meteorite.create_gauge(name, *args, **kwargs)

    @staticmethod
    def create_timer(name: str, *args: Any, **kwargs: Any) -> TimerProtocol:
        return yelp_meteorite.create_timer(name, *args, **kwargs)


class LogCounter(GaugeProtocol):
    def __init__(self, name: str) -> None:
        self.name = name
        self.counter = 0

    def count(self, *args: Any, **kwargs: Any) -> None:
        self.counter += 1
        logger.debug(f'counter {self.name} incremented to {self.counter}')


class LogGauge(GaugeProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

    def set(self, value: Union[int, float], *args: Any, **kwargs: Any) -> None:
        logger.debug(f'gauge {self.name} set to {value}')


class LogTimer(TimerProtocol):
    def __init__(self, name: str) -> None:
        self.name = name

    def start(self, *args: Any, **kwargs: Any) -> None:
        logger.debug('timer {} start at {}'.format(self.name, time.time()))

    def stop(self, *args: Any, **kwargs: Any) -> None:
        logger.debug('timer {} stop at {}'.format(self.name, time.time()))


class LogMonitoringClient(MonitoringClient):
    @staticmethod
    def create_counter(name: str, *args: Any, **kwargs: Any) -> LogCounter:
        return LogCounter(name)

    @staticmethod
    def create_gauge(name: str, *args: Any, **kwargs: Any) -> LogGauge:
        return LogGauge(name)

    @staticmethod
    def create_timer(name: str, *args: Any, **kwargs: Any) -> LogTimer:
        return LogTimer(name)


_clients = {
    'SignalFXMonitoringClient': SignalFXMonitoringClient,
    'LogMonitoringClient': LogMonitoringClient,
}
