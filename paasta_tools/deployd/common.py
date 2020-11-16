import logging
import time
from contextlib import contextmanager
from queue import Empty
from queue import PriorityQueue
from queue import Queue
from threading import Condition
from threading import Event
from threading import Thread
from typing import Any
from typing import Collection
from typing import Generator
from typing import Iterable
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

from typing_extensions import Protocol

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.metrics.metrics_lib import TimerProtocol
from paasta_tools.utils import load_system_paasta_config


class BounceTimers(NamedTuple):
    processed_by_worker: TimerProtocol
    setup_marathon: TimerProtocol
    bounce_length: TimerProtocol


class ServiceInstance(NamedTuple):
    service: str
    instance: str
    watcher: str
    bounce_by: float
    wait_until: float
    enqueue_time: float
    bounce_start_time: float
    failures: int = 0
    processed_count: int = 0


# Hack to make the default values for ServiceInstance work on python 3.6.0. (typing.NamedTuple gained default values in
# python 3.6.1.)
ServiceInstance.__new__.__defaults__ = (0, 0)  # type: ignore


class PaastaThread(Thread):
    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):
    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.name = name
        super().__init__(*args, **kwargs)

    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, item: Any, *args: Any, **kwargs: Any) -> None:
        self.log.debug(f"Adding {item} to {self.name} queue")
        super().put(item, *args, **kwargs)


def exponential_back_off(
    failures: int, factor: float, base: float, max_time: float
) -> float:
    seconds = factor * base ** failures
    return seconds if seconds < max_time else max_time


def get_service_instances_needing_update(
    marathon_clients: MarathonClients,
    instances: Collection[Tuple[str, str]],
    cluster: str,
) -> List[Tuple[str, str, MarathonServiceConfig, str]]:
    marathon_apps = {}
    for marathon_client in marathon_clients.get_all_clients():
        marathon_apps.update(
            {app.id: app for app in get_all_marathon_apps(marathon_client)}
        )

    marathon_app_ids = marathon_apps.keys()
    service_instances = []
    for service, instance in instances:
        try:
            config = load_marathon_service_config_no_cache(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=DEFAULT_SOA_DIR,
            )
            config_app = config.format_marathon_app_dict()
            app_id = "/{}".format(config_app["id"])
        # Not ideal but we rely on a lot of user input to create the app dict
        # and we really can't afford to bail if just one app definition is malformed
        except Exception as e:
            print(
                "ERROR: Skipping {}.{} because: '{}'".format(service, instance, str(e))
            )
            continue
        if (
            app_id not in marathon_app_ids
            or marathon_apps[app_id].instances != config_app["instances"]
        ):
            service_instances.append((service, instance, config, app_id))
    return service_instances


def get_marathon_clients_from_config() -> MarathonClients:
    system_paasta_config = load_system_paasta_config()
    marathon_servers = get_marathon_servers(system_paasta_config)
    marathon_clients = get_marathon_clients(marathon_servers)
    return marathon_clients


class DelayDeadlineQueueProtocol(Protocol):
    def __init__(self) -> None:
        ...

    def put(self, si: ServiceInstance) -> None:
        ...

    @contextmanager
    def get(
        self, block: bool = True, timeout: float = None
    ) -> Generator[ServiceInstance, None, None]:
        ...

    def get_available_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, Optional[ServiceInstance]]]:
        ...

    def get_unavailable_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, float, Optional[ServiceInstance]]]:
        ...


class DelayDeadlineQueue(DelayDeadlineQueueProtocol):
    """Entries into this queue have both a wait_until and a bounce_by. Before wait_until, get() will not return an entry.
    get() returns the entry whose wait_until has passed and which has the lowest bounce_by."""

    def __init__(self) -> None:
        self.available_service_instances: PriorityQueue[
            Tuple[float, ServiceInstance]
        ] = PriorityQueue()
        self.unavailable_service_instances: PriorityQueue[
            Tuple[float, float, ServiceInstance]
        ] = PriorityQueue()

        self.unavailable_service_instances_modify = Condition()
        self.background_thread_started = Event()
        Thread(target=self.move_from_unavailable_to_available, daemon=True).start()
        self.background_thread_started.wait()

    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, si: ServiceInstance) -> None:
        self.log.debug(
            f"adding {si.service}.{si.instance} to queue with wait_until {si.wait_until} and bounce_by {si.bounce_by}"
        )
        with self.unavailable_service_instances_modify:
            self.unavailable_service_instances.put((si.wait_until, si.bounce_by, si))
            self.unavailable_service_instances_modify.notify()

    def move_from_unavailable_to_available(self) -> None:
        self.background_thread_started.set()
        with self.unavailable_service_instances_modify:
            while True:
                try:
                    while True:
                        (
                            wait_until,
                            bounce_by,
                            si,
                        ) = self.unavailable_service_instances.get_nowait()
                        if wait_until < time.time():
                            self.available_service_instances.put_nowait((bounce_by, si))
                        else:
                            self.unavailable_service_instances.put_nowait(
                                (wait_until, bounce_by, si)
                            )
                            timeout = wait_until - time.time()
                            break
                except Empty:
                    timeout = None

                self.unavailable_service_instances_modify.wait(timeout=timeout)

    @contextmanager
    def get(
        self, block: bool = True, timeout: float = None
    ) -> Generator[ServiceInstance, None, None]:
        bounce_by, si = self.available_service_instances.get(
            block=block, timeout=timeout
        )
        try:
            yield si
        except Exception:
            self.available_service_instances.put((bounce_by, si))

    def get_available_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, Optional[ServiceInstance]]]:
        return [
            (bounce_by, (si if fetch_service_instances else None))
            for bounce_by, si in self.available_service_instances.queue
        ]

    def get_unavailable_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, float, Optional[ServiceInstance]]]:
        return [
            (wait_until, bounce_by, (si if fetch_service_instances else None))
            for wait_until, bounce_by, si in self.unavailable_service_instances.queue
        ]
