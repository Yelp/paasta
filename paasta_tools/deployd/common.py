import logging
import time
from collections import namedtuple
from queue import PriorityQueue
from queue import Queue
from threading import Thread
from typing import Any
from typing import Collection
from typing import List
from typing import Optional
from typing import Tuple

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError

BounceTimers = namedtuple('BounceTimers', ['processed_by_worker', 'setup_marathon', 'bounce_length'])
BaseServiceInstance = namedtuple(
    'ServiceInstance', [
        'service',
        'instance',
        'bounce_by',
        'watcher',
        'bounce_timers',
        'failures',
        'priority',
    ],
)


class ServiceInstance(BaseServiceInstance):
    __slots__ = ()

    def __new__(
        _cls,
        service: str,
        instance: str,
        watcher: str,
        cluster: str,
        bounce_by: float,
        failures: int = 0,
        bounce_timers: Optional[BounceTimers] = None,
        priority: Optional[int] = None,
    ) -> 'ServiceInstance':
        if priority is None:
            priority = get_priority(service, instance, cluster)
        return super().__new__(  # type: ignore
            _cls=_cls,
            service=service,
            instance=instance,
            watcher=watcher,
            bounce_by=bounce_by,
            failures=failures,
            bounce_timers=bounce_timers,
            priority=priority,
        )


def get_priority(service: str, instance: str, cluster: str) -> int:
    try:
        config = load_marathon_service_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=DEFAULT_SOA_DIR,
        )
    except (NoDockerImageError, InvalidJobNameError, NoDeploymentsAvailable, NoConfigurationForServiceError):
        return 0
    return config.get_bounce_priority()


class PaastaThread(Thread):

    @property
    def log(self) -> logging.Logger:
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.name = name
        super().__init__(*args, **kwargs)

    @property
    def log(self) -> logging.Logger:
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, item: Any, *args: Any, **kwargs: Any) -> None:
        self.log.debug(f"Adding {item} to {self.name} queue")
        super().put(item, *args, **kwargs)


class PaastaPriorityQueue(PriorityQueue):

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.name = name
        super().__init__(*args, **kwargs)
        self.counter = 0

    @property
    def log(self) -> logging.Logger:
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    # ignored because https://github.com/python/mypy/issues/1237
    def put(self, priority: float, item: Any, *args: Any, **kwargs: Any) -> None:  # type: ignore
        self.log.debug(f"Adding {item} to {self.name} queue with priority {priority}")
        # this counter is to preserve the FIFO nature of the queue, it increments on every put
        # and the python PriorityQueue sorts based on the first item in the tuple (priority)
        # and then the second item in the tuple (counter). This way all items with the same
        # priority come out in the order they were entered.
        self.counter += 1
        super().put((priority, self.counter, item), *args, **kwargs)

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return super().get(*args, **kwargs)[2]


def rate_limit_instances(
    instances: Collection[Tuple[str, str]],
    cluster: str,
    number_per_minute: float,
    watcher_name: str,
    priority: Optional[float] = None,
) -> List[ServiceInstance]:
    service_instances = []
    if not instances:
        return []
    time_now = int(time.time())
    time_step = int(60 / number_per_minute)
    bounce_time = time_now
    for service, instance in instances:
        # https://github.com/python/mypy/issues/2852
        service_instances.append(ServiceInstance(  # type: ignore
            service=service,
            instance=instance,
            watcher=watcher_name,
            cluster=cluster,
            bounce_by=bounce_time,
            bounce_timers=None,
            failures=0,
            priority=priority,
        ))
        bounce_time += time_step
    return service_instances


def exponential_back_off(failures: int, factor: float, base: float, max_time: float) -> float:
    seconds = factor * base ** failures
    return seconds if seconds < max_time else max_time


def get_service_instances_needing_update(
    marathon_clients: MarathonClients,
    instances: Collection[Tuple[str, str]],
    cluster: str,
) -> List[Tuple[str, str]]:
    marathon_apps = {}
    for marathon_client in marathon_clients.get_all_clients():
        marathon_apps.update({app.id: app for app in get_all_marathon_apps(marathon_client)})

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
            app_id = '/{}'.format(config_app['id'])
        # Not ideal but we rely on a lot of user input to create the app dict
        # and we really can't afford to bail if just one app definition is malformed
        except Exception as e:
            print("ERROR: Skipping {}.{} because: '{}'".format(service, instance, str(e)))
            continue
        if app_id not in marathon_app_ids:
            service_instances.append((service, instance))
        elif marathon_apps[app_id].instances != config_app['instances']:
            service_instances.append((service, instance))
    return service_instances


def get_marathon_clients_from_config() -> MarathonClients:
    system_paasta_config = load_system_paasta_config()
    marathon_servers = get_marathon_servers(system_paasta_config)
    marathon_clients = get_marathon_clients(marathon_servers)
    return marathon_clients
