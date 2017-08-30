import logging
import time
from collections import namedtuple
from queue import PriorityQueue
from queue import Queue
from threading import Thread

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.utils import InvalidJobNameError
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

    def __new__(cls, service, instance, watcher, cluster, bounce_by, failures=0, bounce_timers=None, priority=None):
        if priority is None:
            priority = cls.get_priority(service, instance, cluster)
        return super().__new__(
            _cls=cls,
            service=service,
            instance=instance,
            watcher=watcher,
            bounce_by=bounce_by,
            failures=failures,
            bounce_timers=bounce_timers,
            priority=priority,
        )

    def get_priority(service, instance, cluster):
        try:
            config = load_marathon_service_config(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=DEFAULT_SOA_DIR,
            )
        except (NoDockerImageError, InvalidJobNameError, NoDeploymentsAvailable) as e:
            return 0
        return config.get_bounce_priority()


class PaastaThread(Thread):

    @property
    def log(self):
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        super(PaastaQueue, self).__init__(*args, **kwargs)

    @property
    def log(self):
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, item, *args, **kwargs):
        self.log.debug("Adding {} to {} queue".format(item, self.name))
        super(PaastaQueue, self).put(item, *args, **kwargs)


class PaastaPriorityQueue(PriorityQueue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        super(PaastaPriorityQueue, self).__init__(*args, **kwargs)
        self.counter = 0

    @property
    def log(self):
        name = '.'.join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, priority, item, *args, **kwargs):
        self.log.debug("Adding {} to {} queue with priority {}".format(item, self.name, priority))
        # this counter is to preserve the FIFO nature of the queue, it increments on every put
        # and the python PriorityQueue sorts based on the first item in the tuple (priority)
        # and then the second item in the tuple (counter). This way all items with the same
        # priority come out in the order they were entered.
        self.counter += 1
        super(PaastaPriorityQueue, self).put((priority, self.counter, item), *args, **kwargs)

    def get(self, *args, **kwargs):
        return super(PaastaPriorityQueue, self).get(*args, **kwargs)[2]


def rate_limit_instances(instances, cluster, number_per_minute, watcher_name):
    service_instances = []
    if not instances:
        return []
    time_now = int(time.time())
    time_step = int(60 / number_per_minute)
    bounce_time = time_now
    for service, instance in instances:
        service_instances.append(ServiceInstance(
            service=service,
            instance=instance,
            watcher=watcher_name,
            cluster=cluster,
            bounce_by=bounce_time,
            bounce_timers=None,
            failures=0,
        ))
        bounce_time += time_step
    return service_instances


def exponential_back_off(failures, factor, base, max_time):
    seconds = factor * base ** failures
    return seconds if seconds < max_time else max_time


def get_service_instances_needing_update(marathon_client, instances, cluster):
    marathon_apps = {app.id: app for app in get_all_marathon_apps(marathon_client)}
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
        except (NoDockerImageError, InvalidJobNameError, NoDeploymentsAvailable) as e:
            print("DEBUG: Skipping %s.%s because: '%s'" % (service, instance, str(e)))
            continue
        if app_id not in marathon_app_ids:
            service_instances.append((service, instance))
        elif marathon_apps[app_id].instances != config_app['instances']:
            service_instances.append((service, instance))
    return service_instances


def get_marathon_client_from_config():
    marathon_config = load_marathon_config()
    marathon_client = get_marathon_client(
        marathon_config.get_url(), marathon_config.get_username(),
        marathon_config.get_password(),
    )
    return marathon_client
