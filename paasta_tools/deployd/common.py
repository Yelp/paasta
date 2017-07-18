from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from collections import namedtuple
from threading import Thread

from six.moves.queue import Queue

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import NoDockerImageError

BounceTimers = namedtuple('BounceTimers', ['processed_by_worker', 'setup_marathon', 'bounce_length'])
ServiceInstance = namedtuple(
    'ServiceInstance', [
        'service',
        'instance',
        'bounce_by',
        'watcher',
        'bounce_timers',
        'failures',
    ],
)


class PaastaThread(Thread):

    @property
    def log(self):
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        Queue.__init__(self, *args, **kwargs)

    @property
    def log(self):
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)

    def put(self, item, *args, **kwargs):
        self.log.debug("Adding {} to {} queue".format(item, self.name))
        Queue.put(self, item, *args, **kwargs)


def rate_limit_instances(instances, number_per_minute, watcher_name):
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
        config = load_marathon_service_config_no_cache(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=DEFAULT_SOA_DIR,
        )
        try:
            config_app = config.format_marathon_app_dict()
            app_id = '/{}'.format(config_app['id'])
        except (NoDockerImageError, InvalidJobNameError):
            config_app = None
        if not config_app:
            service_instances.append((service, instance))
        elif app_id not in marathon_app_ids:
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
