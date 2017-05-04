from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from functools import reduce

import pyinotify

from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import deformat_job_id
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR


class PaastaWatcher(PaastaThread):

    def __init__(self, inbox_q, cluster):
        super(PaastaWatcher, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.cluster = cluster
        self.is_ready = False


class SoaFileWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, **kwargs):
        super(SoaFileWatcher, self).__init__(inbox_q, cluster)
        self.wm = pyinotify.WatchManager()
        self.wm.add_watch(DEFAULT_SOA_DIR, self.mask, rec=True)
        self.notifier = pyinotify.Notifier(watch_manager=self.wm,
                                           default_proc_fun=YelpSoaEventHandler(filewatcher=self))

    @property
    def mask(self):
        boring_flags = ['IN_CLOSE_NOWRITE', 'IN_OPEN', 'IN_ACCESS', 'IN_ATTRIB']
        return reduce(lambda x, y: x | y,
                      [v for k, v in pyinotify.EventsCodes.OP_FLAGS.items() if k not in boring_flags])

    def run(self):
        self.notifier.loop(callback=self.startup_checker)

    def startup_checker(self, obj):
        self.is_ready = True


class PublicConfigFileWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, **kwargs):
        super(PublicConfigFileWatcher, self).__init__(inbox_q, cluster)
        self.wm = pyinotify.WatchManager()
        self.wm.add_watch(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, self.mask, rec=True)
        self.notifier = pyinotify.Notifier(watch_manager=self.wm,
                                           default_proc_fun=PublicConfigEventHandler(filewatcher=self))

    @property
    def mask(self):
        boring_flags = ['IN_CLOSE_NOWRITE', 'IN_OPEN', 'IN_ACCESS', 'IN_ATTRIB']
        return reduce(lambda x, y: x | y,
                      [v for k, v in pyinotify.EventsCodes.OP_FLAGS.items() if k not in boring_flags])

    def run(self):
        self.notifier.loop(callback=self.startup_checker)

    def startup_checker(self, obj):
        self.is_ready = True


def get_marathon_apps():
    marathon_config = load_marathon_config()
    marathon_client = get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                          marathon_config.get_password())
    return get_all_marathon_apps(marathon_client, embed_failures=True)


class MaintenanceWatcher(PaastaWatcher):
    def __init__(self, inbox_q, cluster, **kwargs):
        super(MaintenanceWatcher, self).__init__(inbox_q, cluster)
        self.draining = []

    def run(self):
        self.is_ready = True
        while True:
            draining_hosts = get_draining_hosts()
            new_draining_hosts = [host for host in draining_hosts if host not in self.draining]
            self.draining = draining_hosts
            service_instances = []
            if new_draining_hosts:
                self.log.debug("Found new draining hosts: {}".format(new_draining_hosts))
                service_instances = self.get_at_risk_service_instances(new_draining_hosts)
            for service_instance in service_instances:
                self.inbox_q.put(service_instance)
            time.sleep(20)

    def get_at_risk_service_instances(self, draining_hosts):
        marathon_apps = get_marathon_apps()
        at_risk_tasks = [task for app in marathon_apps for task in app.tasks if task.host in draining_hosts]
        self.log.debug("At risk tasks: {}".format(at_risk_tasks))
        service_instances = []
        for task in at_risk_tasks:
            app_id = task.app_id.strip('/')
            service, instance, _, __ = deformat_job_id(app_id)
            service_instances.append(ServiceInstance(service=service,
                                                     instance=instance,
                                                     bounce_by=int(time.time()),
                                                     watcher=self.__class__.__name__,
                                                     bounce_timers=None))
        return service_instances


class PublicConfigEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher
        self.public_config = load_system_paasta_config()

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def filter_event(self, event):
        if event.name.endswith('.json'):
            return event

    def watch_new_folder(self, event):
        if event.maskname == 'IN_CREATE|IN_ISDIR':
            self.filewatcher.wm.add_watch(event.pathname, self.filewatcher.mask, rec=True)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        event = self.filter_event(event)
        if event:
            self.log.debug("Public config changed on disk, loading new config")
            try:
                new_config = load_system_paasta_config()
            except ValueError:
                self.log.error("Couldn't load public config, the JSON is invalid!")
                return
            service_instances = []
            if new_config != self.public_config:
                self.log.info("Public config has changed, now checking if it affects any services config shas")
                self.public_config = new_config
                service_instances = self.get_service_instances_with_changed_id()
            if service_instances:
                self.log.info("Found config change affecting {} service instances, "
                              "now doing a staggered bounce".format(len(service_instances)))
                service_instances = rate_limit_instances(service_instances,
                                                         self.public_config.get_deployd_big_bounce_rate(),
                                                         self.__class__.__name__)
            for service_instance in service_instances:
                self.filewatcher.inbox_q.put(service_instance)

    def get_service_instances_with_changed_id(self):
        marathon_app_ids = [app.id.lstrip('/') for app in get_marathon_apps()]
        instances = get_services_for_cluster(cluster=self.public_config.get_cluster(),
                                             instance_type='marathon',
                                             soa_dir=DEFAULT_SOA_DIR)
        service_instances = []
        for service, instance in instances:
            config = load_marathon_service_config(service,
                                                  instance,
                                                  self.public_config.get_cluster(),
                                                  soa_dir=DEFAULT_SOA_DIR)
            if config.format_marathon_app_dict()['id'] not in marathon_app_ids:
                service_instances.append((service, instance))
        return service_instances


class YelpSoaEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def filter_event(self, event):
        starts_with = ['marathon-', 'deployments.json']
        if any([event.name.startswith(x) for x in starts_with]):
            return event

    def watch_new_folder(self, event):
        if event.maskname == 'IN_CREATE|IN_ISDIR':
            self.filewatcher.wm.add_watch(event.pathname, self.filewatcher.mask, rec=True)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        event = self.filter_event(event)
        if event:
            self.log.debug("Change of {} in {}".format(event.name, event.path))
            service_name = event.path.split('/')[-1]
            self.log.debug("Adding all instances for service {} to the bounce inbox "
                           "with immediate priority".format(service_name))
            instances = list_all_instances_for_service(service=service_name,
                                                       clusters=[self.filewatcher.cluster],
                                                       instance_type='marathon')
            self.log.debug(instances)
            service_instances = [ServiceInstance(service=service_name,
                                                 instance=instance,
                                                 bounce_by=int(time.time()),
                                                 watcher=self.__class__.__name__,
                                                 bounce_timers=None)
                                 for instance in instances]
            for service_instance in service_instances:
                self.filewatcher.inbox_q.put(service_instance)
