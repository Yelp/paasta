from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import time
from functools import reduce

import pyinotify
from kazoo.protocol.states import EventType
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch
from requests.exceptions import RequestException

from paasta_tools.deployd.common import get_marathon_client_from_config
from paasta_tools.deployd.common import get_service_instances_needing_update
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.long_running_service_tools import AUTOSCALING_ZK_ROOT
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import deformat_job_id
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR


class PaastaWatcher(PaastaThread):

    def __init__(self, inbox_q, cluster, config):
        super(PaastaWatcher, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.cluster = cluster
        self.config = config
        self.is_ready = False


class AutoscalerWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, config, **kwargs):
        super(AutoscalerWatcher, self).__init__(inbox_q, cluster, config)
        self.zk = kwargs.pop('zookeeper_client')
        self.watchers = {}

    def watch_folder(self, path):
        """recursive nonsense"""
        if "autoscaling.lock" in path:
            return
        self.log.debug("Adding watch on {}".format(path))
        watcher = ChildrenWatch(self.zk, path, func=self.process_folder_event, send_event=True)
        self.watchers[path] = watcher
        children = watcher._prior_children
        if children and ('instances' in children):
            self.watch_node("{}/instances".format(path))
        elif children:
            for child in children:
                self.watch_folder("{}/{}".format(path, child))

    def watch_node(self, path):
        self.log.debug("Adding watch on {}".format(path))
        DataWatch(self.zk, path, func=self.process_node_event, send_event=True)

    def process_node_event(self, data, stat, event):
        self.log.debug("Node change: {}".format(event))
        if event and (event.type == EventType.CREATED or event.type == EventType.CHANGED):
            service, instance = event.path.split('/')[-3:-1]
            self.log.info("Number of instances changed or autoscaling enabled for first time"
                          " for {}.{}".format(service, instance))
            service_instance = ServiceInstance(
                service=service,
                instance=instance,
                bounce_by=int(time.time()),
                bounce_timers=None,
                watcher=self.__class__.__name__,
                failures=0,
            )
            self.inbox_q.put(service_instance)

    def process_folder_event(self, children, event):
        self.log.debug("Folder change: {}".format(event))
        if event and (event.type == EventType.CHILD):
            fq_children = ["{}/{}".format(event.path, child) for child in children]
            for child in fq_children:
                if child not in self.watchers:
                    self.watch_folder(child)

    def run(self):
        self.watchers[AUTOSCALING_ZK_ROOT] = self.watch_folder(AUTOSCALING_ZK_ROOT)
        self.is_ready = True
        while True:
            time.sleep(0.1)


class SoaFileWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, config, **kwargs):
        super(SoaFileWatcher, self).__init__(inbox_q, cluster, config)
        self.wm = pyinotify.WatchManager()
        self.wm.add_watch(DEFAULT_SOA_DIR, self.mask, rec=True)
        self.notifier = pyinotify.Notifier(
            watch_manager=self.wm,
            default_proc_fun=YelpSoaEventHandler(filewatcher=self),
        )

    @property
    def mask(self):
        boring_flags = ['IN_CLOSE_NOWRITE', 'IN_OPEN', 'IN_ACCESS', 'IN_ATTRIB']
        return reduce(
            lambda x, y: x | y,
            [v for k, v in pyinotify.EventsCodes.OP_FLAGS.items() if k not in boring_flags],
        )

    def run(self):
        self.notifier.loop(callback=self.startup_checker)

    def startup_checker(self, obj):
        self.is_ready = True


class PublicConfigFileWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, config, **kwargs):
        super(PublicConfigFileWatcher, self).__init__(inbox_q, cluster, config)
        self.wm = pyinotify.WatchManager()
        self.wm.add_watch(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, self.mask, rec=True)
        self.notifier = pyinotify.Notifier(
            watch_manager=self.wm,
            default_proc_fun=PublicConfigEventHandler(filewatcher=self),
        )

    @property
    def mask(self):
        boring_flags = ['IN_CLOSE_NOWRITE', 'IN_OPEN', 'IN_ACCESS', 'IN_ATTRIB']
        return reduce(
            lambda x, y: x | y,
            [v for k, v in pyinotify.EventsCodes.OP_FLAGS.items() if k not in boring_flags],
        )

    def run(self):
        self.notifier.loop(callback=self.startup_checker)

    def startup_checker(self, obj):
        self.is_ready = True


class MaintenanceWatcher(PaastaWatcher):
    def __init__(self, inbox_q, cluster, config, **kwargs):
        super(MaintenanceWatcher, self).__init__(inbox_q, cluster, config)
        self.draining = set()
        self.marathon_client = get_marathon_client_from_config()

    def get_new_draining_hosts(self):
        try:
            draining_hosts = get_draining_hosts()
        except RequestException as e:
            self.log.error("Unable to get list of draining hosts from mesos: {}".format(e))
            draining_hosts = list(self.draining)
        new_draining_hosts = [host for host in draining_hosts if host not in self.draining]
        for host in new_draining_hosts:
            self.draining.add(host)
        hosts_finished_draining = [host for host in self.draining if host not in draining_hosts]
        for host in hosts_finished_draining:
            self.draining.remove(host)
        return new_draining_hosts

    def run(self):
        self.is_ready = True
        while True:
            new_draining_hosts = self.get_new_draining_hosts()
            service_instances = []
            if new_draining_hosts:
                self.log.info("Found new draining hosts: {}".format(new_draining_hosts))
                service_instances = self.get_at_risk_service_instances(new_draining_hosts)
            for service_instance in service_instances:
                self.inbox_q.put(service_instance)
            time.sleep(self.config.get_deployd_maintenance_polling_frequency())

    def get_at_risk_service_instances(self, draining_hosts):
        marathon_apps = get_all_marathon_apps(self.marathon_client, embed_tasks=True)
        at_risk_tasks = [task for app in marathon_apps for task in app.tasks if task.host in draining_hosts]
        self.log.info("At risk tasks: {}".format(at_risk_tasks))
        service_instances = []
        for task in at_risk_tasks:
            app_id = task.app_id.strip('/')
            service, instance, _, __ = deformat_job_id(app_id)
            # check we haven't already added this instance,
            # no need to add the same instance to the bounce queue
            # more than once
            if not any([(service, instance) == (si.service, si.instance) for si in service_instances]):
                service_instances.append(ServiceInstance(
                    service=service,
                    instance=instance,
                    bounce_by=int(time.time()),
                    watcher=self.__class__.__name__,
                    bounce_timers=None,
                    failures=0,
                ))
        return service_instances


class PublicConfigEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher
        self.public_config = load_system_paasta_config()
        self.marathon_client = get_marathon_client_from_config()

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def filter_event(self, event):
        if event.name.endswith('.json') or event.maskname == 'IN_CREATE|IN_ISDIR':
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
                all_service_instances = get_services_for_cluster(
                    cluster=self.public_config.get_cluster(),
                    instance_type='marathon',
                    soa_dir=DEFAULT_SOA_DIR,
                )
                service_instances = get_service_instances_needing_update(
                    self.marathon_client,
                    all_service_instances,
                    self.public_config.get_cluster(),
                )
            if service_instances:
                self.log.info("Found config change affecting {} service instances, "
                              "now doing a staggered bounce".format(len(service_instances)))
                bounce_rate = self.public_config.get_deployd_big_bounce_rate()
                service_instances = rate_limit_instances(
                    instances=service_instances,
                    number_per_minute=bounce_rate,
                    watcher_name=self.__class__.__name__,
                )
            for service_instance in service_instances:
                self.filewatcher.inbox_q.put(service_instance)


class YelpSoaEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher
        self.marathon_client = get_marathon_client_from_config()

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
            try:
                file_names = os.listdir(event.pathname)
            except OSError:
                return
            if any(['marathon-' in file_name for file_name in file_names]):
                self.log.info("New folder with marathon files: {}".format(event.name))
                self.bounce_service(event.name)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        event = self.filter_event(event)
        if event:
            self.log.info("Change of {} in {}".format(event.name, event.path))
            service_name = event.path.split('/')[-1]
            self.bounce_service(service_name)

    def bounce_service(self, service_name):
        self.log.info("Checking if any instances for {} need bouncing".format(service_name))
        instances = list_all_instances_for_service(
            service=service_name,
            clusters=[self.filewatcher.cluster],
            instance_type='marathon',
            cache=False,
        )
        self.log.debug(instances)
        service_instances = [(service_name, instance) for instance in instances]
        service_instances = get_service_instances_needing_update(
            self.marathon_client,
            service_instances,
            self.filewatcher.cluster,
        )
        for service, instance in service_instances:
            self.log.info("{}.{} has a new marathon app ID, and so needs bouncing".format(service, instance))
        service_instances = [ServiceInstance(
            service=service,
            instance=instance,
            bounce_by=int(time.time()),
            watcher=self.__class__.__name__,
            bounce_timers=None,
            failures=0,
        )
            for service, instance in service_instances]
        for service_instance in service_instances:
            self.filewatcher.inbox_q.put(service_instance)
