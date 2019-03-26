import logging
import os
import time
from functools import reduce
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

import pyinotify
from kazoo.protocol.states import EventType
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch
from requests.exceptions import RequestException

from paasta_tools.deployd.common import get_marathon_clients_from_config
from paasta_tools.deployd.common import get_service_instances_needing_update
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.long_running_service_tools import AUTOSCALING_ZK_ROOT
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import deformat_job_id
from paasta_tools.marathon_tools import get_marathon_apps_with_clients
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR


class PaastaWatcher(PaastaThread):

    def __init__(self, instances_that_need_to_be_bounced_in_the_future, cluster, config):
        super().__init__()
        self.daemon = True
        self.instances_that_need_to_be_bounced_in_the_future = instances_that_need_to_be_bounced_in_the_future
        self.cluster = cluster
        self.config = config
        self.is_ready = False


class AutoscalerWatcher(PaastaWatcher):

    def __init__(self, instances_that_need_to_be_bounced_in_the_future, cluster, config, **kwargs):
        super().__init__(instances_that_need_to_be_bounced_in_the_future, cluster, config)
        self.zk = kwargs.pop('zookeeper_client')
        self.watchers: Dict[str, PaastaWatcher] = {}

    def watch_folder(self, path, enqueue_children=False):
        """recursive nonsense"""
        if "autoscaling.lock" in path:
            return
        if path.split('/')[-1] == 'instances':
            self.watch_node(path, enqueue=enqueue_children)
            return
        self.log.info(f"Adding folder watch on {path}")
        watcher = ChildrenWatch(self.zk, path, func=self.process_folder_event, send_event=True)
        self.watchers[path] = watcher
        children = watcher._client.get_children(watcher._path)
        if children:
            for child in children:
                self.watch_folder(f"{path}/{child}", enqueue_children=enqueue_children)

    def _enqueue_service_instance(self, path):
        service, instance = path.split('/')[-3:-1]
        self.log.info(f"Number of instances changed for {service}.{instance} by the autoscaler.")
        # https://github.com/python/mypy/issues/2852
        service_instance = ServiceInstance(  # type: ignore
            service=service,
            instance=instance,
            cluster=self.config.get_cluster(),
            bounce_by=int(time.time()),
            bounce_timers=None,
            watcher=type(self).__name__,
            failures=0,
        )
        self.instances_that_need_to_be_bounced_in_the_future.put(service_instance)

    def watch_node(self, path, enqueue=False):
        self.log.info(f"Adding zk node watch on {path}")
        DataWatch(self.zk, path, func=self.process_node_event, send_event=True)
        if enqueue:
            self._enqueue_service_instance(path)

    def process_node_event(self, data, stat, event):
        self.log.debug(f"zk node change: {event}")
        if event and (event.type == EventType.CREATED or event.type == EventType.CHANGED):
            self._enqueue_service_instance(event.path)

    def process_folder_event(self, children, event):
        self.log.debug(f"Folder change: {event}")
        if event and (event.type == EventType.CHILD):
            fq_children = [f"{event.path}/{child}" for child in children]
            for child in fq_children:
                if child not in self.watchers:
                    self.watch_folder(child, enqueue_children=True)

    def run(self):
        if not self.zk.exists(AUTOSCALING_ZK_ROOT):
            self.zk.ensure_path(AUTOSCALING_ZK_ROOT)
        self.watchers[AUTOSCALING_ZK_ROOT] = self.watch_folder(AUTOSCALING_ZK_ROOT)
        self.is_ready = True
        while True:
            time.sleep(0.1)


class SoaFileWatcher(PaastaWatcher):

    def __init__(self, instances_that_need_to_be_bounced_in_the_future, cluster, config, **kwargs):
        super().__init__(instances_that_need_to_be_bounced_in_the_future, cluster, config)
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

    def __init__(self, instances_that_need_to_be_bounced_in_the_future, cluster, config, **kwargs):
        super().__init__(instances_that_need_to_be_bounced_in_the_future, cluster, config)
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
    def __init__(self, instances_that_need_to_be_bounced_in_the_future, cluster, config, **kwargs):
        super().__init__(instances_that_need_to_be_bounced_in_the_future, cluster, config)
        self.draining: Set[str] = set()
        self.marathon_clients = get_marathon_clients_from_config()

    def get_new_draining_hosts(self):
        try:
            draining_hosts = get_draining_hosts()
        except RequestException as e:
            self.log.error(f"Unable to get list of draining hosts from mesos: {e}")
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
            service_instances: List[ServiceInstance] = []
            if new_draining_hosts:
                self.log.info(f"Found new draining hosts: {new_draining_hosts}")
                service_instances = self.get_at_risk_service_instances(new_draining_hosts)
            for service_instance in service_instances:
                self.instances_that_need_to_be_bounced_in_the_future.put(service_instance)
            time.sleep(self.config.get_deployd_maintenance_polling_frequency())

    def get_at_risk_service_instances(self, draining_hosts) -> List[ServiceInstance]:
        marathon_apps_with_clients = get_marathon_apps_with_clients(
            clients=self.marathon_clients.get_all_clients(),
            embed_tasks=True,
        )
        at_risk_tasks = []
        for app, client in marathon_apps_with_clients:
            for task in app.tasks:
                if task.host in draining_hosts:
                    at_risk_tasks.append(task)
        self.log.info(f"At risk tasks: {at_risk_tasks}")
        service_instances: List[ServiceInstance] = []
        for task in at_risk_tasks:
            app_id = task.app_id.strip('/')
            service, instance, _, __ = deformat_job_id(app_id)
            # check we haven't already added this instance,
            # no need to add the same instance to the bounce queue
            # more than once
            if not any([(service, instance) == (si.service, si.instance) for si in service_instances]):
                # https://github.com/python/mypy/issues/2852
                service_instances.append(ServiceInstance(  # type: ignore
                    service=service,
                    instance=instance,
                    cluster=self.config.get_cluster(),
                    bounce_by=int(time.time()),
                    watcher=type(self).__name__,
                    bounce_timers=None,
                    failures=0,
                ))
        return service_instances


class PublicConfigEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher
        self.public_config = load_system_paasta_config()
        self.marathon_clients = get_marathon_clients_from_config()

    @property
    def log(self):
        name = '.'.join([__name__, type(self).__name__])
        return logging.getLogger(name)

    def filter_event(self, event):
        if event.name.endswith('.json') or event.maskname == 'IN_CREATE|IN_ISDIR':
            return event

    def watch_new_folder(self, event):
        if event.maskname == 'IN_CREATE|IN_ISDIR' and '.~tmp~' not in event.pathname:
            self.filewatcher.wm.add_watch(event.pathname, self.filewatcher.mask, rec=True)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        event = self.filter_event(event)
        if event:
            self.log.debug("Public config changed on disk, loading new config.")
            try:
                new_config = load_system_paasta_config()
            except ValueError:
                self.log.error("Couldn't load public config, the JSON is invalid!")
                return
            service_instances: List[Tuple[str, str]] = []
            if new_config != self.public_config:
                self.log.info("Public config has changed, now checking if it affects any services config shas.")
                self.public_config = new_config
                all_service_instances = get_services_for_cluster(
                    cluster=self.public_config.get_cluster(),
                    instance_type='marathon',
                    soa_dir=DEFAULT_SOA_DIR,
                )
                service_instances = get_service_instances_needing_update(
                    self.marathon_clients,
                    all_service_instances,
                    self.public_config.get_cluster(),
                )
            if service_instances:
                self.log.info(f"{len(service_instances)} service instances affected. Doing a staggered bounce.")
                bounce_rate = self.public_config.get_deployd_big_bounce_rate()
                for service_instance in rate_limit_instances(
                    instances=service_instances,
                    cluster=self.public_config.get_cluster(),
                    number_per_minute=bounce_rate,
                    watcher_name=type(self).__name__,
                    priority=99,
                ):
                    self.filewatcher.instances_that_need_to_be_bounced_in_the_future.put(service_instance)


class YelpSoaEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher
        self.marathon_clients = get_marathon_clients_from_config()

    @property
    def log(self):
        name = '.'.join([__name__, type(self).__name__])
        return logging.getLogger(name)

    def get_service_name_from_event(self, event):
        """Get service_name from the file inotify event,
        returns None if it is not an event we're interested in"""
        starts_with = ['marathon-', 'deployments.json']
        if any([event.name.startswith(x) for x in starts_with]):
            service_name = event.path.split('/')[-1]
        elif event.name.endswith('.json') and event.path.split('/')[-1] == 'secrets':
            # this is needed because we put the secrets json files in a
            # subdirectory so the service name would be "secrets" otherwise
            service_name = event.path.split('/')[-2]
        else:
            service_name = None
        return service_name

    def watch_new_folder(self, event):
        if event.maskname == 'IN_CREATE|IN_ISDIR' and '.~tmp~' not in event.pathname:
            self.filewatcher.wm.add_watch(event.pathname, self.filewatcher.mask, rec=True)
            try:
                file_names = os.listdir(event.pathname)
            except OSError:
                return
            if any(['marathon-' in file_name for file_name in file_names]):
                self.log.info(f"New folder with marathon files: {event.name}.")
                self.bounce_service(event.name)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        service_name = self.get_service_name_from_event(event)
        if service_name:
            self.log.info(f"Looking for things to bounce for {service_name} because {event.path}/{event.name} changed.")
            self.bounce_service(service_name)

    def bounce_service(self, service_name):
        self.log.info(f"Checking if any marathon instances of {service_name} need bouncing.")
        instances = list_all_instances_for_service(
            service=service_name,
            clusters=[self.filewatcher.cluster],
            instance_type='marathon',
            cache=False,
        )
        self.log.debug(instances)
        service_instances = [(service_name, instance) for instance in instances]
        service_instances = get_service_instances_needing_update(
            self.marathon_clients,
            service_instances,
            self.filewatcher.cluster,
        )
        for service, instance in service_instances:
            self.log.info(f"{service}.{instance} has a new marathon app ID. Enqueuing it to be bounced.")
        service_instances_to_queue = [
            # https://github.com/python/mypy/issues/2852
            ServiceInstance(  # type: ignore
                service=service,
                instance=instance,
                cluster=self.filewatcher.cluster,
                bounce_by=int(time.time()),
                watcher=type(self).__name__,
                bounce_timers=None,
                failures=0,
            )
            for service, instance in service_instances
        ]
        for service_instance in service_instances_to_queue:
            self.filewatcher.instances_that_need_to_be_bounced_in_the_future.put(service_instance)
