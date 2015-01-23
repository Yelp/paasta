from .base import MarathonObject, MarathonResource


class MarathonInfo(MarathonResource):
    """Marathon Info.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#get-/v2/info

    :param str framework_id:
    :param str leader:
    :param marathon_config:
    :type marathon_config: :class:`marathon.models.info.MarathonConfig` or dict
    :param str name:
    :param str version:
    :param zookeeper_config:
    :type zookeeper_config: :class:`marathon.models.info.MarathonZooKeeperConfig` or dict
    :param http_config:
    :type http_config: :class:`marathon.models.info.MarathonHttpConfig` or dict
    :param event_subscriber:
    :type event_subscriber: :class`marathon.models.info.MarathonEventSubscriber` or dict
    """

    def __init__(self, event_subscriber=None, framework_id=None, http_config=None, leader=None, marathon_config=None,
                 name=None, version=None, zookeeper_config=None):
        self.event_subscriber = event_subscriber if isinstance(event_subscriber, MarathonEventSubscriber) \
            else MarathonEventSubscriber().from_json(event_subscriber)
        self.framework_id = framework_id
        self.http_config = http_config if isinstance(http_config, MarathonHttpConfig) \
            else MarathonHttpConfig().from_json(http_config)
        self.leader = leader
        self.marathon_config = marathon_config if isinstance(marathon_config, MarathonConfig) \
            else MarathonConfig().from_json(marathon_config)
        self.name = name
        self.version = version
        self.zookeeper_config = zookeeper_config if isinstance(zookeeper_config, MarathonZooKeeperConfig) \
            else MarathonZooKeeperConfig().from_json(zookeeper_config)


class MarathonConfig(MarathonObject):
    """Marathon config resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#get-/v2/info

    :param bool checkpoint:
    :param str executor:
    :param int failover_timeout:
    :param bool ha:
    :param str hostname:
    :param int local_port_min:
    :param int local_port_max:
    :param str master:
    :param str mesos_role:
    :param str mesos_user:
    :param int reconciliation_initial_delay:
    :param int reconciliation_interval:
    :param int task_launch_timeout:
    """

    def __init__(self, checkpoint=None, executor=None, failover_timeout=None, ha=None, hostname=None,
                 local_port_min=None, local_port_max=None, master=None, mesos_role=None, mesos_user=None,
                 reconciliation_initial_delay=None, reconciliation_interval=None, task_launch_timeout=None):
        self.checkpoint = checkpoint
        self.executor = executor
        self.failover_timeout = failover_timeout
        self.ha = ha
        self.hostname = hostname
        self.local_port_min = local_port_min
        self.local_port_max = local_port_max
        self.master = master
        self.mesos_role = mesos_role
        self.mesos_user = mesos_user
        self.reconciliation_initial_delay = reconciliation_initial_delay
        self.reconciliation_interval = reconciliation_interval
        self.task_launch_timeout = task_launch_timeout


class MarathonZooKeeperConfig(MarathonObject):
    """Marathon zookeeper config resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#get-/v2/info

    :param str zk:
    :param dict zk_future_timeout:
    :param str zk_hosts:
    :param str zk_path:
    :param str zk_state:
    :param int zk_timeout:
    """

    def __init__(self, zk=None, zk_future_timeout=None, zk_hosts=None, zk_path=None, zk_state=None, zk_timeout=None):
        self.zk = zk
        self.zk_future_timeout = zk_future_timeout
        self.zk_hosts = zk_hosts
        self.zk_path = zk_path
        self.zk_state = zk_state
        self.zk_timeout = zk_timeout


class MarathonHttpConfig(MarathonObject):
    """Marathon http config resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#get-/v2/info

    :param str assets_path:
    :param int http_port:
    :param int https_port:
    """

    def __init__(self, assets_path=None, http_port=None, https_port=None):
        self.assets_path = assets_path
        self.http_port = http_port
        self.https_port = https_port


class MarathonEventSubscriber(MarathonObject):
    """Marathon event subscriber resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#get-/v2/info

    :param str type:
    :param list[str] http_endpoints:
    """

    def __init__(self, type=None, http_endpoints=None):
        self.type = type
        self.http_endpoints = http_endpoints
