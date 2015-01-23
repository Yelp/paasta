from datetime import datetime

from .base import MarathonResource, MarathonObject
from .constraint import MarathonConstraint
from .container import MarathonContainer
from .deployment import MarathonDeployment
from .task import MarathonTask


class MarathonApp(MarathonResource):
    """Marathon Application resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#post-/v2/apps

    :param list[str] args: args form of the command to run
    :param int backoff_factor: multiplier for subsequent backoff
    :param int backoff_seconds: base time, in seconds, for exponential backoff
    :param str cmd: cmd form of the command to run
    :param constraints: placement constraints
    :type constraints: list[:class:`marathon.models.constraint.MarathonConstraint`] or list[tuple]
    :param container: container info
    :type container: :class:`marathon.models.container.MarathonContainer` or dict
    :param float cpus: cpus required per instance
    :param list[str] dependencies: services (app IDs) on which this app depends
    :param int disk: disk required per instance
    :param deployments: (read-only) currently running deployments that affect this app
    :type deployments: list[:class:`marathon.models.deployment.MarathonDeployment`]
    :param dict env: env vars
    :param str executor: executor
    :param health_checks: health checks
    :type health_checks: list[:class:`marathon.models.MarathonHealthCheck`] or list[dict]
    :param str id: app id
    :param int instances: instances
    :param last_task_failure: last task failure
    :type last_task_failure: :class:`marathon.models.app.MarathonTaskFailure` or dict
    :param float mem: memory (in MB) required per instance
    :param list[int] ports: ports
    :param bool require_ports: require the specified `ports` to be available in the resource offer
    :param list[str] store_urls: store URLs
    :param float task_rate_limit: (Removed in Marathon 0.7.0) maximum number of tasks launched per second
    :param tasks: (read-only) tasks
    :type tasks: list[:class:`marathon.models.task.MarathonTask`]
    :param int tasks_running: (read-only) the number of running tasks
    :param int tasks_staged: (read-only) the number of staged tasks
    :param upgrade_strategy: strategy by which app instances are replaced during a deployment
    :type upgrade_strategy: :class:`marathon.models.app.MarathonUpgradeStrategy` or dict
    :param list[str] uris: uris
    :param str user: user
    :param str version: version id
    """

    UPDATE_OK_ATTRIBUTES = [
        'args', 'backoff_factor', 'backoff_seconds', 'cmd', 'constraints', 'container', 'cpus', 'dependencies', 'disk',
        'env', 'executor', 'health_checks', 'instances', 'mem', 'ports', 'require_ports', 'store_urls',
        'task_rate_limit', 'upgrade_strategy', 'uris', 'user', 'version'
    ]
    """List of attributes which may be updated/changed after app creation"""

    CREATE_ONLY_ATTRIBUTES = ['id']
    """List of attributes that should only be passed on creation"""

    READ_ONLY_ATTRIBUTES = ['deployments', 'tasks', 'tasks_running', 'tasks_staging']
    """List of read-only attributes"""

    def __init__(self, args=None, backoff_factor=None, backoff_seconds=None, cmd=None, constraints=None, container=None,
                 cpus=None, dependencies=None, deployments=None, disk=None, env=None, executor=None, health_checks=None,
                 id=None, instances=None, last_task_failure=None, mem=None, ports=None, require_ports=None,
                 store_urls=None, task_rate_limit=None, tasks=None, tasks_running=None, tasks_staged=None,
                 upgrade_strategy=None, uris=None, user=None, version=None):

        # self.args = args or []
        self.args = args
        # Marathon 0.7.0-RC1 throws a validation error if this is [] and cmd is passed:
        # "error": "AppDefinition must either contain a 'cmd' or a 'container'."

        self.backoff_factor = backoff_factor
        self.backoff_seconds = backoff_seconds
        self.cmd = cmd
        self.constraints = [
            c if isinstance(c, MarathonConstraint) else MarathonConstraint(*c)
            for c in (constraints or [])
        ]
        self.container = container if (isinstance(container, MarathonContainer) or container is None) \
            else MarathonContainer.from_json(container)
        self.cpus = cpus
        self.dependencies = dependencies or []
        self.deployments = [
            d if isinstance(d, MarathonDeployment) else MarathonDeployment().from_json(d)
            for d in (deployments or [])
        ]
        self.disk = disk
        self.env = env
        self.executor = executor
        self.health_checks = health_checks or []
        self.health_checks = [
            hc if isinstance(hc, MarathonHealthCheck) else MarathonHealthCheck().from_json(hc)
            for hc in (health_checks or [])
        ]
        self.id = id
        self.instances = instances
        self.last_task_failure = last_task_failure if (isinstance(last_task_failure, MarathonTaskFailure) or last_task_failure is None) \
            else MarathonTaskFailure.from_json(last_task_failure)
        self.mem = mem
        self.ports = ports or []
        self.require_ports = require_ports
        self.store_urls = store_urls or []
        self.task_rate_limit = task_rate_limit
        self.tasks = [
            t if isinstance(t, MarathonTask) else MarathonTask().from_json(t)
            for t in (tasks or [])
        ]
        self.tasks_running = tasks_running
        self.tasks_staged = tasks_staged
        self.upgrade_strategy = upgrade_strategy if (isinstance(upgrade_strategy, MarathonUpgradeStrategy) or upgrade_strategy is None) \
            else MarathonUpgradeStrategy.from_json(upgrade_strategy)
        self.uris = uris or []
        self.user = user
        self.version = version


class MarathonHealthCheck(MarathonObject):
    """Marathon health check.

    See https://mesosphere.github.io/marathon/docs/health-checks.html

    :param str command: health check command (if protocol == 'COMMAND')
    :param int grace_period_seconds: how long to ignore health check failures on initial task launch (before first healthy status)
    :param int interval_seconds: how long to wait between health checks
    :param int max_consecutive_failures: max number of consecutive failures before the task should be killed
    :param str path: health check target path (if protocol == 'HTTP')
    :param int port_index: target port as indexed in app's `ports` array
    :param str protocol: health check protocol ('HTTP', 'TCP', or 'COMMAND')
    :param int timeout_seconds: how long before a waiting health check is considered failed
    """

    def __init__(self, command=None, grace_period_seconds=None, interval_seconds=None, max_consecutive_failures=None,
                 path=None, port_index=None, protocol=None, timeout_seconds=None):
        self.command = command
        self.grace_period_seconds = grace_period_seconds
        self.interval_seconds = interval_seconds
        self.max_consecutive_failures = max_consecutive_failures
        self.path = path
        self.port_index = port_index
        self.protocol = protocol
        self.timeout_seconds = timeout_seconds


class MarathonTaskFailure(MarathonObject):
    """Marathon Task Failure.

    :param str app_id: application id
    :param str host: mesos slave running the task
    :param str message: error message
    :param str task_id: task id
    :param str state: task state
    :param timestamp: when this task failed
    :type timestamp: datetime or str
    :param str version: app version with which this task was started
    """

    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, app_id=None, host=None, message=None, task_id=None, state=None, timestamp=None, version=None):
        self.app_id = app_id
        self.host = host
        self.message = message
        self.task_id = task_id
        self.state = state
        self.timestamp = timestamp if (timestamp is None or isinstance(timestamp, datetime)) \
            else datetime.strptime(timestamp, self.DATETIME_FORMAT)
        self.version = version


class MarathonUpgradeStrategy(MarathonObject):
    """Marathon health check.

    See https://mesosphere.github.io/marathon/docs/health-checks.html

    :param float minimum_health_capacity: minimum % of instances kept healthy on deploy
    """

    def __init__(self, minimum_health_capacity=None):
        self.minimum_health_capacity = minimum_health_capacity