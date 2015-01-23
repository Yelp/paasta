from datetime import datetime

from .base import MarathonResource, MarathonObject


class MarathonTask(MarathonResource):
    """Marathon Task resource.

    :param str app_id: application id
    :param health_check_results: health check results
    :type health_check_results: list[:class:`marathon.models.MarathonHealthCheckResult`] or list[dict]
    :param str host: mesos slave running the task
    :param str id: task id
    :param list[int] ports: allocated ports
    :param list[int] service_ports: ports exposed for load balancing
    :param staged_at: when this task was staged
    :type staged_at: datetime or str
    :param started_at: when this task was started
    :type started_at: datetime or str
    :param str version: app version with which this task was started
    """

    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, app_id=None, health_check_results=None, host=None, id=None, ports=None, service_ports=None,
                 staged_at=None, started_at=None, version=None):
        self.app_id = app_id
        self.health_check_results = health_check_results or []
        self.health_check_results = [
            hcr if isinstance(hcr, MarathonHealthCheckResult) else MarathonHealthCheckResult().from_json(hcr)
            for hcr in (health_check_results or []) if any(health_check_results)
        ]
        self.host = host
        self.id = id
        self.ports = ports or []
        self.service_ports = service_ports or []
        self.staged_at = staged_at if (staged_at is None or isinstance(staged_at, datetime)) \
            else datetime.strptime(staged_at, self.DATETIME_FORMAT)
        self.started_at = started_at if (started_at is None or isinstance(started_at, datetime)) \
            else datetime.strptime(started_at, self.DATETIME_FORMAT)
        self.version = version


class MarathonHealthCheckResult(MarathonObject):
    """Marathon health check result.

    See https://mesosphere.github.io/marathon/docs/health-checks.html

    :param bool alive: boolean to determine task health
    :param int consecutive_failures: number of failed healthchecks in a row
    :param str first_success: first time when which healthcheck succeeded
    :param str last_failure: last time when which healthcheck failed
    :param str last_success: last time when which healthcheck succeeded
    :param str task_id: task id
    """

    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, alive=None, consecutive_failures=None, first_success=None,
                 last_failure=None, last_success=None, task_id=None):
        self.alive = alive
        self.consecutive_failures = consecutive_failures
        self.first_success = first_success if (first_success is None or isinstance(first_success, datetime)) \
            else datetime.strptime(first_success, self.DATETIME_FORMAT)
        self.last_failure = last_failure if (last_failure is None or isinstance(last_failure, datetime)) \
            else datetime.strptime(last_failure, self.DATETIME_FORMAT)
        self.last_success = last_success if (last_success is None or isinstance(last_success, datetime)) \
            else datetime.strptime(last_success, self.DATETIME_FORMAT)
        self.task_id = task_id
