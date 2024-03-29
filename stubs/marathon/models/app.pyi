# Stubs for marathon.models.app (Python 3.7)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.

from typing import Any, Optional, List, Union
from ..exceptions import InvalidChoiceError as InvalidChoiceError
from .base import (
    MarathonResource as MarathonResource,
    MarathonObject as MarathonObject,
    assert_valid_path as assert_valid_path,
)
from .constraint import MarathonConstraint as MarathonConstraint
from .container import MarathonContainer as MarathonContainer
from .deployment import MarathonDeployment as MarathonDeployment
from .task import MarathonTask as MarathonTask

class MarathonApp(MarathonResource):
    UPDATE_OK_ATTRIBUTES = ...  # type: Any
    CREATE_ONLY_ATTRIBUTES = ...  # type: Any
    READ_ONLY_ATTRIBUTES = ...  # type: Any
    KILL_SELECTIONS = ...  # type: Any
    accepted_resource_roles = ...  # type: List[str]
    args = ...  # type: List[str]
    backoff_factor = ...  # type: Any
    backoff_seconds = ...  # type: Any
    cmd = ...  # type: Any
    constraints = ...  # type: Any
    container = ...  # type: Any
    cpus = ...  # type: Any
    dependencies = ...  # type: Any
    deployments = ...  # type: Any
    disk = ...  # type: Any
    env = ...  # type: Any
    executor = ...  # type: Any
    gpus = ...  # type: Any
    health_checks = ...  # type: Any
    id = ...  # type: str
    instances = ...  # type: Any
    kill_selection = ...  # type: Any
    labels = ...  # type: Any
    last_task_failure = ...  # type: Any
    max_launch_delay_seconds = ...  # type: Any
    mem = ...  # type: Any
    ports = ...  # type: Any
    port_definitions = ...  # type: Any
    readiness_checks = ...  # type: Any
    readiness_check_results = ...  # type: Any
    residency = ...  # type: Any
    require_ports = ...  # type: Any
    secrets = ...  # type: Any
    store_urls = ...  # type: Any
    task_rate_limit = ...  # type: Any
    tasks = ...  # type: List[MarathonTask]
    tasks_running = ...  # type: Any
    tasks_staged = ...  # type: Any
    tasks_healthy = ...  # type: Any
    task_kill_grace_period_seconds = ...  # type: Any
    tasks_unhealthy = ...  # type: Any
    upgrade_strategy = ...  # type: Any
    unreachable_strategy = ...  # type: Any
    uris = ...  # type: Any
    fetch = ...  # type: Any
    user = ...  # type: Any
    version = ...  # type: Any
    version_info = ...  # type: Any
    task_stats = ...  # type: Any
    networks = ...  # type: Any
    def __init__(
        self,
        accepted_resource_roles: Optional[List[str]] = ...,
        args: Optional[List[str]] = ...,
        backoff_factor: Optional[Any] = ...,
        backoff_seconds: Optional[Any] = ...,
        cmd: Optional[Any] = ...,
        constraints: Optional[Any] = ...,
        container: Optional[Any] = ...,
        cpus: Optional[Any] = ...,
        dependencies: Optional[Any] = ...,
        deployments: Optional[Any] = ...,
        disk: Optional[Any] = ...,
        env: Optional[Any] = ...,
        executor: Optional[Any] = ...,
        health_checks: Optional[Any] = ...,
        id: Optional[str] = ...,
        instances: Optional[Any] = ...,
        kill_selection: Optional[Any] = ...,
        labels: Optional[Any] = ...,
        last_task_failure: Optional[Any] = ...,
        max_launch_delay_seconds: Optional[Any] = ...,
        mem: Optional[Any] = ...,
        ports: Optional[Any] = ...,
        require_ports: Optional[Any] = ...,
        store_urls: Optional[Any] = ...,
        task_rate_limit: Optional[Any] = ...,
        tasks: Optional[Union[MarathonTask, str]] = ...,
        tasks_running: Optional[Any] = ...,
        tasks_staged: Optional[Any] = ...,
        tasks_healthy: Optional[Any] = ...,
        task_kill_grace_period_seconds: Optional[Any] = ...,
        tasks_unhealthy: Optional[Any] = ...,
        upgrade_strategy: Optional[Any] = ...,
        unreachable_strategy: Optional[Any] = ...,
        uris: Optional[Any] = ...,
        user: Optional[Any] = ...,
        version: Optional[Any] = ...,
        version_info: Optional[Any] = ...,
        ip_address: Optional[Any] = ...,
        fetch: Optional[Any] = ...,
        task_stats: Optional[Any] = ...,
        readiness_checks: Optional[Any] = ...,
        readiness_check_results: Optional[Any] = ...,
        secrets: Optional[Any] = ...,
        port_definitions: Optional[Any] = ...,
        residency: Optional[Any] = ...,
        gpus: Optional[Any] = ...,
        networks: Optional[Any] = ...,
    ) -> None: ...
    def add_env(self, key, value): ...

class MarathonHealthCheck(MarathonObject):
    command = ...  # type: Any
    grace_period_seconds = ...  # type: Any
    interval_seconds = ...  # type: Any
    max_consecutive_failures = ...  # type: Any
    path = ...  # type: Any
    port_index = ...  # type: Any
    protocol = ...  # type: Any
    timeout_seconds = ...  # type: Any
    ignore_http1xx = ...  # type: Any
    def __init__(
        self,
        command: Optional[Any] = ...,
        grace_period_seconds: Optional[Any] = ...,
        interval_seconds: Optional[Any] = ...,
        max_consecutive_failures: Optional[Any] = ...,
        path: Optional[Any] = ...,
        port_index: Optional[Any] = ...,
        protocol: Optional[Any] = ...,
        timeout_seconds: Optional[Any] = ...,
        ignore_http1xx: Optional[Any] = ...,
        **kwargs,
    ) -> None: ...

class MarathonTaskFailure(MarathonObject):
    DATETIME_FORMAT = ...  # type: str
    app_id = ...  # type: Any
    host = ...  # type: Any
    message = ...  # type: Any
    task_id = ...  # type: Any
    instance_id = ...  # type: Any
    slave_id = ...  # type: Any
    state = ...  # type: Any
    timestamp = ...  # type: Any
    version = ...  # type: Any
    def __init__(
        self,
        app_id: Optional[Any] = ...,
        host: Optional[Any] = ...,
        message: Optional[Any] = ...,
        task_id: Optional[Any] = ...,
        instance_id: Optional[Any] = ...,
        slave_id: Optional[Any] = ...,
        state: Optional[Any] = ...,
        timestamp: Optional[Any] = ...,
        version: Optional[Any] = ...,
    ) -> None: ...

class MarathonUpgradeStrategy(MarathonObject):
    maximum_over_capacity = ...  # type: Any
    minimum_health_capacity = ...  # type: Any
    def __init__(
        self,
        maximum_over_capacity: Optional[Any] = ...,
        minimum_health_capacity: Optional[Any] = ...,
    ) -> None: ...

class MarathonUnreachableStrategy(MarathonObject):
    DISABLED = ...  # type: str
    unreachable_inactive_after_seconds = ...  # type: Any
    unreachable_expunge_after_seconds = ...  # type: Any
    inactive_after_seconds = ...  # type: Any
    expunge_after_seconds = ...  # type: Any
    def __init__(
        self,
        unreachable_inactive_after_seconds: Optional[Any] = ...,
        unreachable_expunge_after_seconds: Optional[Any] = ...,
        inactive_after_seconds: Optional[Any] = ...,
        expunge_after_seconds: Optional[Any] = ...,
    ) -> None: ...
    @classmethod
    def from_json(cls, attributes): ...

class MarathonAppVersionInfo(MarathonObject):
    DATETIME_FORMATS = ...  # type: Any
    last_scaling_at = ...  # type: Any
    last_config_change_at = ...  # type: Any
    def __init__(
        self,
        last_scaling_at: Optional[Any] = ...,
        last_config_change_at: Optional[Any] = ...,
    ) -> None: ...

class MarathonTaskStats(MarathonObject):
    started_after_last_scaling = ...  # type: Any
    with_latest_config = ...  # type: Any
    with_outdated_config = ...  # type: Any
    total_summary = ...  # type: Any
    def __init__(
        self,
        started_after_last_scaling: Optional[Any] = ...,
        with_latest_config: Optional[Any] = ...,
        with_outdated_config: Optional[Any] = ...,
        total_summary: Optional[Any] = ...,
    ) -> None: ...

class MarathonTaskStatsType(MarathonObject):
    stats = ...  # type: Any
    def __init__(self, stats: Optional[Any] = ...) -> None: ...

class MarathonTaskStatsStats(MarathonObject):
    counts = ...  # type: Any
    life_time = ...  # type: Any
    def __init__(
        self, counts: Optional[Any] = ..., life_time: Optional[Any] = ...
    ) -> None: ...

class MarathonTaskStatsCounts(MarathonObject):
    staged = ...  # type: Any
    running = ...  # type: Any
    healthy = ...  # type: Any
    unhealthy = ...  # type: Any
    def __init__(
        self,
        staged: Optional[Any] = ...,
        running: Optional[Any] = ...,
        healthy: Optional[Any] = ...,
        unhealthy: Optional[Any] = ...,
    ) -> None: ...

class MarathonTaskStatsLifeTime(MarathonObject):
    average_seconds = ...  # type: Any
    median_seconds = ...  # type: Any
    def __init__(
        self, average_seconds: Optional[Any] = ..., median_seconds: Optional[Any] = ...
    ) -> None: ...

class ReadinessCheck(MarathonObject):
    name = ...  # type: Any
    protocol = ...  # type: Any
    path = ...  # type: Any
    port_name = ...  # type: Any
    interval_seconds = ...  # type: Any
    http_status_codes_for_ready = ...  # type: Any
    preserve_last_response = ...  # type: Any
    timeout_seconds = ...  # type: Any
    def __init__(
        self,
        name: Optional[Any] = ...,
        protocol: Optional[Any] = ...,
        path: Optional[Any] = ...,
        port_name: Optional[Any] = ...,
        interval_seconds: Optional[Any] = ...,
        http_status_codes_for_ready: Optional[Any] = ...,
        preserve_last_response: Optional[Any] = ...,
        timeout_seconds: Optional[Any] = ...,
    ) -> None: ...

class PortDefinition(MarathonObject):
    port = ...  # type: Any
    protocol = ...  # type: Any
    name = ...  # type: Any
    labels = ...  # type: Any
    def __init__(
        self,
        port: Optional[Any] = ...,
        protocol: Optional[Any] = ...,
        name: Optional[Any] = ...,
        labels: Optional[Any] = ...,
    ) -> None: ...

class Residency(MarathonObject):
    relaunch_escalation_timeout_seconds = ...  # type: Any
    task_lost_behavior = ...  # type: Any
    def __init__(
        self,
        relaunch_escalation_timeout_seconds: Optional[Any] = ...,
        task_lost_behavior: Optional[Any] = ...,
    ) -> None: ...

class Secret(MarathonObject):
    source = ...  # type: Any
    def __init__(self, source: Optional[Any] = ...) -> None: ...
