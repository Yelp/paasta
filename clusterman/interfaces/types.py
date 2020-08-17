import enum
from typing import NamedTuple
from typing import Optional

import arrow

from clusterman.aws.markets import InstanceMarket
from clusterman.util import ClustermanResources


class AgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class AgentMetadata(NamedTuple):
    agent_id: str = ''
    allocated_resources: ClustermanResources = ClustermanResources()
    batch_task_count: int = 0
    is_safe_to_kill: bool = True
    state: AgentState = AgentState.UNKNOWN
    task_count: int = 0
    total_resources: ClustermanResources = ClustermanResources()


class InstanceMetadata(NamedTuple):
    market: InstanceMarket
    weight: float
    group_id: str = ''
    hostname: Optional[str] = None
    instance_id: str = ''
    ip_address: Optional[str] = None
    is_stale: bool = False
    state: str = ''
    uptime: arrow.Arrow = 0


class ClusterNodeMetadata(NamedTuple):
    agent: AgentMetadata  # Agent metadata is information associated with the Mesos or Kubernetes agent
    instance: InstanceMetadata  # Instance metadata is information associated with the EC2 instance
