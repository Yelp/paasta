from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterInfo:
    name: str
    api_endpoint: str


@dataclass(frozen=True)
class ServiceInfo:
    name: str


@dataclass(frozen=True)
class InstanceInfo:
    name: str
    instance_type: str
    state: str
    ready: int
    desired: int
    git_sha: str
    num_versions: int
    error: str
