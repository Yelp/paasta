from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterInfo:
    name: str
    api_endpoint: str


@dataclass(frozen=True)
class ServiceInfo:
    name: str
