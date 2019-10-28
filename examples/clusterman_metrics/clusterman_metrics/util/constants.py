

CONFIG_NAMESPACE = 'clusterman_metrics'

CLUSTERMAN_NAME = 'clusterman'

SYSTEM_METRICS = 'system_metrics'  #: metrics collected about the cluster state (e.g., CPU, memory allocation)
APP_METRICS = 'app_metrics'  #: metrics collected from client applications (e.g., number of application runs)
METADATA = 'metadata'  #: metrics collected about the cluster (e.g., current spot prices, instance types present)

METRIC_TYPES = frozenset([
    SYSTEM_METRICS,
    APP_METRICS,
    METADATA,
])
