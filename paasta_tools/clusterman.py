import staticconf

CLUSTERMAN_YAML_FILE_PATH = "/nail/srv/configs/clusterman.yaml"
CLUSTERMAN_METRICS_YAML_FILE_PATH = "/nail/srv/configs/clusterman_metrics.yaml"


def get_clusterman_metrics():
    try:
        import clusterman_metrics
        import clusterman_metrics.util.costs

        clusterman_yaml = CLUSTERMAN_YAML_FILE_PATH
        staticconf.YamlConfiguration(
            CLUSTERMAN_METRICS_YAML_FILE_PATH, namespace="clusterman_metrics"
        )
    except (ImportError, FileNotFoundError):
        # our cluster autoscaler is not currently open source, sorry!
        clusterman_metrics = None
        clusterman_yaml = None

    return clusterman_metrics, clusterman_yaml
