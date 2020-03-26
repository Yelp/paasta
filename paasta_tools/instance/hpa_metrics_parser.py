class HPAMetricsParser:
    def __init__(self, hpa):
        self.NAME = "name"
        self.TARGET = "target_value"
        self.CURRENT = "current_value"

    def parse_target(self, metric):
        """
        Parse target metrics.
        """
        metric_spec = getattr(metric, metric.type.lower())
        status = {}
        switchers = {
            "Pods": self.parse_pod_metric,
            "External": self.parse_external_metric,
            "Resource": self.parse_resource_metric,
        }
        switchers[metric.type](metric_spec, status)
        status[self.TARGET] = str(status[self.TARGET]) if status[self.TARGET] else "N/A"
        return status

    def parse_current(self, metric):
        """
        Parse current metrics
        """
        metric_spec = getattr(metric, metric.type)
        status = {}
        status[self.NAME] = metric_spec.metric_name
        switchers = {
            "Pods": self.parse_pod_metric_current,
            "External": self.parse_external_metric_current,
            "Resource": self.parse_resource_metric_current,
        }
        switchers[metric.type](metric_spec, status)
        status[self.CURRENT] = (
            str(status[self.CURRENT]) if status[self.CURRENT] else "N/A"
        )
        return status

    def parse_external_metric(self, metric_spec, status):
        status[self.NAME] = metric_spec.metric_name
        status[self.TARGET] = (
            metric_spec.target_average_value
            if getattr(metric_spec, "target_average_value")
            else metric_spec.target_value
        )

    def parse_external_metric_current(self, metric_spec, status):
        status[self.NAME] = metric_spec.metric_name
        status[self.CURRENT] = (
            metric_spec.current_average_value
            if getattr(metric_spec, "current_average_value")
            else metric_spec.current_value
        )

    def parse_pod_metric(self, metric_spec, status):
        status[self.NAME] = metric_spec.metric_name
        status[self.TARGET] = metric_spec.target_average_value

    def parse_pod_metric_current(self, metric_spec, status):
        status[self.NAME] = metric_spec.metric_name
        status[self.CURRENT] = metric_spec.current_value

    def parse_resource_metric(self, metric_spec, status):
        status[self.NAME] = metric_spec.name
        status[self.TARGET] = (
            metric_spec.target_average_value
            if getattr(metric_spec, "target_average_value")
            else metric_spec.target_average_utilization
        )

    def parse_resource_metric_current(self, metric_spec, status):
        status[self.NAME] = metric_spec.name
        status[self.CURRENT] = (
            metric_spec.current_average_value
            if getattr(metric_spec, "current_average_value")
            else metric_spec.current_average_utilization
        )
