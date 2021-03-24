from typing import Optional

from kubernetes.client.models.v2beta2_object_metric_status import (
    V2beta2ObjectMetricStatus,
)
from mypy_extensions import TypedDict


class HPAMetricsDict(TypedDict, total=False):
    name: str
    target_value: str
    current_value: str


class HPAMetricsParser:
    def __init__(self, hpa):
        self.NAME = "name"
        self.TARGET = "target_value"
        self.CURRENT = "current_value"

    def parse_target(self, metric) -> HPAMetricsDict:
        """
        Parse target metrics.
        """
        metric_spec = getattr(metric, metric.type.lower())
        status: HPAMetricsDict = {}
        switchers = {
            "Pods": self.parse_pod_metric,
            "External": self.parse_external_metric,
            "Resource": self.parse_resource_metric,
            "Object": self.parse_object_metric,
        }
        switchers[metric.type](metric_spec, status)
        status["target_value"] = (
            str(status["target_value"]) if status["target_value"] else "N/A"
        )
        return status

    def parse_current(self, metric) -> Optional[HPAMetricsDict]:
        """
        Parse current metrics
        """
        status: HPAMetricsDict = {}

        # Sometimes, when the custom metrics adapter is having a bad day, we get metric = {"type": ""}
        # Try to gracefully handle that.
        try:
            metric_spec = getattr(metric, metric.type.lower())
        except AttributeError:
            return None

        switchers = {
            "Pods": self.parse_pod_metric_current,
            "External": self.parse_external_metric_current,
            "Resource": self.parse_resource_metric_current,
            "Object": self.parse_object_metric_current,
        }
        switchers[metric.type](metric_spec, status)
        status["current_value"] = (
            str(status["current_value"]) if status["current_value"] else "N/A"
        )
        return status

    def parse_external_metric(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.metric.name
        status["target_value"] = (
            metric_spec.target.average_value
            if getattr(metric_spec.target, "average_value")
            else metric_spec.target.value
        )

    def parse_external_metric_current(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.metric.name
        status["current_value"] = (
            metric_spec.current.average_value
            if getattr(metric_spec.current, "average_value")
            else metric_spec.current.value
        )

    def parse_pod_metric(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.metric.name
        status["target_value"] = metric_spec.target.average_value

    def parse_pod_metric_current(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.metric.name
        status["current_value"] = metric_spec.current.average_value

    def parse_resource_metric(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.name
        status["target_value"] = (
            metric_spec.target.average_value
            if getattr(metric_spec.target, "average_value")
            else metric_spec.target.average_utilization
        )

    def parse_resource_metric_current(self, metric_spec, status: HPAMetricsDict):
        status["name"] = metric_spec.name
        status["current_value"] = (
            metric_spec.current.average_value
            if getattr(metric_spec.current, "average_value")
            else metric_spec.current.average_utilization
        )

    def parse_object_metric(
        self, metric_spec: V2beta2ObjectMetricStatus, status: HPAMetricsDict
    ) -> None:
        status["name"] = metric_spec.metric.name
        status["target_value"] = (
            metric_spec.target.average_value
            if getattr(metric_spec.target, "average_value")
            else metric_spec.target.value
        )

    def parse_object_metric_current(
        self, metric_spec: V2beta2ObjectMetricStatus, status: HPAMetricsDict
    ) -> None:
        status["name"] = metric_spec.metric.name
        status["current_value"] = (
            metric_spec.current.average_value
            if getattr(metric_spec.current, "average_value")
            else metric_spec.current.value
        )
