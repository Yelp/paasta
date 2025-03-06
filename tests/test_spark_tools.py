import sys
from unittest import mock

import pytest

from paasta_tools import spark_tools
from paasta_tools.spark_tools import auto_add_timeout_for_spark_job


def test_get_webui_url():
    with mock.patch("socket.getfqdn", return_value="1.2.3.4"):
        assert spark_tools.get_webui_url("1234") == "http://1.2.3.4:1234"


@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("spark-shell", "spark-shell --conf spark.max.cores=100"),
        (
            "/venv/bin/pyspark test.py",
            "/venv/bin/pyspark --conf spark.max.cores=100 test.py",
        ),
        (
            "spark-submit script.py --other args",
            "spark-submit --conf spark.max.cores=100 script.py --other args",
        ),
        ("history-server", "history-server"),
    ],
)
def test_inject_spark_conf_str(cmd, expected):
    assert (
        spark_tools.inject_spark_conf_str(cmd, "--conf spark.max.cores=100") == expected
    )


@pytest.mark.parametrize(
    "spark_conf,expected",
    [
        (
            {
                "spark.kubernetes.executor.volumes.hostPath.nailsrv-123.mount.path": "/nail/srv",
                "spark.kubernetes.executor.volumes.hostPath.nailsrv-123.options.path": "/nail/srv",
                "spark.kubernetes.executor.volumes.hostPath.nailsrv-123.mount.readOnly": "true",
                "spark.kubernetes.executor.volumes.hostPath.123.mount.path": "/nail/123",
                "spark.kubernetes.executor.volumes.hostPath.123.options.path": "/nail/123",
                "spark.kubernetes.executor.volumes.hostPath.123.mount.readOnly": "false",
            },
            ["/nail/srv:/nail/srv:ro", "/nail/123:/nail/123:rw"],
        ),
        (
            {
                "spark.kubernetes.executor.volumes.hostPath.NAILsrv-123.mount.path": "/one/two",
                "spark.kubernetes.executor.volumes.hostPath.NAILsrv-123.options.path": "/one/two",
                "spark.kubernetes.executor.volumes.hostPath.NAILsrv-123.mount.readOnly": "true",
            },
            [""],
        ),
    ],
)
@mock.patch.object(sys, "exit")
def test_get_volumes_from_spark_k8s_configs(mock_sys, spark_conf, expected):
    result = spark_tools.get_volumes_from_spark_k8s_configs(spark_conf)
    if (
        "spark.kubernetes.executor.volumes.hostPath.NAILsrv-123.mount.path"
        in spark_conf
    ):
        mock_sys.assert_called_with(1)
    else:
        assert result == expected


@pytest.mark.parametrize(
    "spark_config,expected",
    [
        # Empty config
        ({}, {}),
        # Only UI port specified
        (
            {"spark.ui.port": "4040"},
            {
                "prometheus.io/port": "4040",
                "prometheus.io/path": "/metrics/prometheus",
            },
        ),
        # Only service and instance specified
        (
            {
                "spark.kubernetes.executor.annotation.paasta.yelp.com/service": "my-service",
                "spark.kubernetes.executor.annotation.paasta.yelp.com/instance": "my-instance",
            },
            {
                "paasta.yelp.com/service": "my-service",
                "paasta.yelp.com/instance": "my-instance",
            },
        ),
        # All annotations specified
        (
            {
                "spark.ui.port": "4040",
                "spark.kubernetes.executor.annotation.paasta.yelp.com/service": "my-service",
                "spark.kubernetes.executor.annotation.paasta.yelp.com/instance": "my-instance",
            },
            {
                "prometheus.io/port": "4040",
                "prometheus.io/path": "/metrics/prometheus",
                "paasta.yelp.com/service": "my-service",
                "paasta.yelp.com/instance": "my-instance",
            },
        ),
        # Missing service
        (
            {
                "spark.ui.port": "4040",
                "spark.kubernetes.executor.annotation.paasta.yelp.com/instance": "my-instance",
            },
            {
                "prometheus.io/port": "4040",
                "prometheus.io/path": "/metrics/prometheus",
            },
        ),
        # Missing instance
        (
            {
                "spark.ui.port": "4040",
                "spark.kubernetes.executor.annotation.paasta.yelp.com/service": "my-service",
            },
            {
                "prometheus.io/port": "4040",
                "prometheus.io/path": "/metrics/prometheus",
            },
        ),
    ],
)
def test_get_spark_driver_monitoring_annotations(spark_config, expected):
    result = spark_tools.get_spark_driver_monitoring_annotations(spark_config)
    assert result == expected


@pytest.mark.parametrize(
    argnames=[
        "cmd",
        "timeout_duration",
        "expected",
    ],
    argvalues=[
        pytest.param(
            "spark-submit abc.py",
            "4h",
            "timeout 4h spark-submit abc.py",
            id="No timeout",
        ),
        pytest.param(
            "timeout 2h spark-submit abc.py",
            "12h",
            "timeout 2h spark-submit abc.py",
            id="Timeout without options",
        ),
        pytest.param(
            "timeout -v 2h spark-submit abc.py",
            "12h",
            "timeout -v 2h spark-submit abc.py",
            id="Timeout with options",
        ),
        pytest.param(
            "timeout -v -s 1 2h spark-submit abc.py",
            "12h",
            "timeout -v -s 1 2h spark-submit abc.py",
            id="Timeout with multiple options",
        ),
        pytest.param(
            "timeout -k 10m --signal=SIGKILL 2h spark-submit abc.py",
            "12h",
            "timeout -k 10m --signal=SIGKILL 2h spark-submit abc.py",
            id="Timeout with double dash option",
        ),
    ],
)
def test_auto_add_timeout_for_spark_job(cmd, timeout_duration, expected):
    result = auto_add_timeout_for_spark_job(cmd, timeout_duration)

    assert result == expected
