import sys
from unittest import mock

import pytest

from paasta_tools import spark_tools


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
