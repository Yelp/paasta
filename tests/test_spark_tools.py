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
