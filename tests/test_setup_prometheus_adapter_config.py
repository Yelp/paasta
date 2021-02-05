import json
from pathlib import Path

import pytest
import ruamel.yaml as yaml
from py._path.local import LocalPath

from paasta_tools.long_running_service_tools import AutoscalingParamsDict
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_uwsgi_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_prometheus_adapter_config,
)
from paasta_tools.setup_prometheus_adapter_config import (
    should_create_uwsgi_scaling_rule,
)


@pytest.mark.parametrize(
    "instance_name,autoscaling_config,expected",
    [
        (
            "not_uwsgi_autoscaled",
            {
                "metrics_provider": "mesos_cpu",
                "decision_policy": "bespoke",
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            False,
        ),
        (
            "uwsgi_autoscaled_no_prometheus",
            {
                "metrics_provider": "uwsgi",
                "moving_average_window_seconds": 124,
                "setpoint": 0.425,
            },
            False,
        ),
        (
            "uwsgi_autoscaled_prometheus",
            {
                "metrics_provider": "uwsgi",
                "use_prometheus": True,
                "moving_average_window_seconds": 544,
                "setpoint": 0.764,
            },
            True,
        ),
    ],
)
def test_should_create_uswgi_scaling_rule(
    instance_name: str, autoscaling_config: AutoscalingParamsDict, expected: bool
) -> None:
    should_create, reason = should_create_uwsgi_scaling_rule(
        instance=instance_name, autoscaling_config=autoscaling_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None


def test_create_instance_uwsgi_scaling_rule() -> None:
    service_name = "test_service"
    instance_name = "test_instance"
    paasta_cluster = "test_cluster"
    autoscaling_config: AutoscalingParamsDict = {
        "metrics_provider": "uwsgi",
        "setpoint": 0.1234567890,
        "moving_average_window_seconds": 20120302,
        "use_prometheus": True,
    }

    rule = create_instance_uwsgi_scaling_rule(
        service=service_name,
        instance=instance_name,
        paasta_cluster=paasta_cluster,
        autoscaling_config=autoscaling_config,
    )

    # we test that the format of the dictionary is as expected with mypy
    # and we don't want to test the full contents of the retval since then
    # we're basically just writting a change-detector test - instead, we test
    # that we're actually using our inputs
    assert service_name in rule["seriesQuery"]
    assert instance_name in rule["seriesQuery"]
    assert paasta_cluster in rule["seriesQuery"]
    # these two numbers are distinctive and unlikely to be used as constants
    assert str(autoscaling_config["setpoint"]) in rule["metricsQuery"]
    assert (
        str(autoscaling_config["moving_average_window_seconds"]) in rule["metricsQuery"]
    )


def test_create_prometheus_adapter_config(tmpdir: LocalPath) -> None:
    # TODO: if we upgrade to pytest>=3.9, we can use their tmp_path fixture directly
    tmp_path = Path(str(tmpdir))
    service_config = {
        "_shared_env": {"env": {"PAASTA_IS_GREAT": True}},
        "test_instance": {
            "deploy_group": "some-group",
            "min_instances": 1,
            "max_instances": 3,
            "registrations": ["test_service.test_instance"],
            "autoscaling": {
                "metrics_provider": "uwsgi",
                "setpoint": 0.45,
                "use_prometheus": True,
            },
        },
        "another_test_instance": {
            "deploy_group": "some-group",
            "min_instances": 1,
            "max_instances": 3,
            "registrations": ["test_service.another_test_instance"],
            "autoscaling": {
                "metrics_provider": "uwsgi",
                "setpoint": 0.45,
                "use_prometheus": True,
            },
        },
    }
    deployments = {
        "v2": {
            "deployments": {"some-group": {"docker_image": "image", "git_sha": "sha",}},
            "controls": {
                "test_service:some-cluster.test_instance": {
                    "desired_state": "start",
                    "force_bounce": "20210129T005338",
                },
                "test_service:some-cluster.another_test_instance": {
                    "desired_state": "start",
                    "force_bounce": "20210129T005338",
                },
            },
        },
    }

    (tmp_path / "test_service").mkdir()
    (tmp_path / "test_service" / "kubernetes-some-cluster.yaml").write_text(
        yaml.dump(service_config), encoding="utf-8"
    )
    (tmp_path / "test_service" / "deployments.json").write_text(
        json.dumps(deployments), encoding="utf-8"
    )
    config = create_prometheus_adapter_config(
        paasta_cluster="some-cluster", soa_dir=tmp_path
    )

    assert len(config["rules"]) == len(service_config.keys()) - 1
