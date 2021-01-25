from typing import Any
from typing import Dict

import pytest

from paasta_tools.setup_prometheus_adapter_config import (
    should_create_uwsgi_scaling_rule,
)


@pytest.mark.parametrize(
    "instance_name, instance_config,expected",
    [
        ("_not_a_real_instance", {}, False),
        ("not_autoscaled", {}, False),
        (
            "not_uwsgi_autoscaled",
            {"autoscaling": {"decision_policy": "bespoke"}},
            False,
        ),
        (
            "uwsgi_autoscaled_no_prometheus",
            {"autoscaling": {"metrics_provider": "uwsgi"}},
            False,
        ),
        (
            "uwsgi_autoscaled_prometheus",
            {"autoscaling": {"metrics_provider": "uwsgi", "use_prometheus": True}},
            True,
        ),
    ],
)
def test_should_create_uswgi_scaling_rule(
    instance_name: str, instance_config: Dict[str, Any], expected: bool
) -> None:
    should_create, reason = should_create_uwsgi_scaling_rule(
        instance=instance_name, instance_config=instance_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None
