import staticconf.testing

from clusterman.autoscaler.config import AutoscalingConfig
from clusterman.autoscaler.config import get_autoscaling_config


def test_get_autoscaling_config():
    default_autoscaling_values = {
        'setpoint': 0.7,
        'target_capacity_margin': 0.1,
        'excluded_resources': ['gpus']
    }
    pool_autoscaling_values = {
        'setpoint': 0.8,
        'excluded_resources': ['cpus']
    }
    with staticconf.testing.MockConfiguration({'autoscaling': default_autoscaling_values}), \
            staticconf.testing.MockConfiguration({'autoscaling': pool_autoscaling_values}, namespace='pool_namespace'):
        autoscaling_config = get_autoscaling_config('pool_namespace')

        assert autoscaling_config == AutoscalingConfig(['cpus'], 0.8, 0.1)
