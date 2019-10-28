from typing import List
from typing import NamedTuple

import colorlog
import staticconf

logger = colorlog.getLogger(__name__)


class AutoscalingConfig(NamedTuple):
    excluded_resources: List[str]
    setpoint: float
    target_capacity_margin: float


def get_autoscaling_config(config_namespace: str) -> AutoscalingConfig:
    """ Load autoscaling configuration values from the provided config_namespace, falling back to the
    values stored in the default namespace if none are specified.

    :param config_namespace: namespace to read from before falling back to the default namespace
    :returns: AutoscalingConfig object with loaded config values
    """
    default_excluded_resources = staticconf.read_list('autoscaling.excluded_resources', default=[])
    default_setpoint = staticconf.read_float('autoscaling.setpoint')
    default_target_capacity_margin = staticconf.read_float('autoscaling.target_capacity_margin')

    reader = staticconf.NamespaceReaders(config_namespace)
    return AutoscalingConfig(
        excluded_resources=reader.read_list('autoscaling.excluded_resources', default=default_excluded_resources),
        setpoint=reader.read_float('autoscaling.setpoint', default=default_setpoint),
        target_capacity_margin=reader.read_float(
            'autoscaling.target_capacity_margin',
            default=default_target_capacity_margin,
        ),
    )
