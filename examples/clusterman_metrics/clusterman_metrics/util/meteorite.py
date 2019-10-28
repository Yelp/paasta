# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Mapping
from typing import Optional
from typing import Tuple

from clusterman_metrics.util.constants import APP_METRICS
from clusterman_metrics.util.constants import CLUSTERMAN_NAME


def _parse_dimensions(metric_name):
    """ Parse out existing dimensions from the metric name """
    try:
        metric_name, dims = metric_name.split('|', 1)
    except ValueError:
        dims = ''

    return metric_name, dict(dim_pair.split('=') for dim_pair in dims.split(',') if dim_pair)


def generate_key_with_dimensions(metric_name: str, dimensions: Optional[Mapping[str, str]] = None) -> str:
    """ Helper function to generate a key used to reference metric timeseries data in DynamoDB; this key will
    be parsed by ``get_meteorite_identifiers`` to store data in SignalFX.

    :param metric_name: the name of the metric (can include some pre-existing dimensions)
    :param dimensions: dict of dimension names to values; dimensions in the metric name will by overwritten by
        values here
    :returns: string that can be passed to ``get_writer`` as the metric key
    """
    if not dimensions:
        return metric_name

    # dimensions passed in override dimensions in the name
    metric_name, new_dimensions = _parse_dimensions(metric_name)
    new_dimensions.update(dimensions)

    dimension_parts = []
    for key, value in sorted(new_dimensions.items()):
        dimension_parts.append('{key}={value}'.format(key=key, value=value))

    return '{metric_name}|{dim_string}'.format(
        metric_name=metric_name,
        dim_string=','.join(dimension_parts),
    )


def get_meteorite_identifiers(metric_type: str, metric_key: str) -> Tuple[str, Optional[Mapping[str, str]]]:
    """
    Given the primary key for a timeseries in the datastore and its Clusterman metric type, return the metric name and
    dimensions for that timeseries in meteorite.

    :param metric_type: string, one of METRIC_TYPES
    :param metric_key: string, the unique key for the timeseries in the datastore.
    :returns: (metric_name, dimensions_dict) tuple. Dimensions may be None.
    """
    dimensions = None
    name_parts = [CLUSTERMAN_NAME, metric_type]

    metric_name, dimensions = _parse_dimensions(metric_key)

    if metric_type == APP_METRICS:
        # Namespace app metrics by the app identifier.
        name_parts.extend(metric_name.split(',', 1))
    else:
        name_parts.append(metric_name)

    meteorite_name = '.'.join(name_parts)
    return meteorite_name, dimensions
