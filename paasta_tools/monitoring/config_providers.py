# Copyright 2015-2016 Yelp Inc.
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
"""A generic way to extract monitoring information from various sources.

This is abstracted so that the new marathon system can use the same
interface with a different provider.

"""
import copy
from typing import Any
from typing import Dict
from typing import Union

from mypy_extensions import TypedDict


MonitoringInfo = TypedDict(
    'MonitoringInfo',
    {
        "team": Union[str, None],
        "notification_email": Union[str, None],
        "service_type": Union[str, None],
        "runbook": Union[str, None],
        "tip": Union[str, None],
        "page": Union[str, None],
        "alert_after": Union[str, None],
        "realert_every": Union[str, None],
        "extra": Dict[Any, Any],
    },
)


def extract_classic_monitoring_info(service_config, **kwargs) -> MonitoringInfo:
    monitoring_config = copy.deepcopy(service_config.get('monitoring', {}))

    info = MonitoringInfo({
        "team": monitoring_config.pop("team", None),
        "notification_email": monitoring_config.pop("notification_email", None),
        "service_type": monitoring_config.pop("service_type", None),
        "runbook": monitoring_config.pop("runbook", None),
        "tip": monitoring_config.pop("tip", None),
        "page": monitoring_config.pop("page", None),
        "alert_after": monitoring_config.pop("alert_after", None),
        "realert_every": monitoring_config.pop("realert_every", None),
        # Note, this will clobber whatever was in the extra key before, which
        # we need to do for extra to actually mean what we want.
        "extra": monitoring_config,
    })
    return info


def extract_monitoring_info(framework, config, **kwargs) -> MonitoringInfo:
    """Extract monitoring information from wherever it is specified

    To be monitored a service *must* supply a team and notification email.

    :param framework: A string that represents one of the understood
                      frameworks in the set: ``('marathon', 'classic')``
    :param config: A dictionary containing the config source information.
                   This must be the input format expected by the extractor
    :param kwargs: Any info you want to pass to the appropriate extractor

    :returns A dictionary containing the following keys:

    - team: The team to send pages to
    - notification_email: The email to send emails to
    -  service_type: Must be "classic" for this check to run
    -  runbook: The runbook to refer oncall members to
    -  tip: tip for oncall members
    -  page: Whether to page the provided team on failure
    -  alert_after: Time before alerting, e.g. "0s" or "10m"
    -  extra: Any additional information provided by the framework
    """
    extractor = {
        'classic': extract_classic_monitoring_info
    }.get(framework)

    if extractor is None:
        raise Exception(f"Can not extract from {framework}")

    return extractor(config, **kwargs)
