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


monitoring_keys = ['team', 'notification_email', 'service_type',
                   'runbook', 'tip', 'page', 'alert_after', 'realert_every',
                   'extra']


def extract_classic_monitoring_info(service_config):
    monitoring_config = copy.deepcopy(service_config.get('monitoring', {}))
    info = dict([(key, monitoring_config.pop(key, None)) for
                 key in monitoring_keys])
    # Note, this will clobber whatever was in the extra key before, which
    # we need to do for extra to actually mean what we want.
    info['extra'] = monitoring_config
    return info


def extract_monitoring_info(framework, config, **kwargs):
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
        raise Exception("Can not extract from {0}".format(framework))

    return extractor(config, **kwargs)
