#!/usr/bin/env python
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

import requests
from simplejson.scanner import JSONDecodeError

from paasta_tools.utils import load_system_paasta_config
class PaastaClient(object):
    '''PaaSTA API client'''

    def __init__(self):
        system_paasta_config = load_system_paasta_config()
        self.endpoints = system_paasta_config.get_api_endpoints()

    def get_clusters(self):
        return self.endpoints

    def status(self, cluster, service, instance):
        url = '{url}/v1/{service}/{instance}/status'.format(
                url=self.get_clusters()[cluster], service=service,
                instance=instance)
        res = requests.get(url)
        try:
            return res.json()
        except JSONDecodeError:
            return {}
