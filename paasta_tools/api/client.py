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
"""
Client interface for the Paasta rest api.
"""
import logging

import requests

from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaNotConfiguredError


log = logging.getLogger(__name__)


class PaastaApiError(Exception):
    pass


class PaastaApiClient(object):
    def __init__(self, cluster=None, system_paasta_config=None, timeout=30):
        """Create a PaastaApiClient instance.
        :param str cluster: name of the cluster of an api server
        :param int timeout: Timeout (in seconds) for a request to an api endpoint
        :param :class:`requests.Session` session: the session for request and response
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': get_user_agent()})

        try:
            if not system_paasta_config:
                system_paasta_config = load_system_paasta_config()
            api_endpoints = system_paasta_config.get_api_endpoints()
            if not cluster:
                cluster = system_paasta_config.get_cluster()
            self.server = api_endpoints[cluster]
        except:
            raise PaastaNotConfiguredError(
                "Could not find cluster {0} in system paasta configuration directory".format(cluster))

    def _do_request(self, method, path, params=None, data=None):
        """Hit the api server."""
        headers = {
            'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = ''.join([self.server.rstrip('/'), path])
        try:
            response = self.session.request(method,
                                            url,
                                            params=params,
                                            data=data,
                                            headers=headers,
                                            timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            log.error('Paasta api error while calling %s: %s', url, str(e))
            raise PaastaApiError('No remaining Marathon servers to try')

        if response.status_code >= 300:
            log.error('Paasta api got HTTP {code}: {body}'.format(
                code=response.status_code, body=response.text))
            raise PaastaApiError(response)
        else:
            log.debug('Paasta api got HTTP {code}: {body}'.format(
                code=response.status_code, body=response.text))

        return response

    def list_instances(self, service):
        """List instances of a paasta service."""
        response = self._do_request(
            'GET', '/v1/services/{service_name}'.format(service_name=service))
        response_json = response.json()
        if 'instances' in response_json:
            return response_json['instances']
        else:
            log.error('Paasta api list_instances got HTTP {code}: {body}'.format(
                code=response.status_code, body=response.text))
            raise PaastaApiError(response)

    def instance_status(self, service, instance, verbose=False):
        """Get status of a paasta service instance."""
        params = {'verbose': verbose}
        response = self._do_request(
            'GET', '/v1/services/{service_name}/{instance_name}/status'.format(
                service_name=service, instance_name=instance),
            params=params)
        response_json = response.json()
        if service == response_json['service'] and instance == response_json['instance']:
            return response_json
        else:
            log.error('Paasta api instance_status got HTTP {code}: {body}'.format(
                code=response.status_code, body=response.text))
            raise PaastaApiError(response)
