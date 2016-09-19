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
Responds to paasta service and instance requests.
"""
import argparse
import logging
import os
import sys

import requests_cache
import service_configuration_lib
from gevent.wsgi import WSGIServer
from pyramid.config import Configurator

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def parse_paasta_api_args():
    parser = argparse.ArgumentParser(description='Runs a PaaSTA API server')
    parser.add_argument(
        '-D', '--debug',
        dest='debug',
        action='store_true', default=False,
        help="output the debug logs"
    )
    parser.add_argument(
        'port', type=int,
        help="port number for the api server")
    parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        help="define a different soa config directory"
    )
    args = parser.parse_args()
    return args


def make_app():
    paasta_api_path = os.path.dirname(sys.modules['paasta_tools.api'].__file__)
    config = Configurator(settings={
        'service_name': 'paasta-api',
        'pyramid_swagger.schema_directory': os.path.join(paasta_api_path, 'api_docs'),
        'pyramid_swagger.skip_validation': ['/(static)\\b', '/(status)\\b', '/(swagger.json)\\b'],
        'pyramid_swagger.swagger_versions': ['2.0'],
    })

    config.include('pyramid_swagger')
    config.add_route('service.instance.status', '/v1/services/{service}/{instance}/status')
    config.add_route('service.list', '/v1/services/{service}')
    config.add_route('service.autoscaler.get', '/v1/services/{service}/{instance}/autoscaler', request_method="GET")
    config.add_route('service.autoscaler.post', '/v1/services/{service}/{instance}/autoscaler', request_method="POST")
    config.add_route('version', '/v1/version')
    config.scan()
    return config.make_wsgi_app()


def main(argv=None):
    args = parse_paasta_api_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    if args.soa_dir:
        settings.soa_dir = args.soa_dir

    # pyinotify is a better solution than turning off file caching completely
    service_configuration_lib.disable_yaml_cache()

    # Exit on exceptions while loading settings
    settings.cluster = load_system_paasta_config().get_cluster()

    marathon_config = marathon_tools.load_marathon_config()
    settings.marathon_client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )

    # Set up transparent cache for http API calls. With expire_after, responses
    # are removed only when the same request is made. Expired storage is not a
    # concern here. Thus remove_expired_responses is not needed.
    requests_cache.install_cache("paasta-api", backend="memory", expire_after=30)

    server = WSGIServer(('', int(args.port)), make_app())
    log.info("paasta-api started on port %d with soa_dir %s" % (args.port, settings.soa_dir))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == '__main__':
    main()
