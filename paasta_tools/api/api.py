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

import manhole
import requests_cache
import service_configuration_lib
from pyramid.config import Configurator
from wsgicors import CORS

import paasta_tools.api
from paasta_tools import kubernetes_tools
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
        help="output the debug logs",
    )
    parser.add_argument(
        'port', type=int,
        help="port number for the api server",
    )
    parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-c', '--cluster',
        dest='cluster',
        help="specify a cluster. If no empty, the cluster from /etc/paasta is used",
    )
    args = parser.parse_args()
    return args


def make_app(global_config=None):
    paasta_api_path = os.path.dirname(paasta_tools.api.__file__)
    setup_paasta_api()

    config = Configurator(settings={
        'service_name': 'paasta-api',
        'pyramid_swagger.schema_directory': os.path.join(paasta_api_path, 'api_docs'),
        'pyramid_swagger.skip_validation': ['/(static)\\b', '/(status)\\b', '/(swagger.json)\\b'],
        'pyramid_swagger.swagger_versions': ['2.0'],
    })

    config.include('pyramid_swagger')
    config.add_route('resources.utilization', '/v1/resources/utilization')
    config.add_route('service.instance.status', '/v1/services/{service}/{instance}/status')
    config.add_route('service.instance.delay', '/v1/services/{service}/{instance}/delay')
    config.add_route('service.instance.tasks', '/v1/services/{service}/{instance}/tasks')
    config.add_route('service.instance.tasks.task', '/v1/services/{service}/{instance}/tasks/{task_id}')
    config.add_route('service.list', '/v1/services/{service}')
    config.add_route('services', '/v1/services')
    config.add_route('service.autoscaler.get', '/v1/services/{service}/{instance}/autoscaler', request_method="GET")
    config.add_route('service.autoscaler.post', '/v1/services/{service}/{instance}/autoscaler', request_method="POST")
    config.add_route('service_autoscaler.pause.post', '/v1/service_autoscaler/pause', request_method="POST")
    config.add_route('service_autoscaler.pause.delete', '/v1/service_autoscaler/pause', request_method="DELETE")
    config.add_route('service_autoscaler.pause.get', '/v1/service_autoscaler/pause', request_method="GET")
    config.add_route('version', '/v1/version')
    config.add_route('marathon_dashboard', '/v1/marathon_dashboard', request_method="GET")
    config.add_route('metastatus', '/v1/metastatus')
    config.scan()
    return CORS(config.make_wsgi_app(), headers="*", methods="*", maxage="180", origin="*")


_app = None


def application(env, start_response):
    """For uwsgi or gunicorn."""
    global _app
    if not _app:
        _app = make_app()
        manhole_path = os.environ.get("PAASTA_MANHOLE_PATH")
        if manhole_path:
            manhole.install(socket_path=f'{manhole_path}-{os.getpid()}', locals={'_app': _app})
    return _app(env, start_response)


def setup_paasta_api():
    if os.environ.get("PAASTA_API_DEBUG"):
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    # pyinotify is a better solution than turning off file caching completely
    service_configuration_lib.disable_yaml_cache()

    settings.system_paasta_config = load_system_paasta_config()
    if os.environ.get("PAASTA_API_CLUSTER"):
        settings.cluster = os.environ.get("PAASTA_API_CLUSTER")
    else:
        settings.cluster = settings.system_paasta_config.get_cluster()

    settings.marathon_clients = marathon_tools.get_marathon_clients(
        marathon_tools.get_marathon_servers(settings.system_paasta_config),
    )

    settings.marathon_servers = marathon_tools.get_marathon_servers(system_paasta_config=settings.system_paasta_config)
    settings.marathon_clients = marathon_tools.get_marathon_clients(
        marathon_servers=settings.marathon_servers,
        cached=False,
    )

    try:
        settings.kubernetes_client = kubernetes_tools.KubeClient()
    except FileNotFoundError:
        log.info('Kubernetes not found')
        settings.kubernetes_client = None
    except Exception:
        log.exception('Error while initializing KubeClient')
        settings.kubernetes_client = None

    # Set up transparent cache for http API calls. With expire_after, responses
    # are removed only when the same request is made. Expired storage is not a
    # concern here. Thus remove_expired_responses is not needed.
    requests_cache.install_cache("paasta-api", backend="memory", expire_after=5)


def main(argv=None):
    args = parse_paasta_api_args()

    if args.debug:
        os.environ["PAASTA_API_DEBUG"] = "1"

    if args.soa_dir:
        os.environ["PAASTA_API_SOA_DIR"] = args.soa_dir

    if args.cluster:
        os.environ["PAASTA_API_CLUSTER"] = args.cluster

    os.execlp(
        os.path.join(sys.exec_prefix, "bin", "gunicorn"),
        "gunicorn",
        "-w", "4",
        "--bind", f":{args.port}",
        "paasta_tools.api.api:application",
    )


if __name__ == '__main__':
    main()
