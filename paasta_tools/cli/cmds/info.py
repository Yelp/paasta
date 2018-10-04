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
from typing import Collection
from typing import List

from service_configuration_lib import read_service_configuration

from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.marathon_tools import get_all_namespaces_for_service
from paasta_tools.marathon_tools import load_service_namespace_config
from paasta_tools.monitoring_tools import get_runbook
from paasta_tools.monitoring_tools import get_team
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_services
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors

NO_DESCRIPTION_MESSAGE = (
    "No 'description' entry in service.yaml. Please a one line sentence that describes this service"
)
NO_EXTERNAL_LINK_MESSAGE = (
    "No 'external_link' entry in service.yaml. "
    "Please add one that points to a reference doc for your service"
)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'info',
        help="Prints the general information about a service.",
        description=(
            "'paasta info' gathers information about a service from soa-configs "
            "and prints it in a human-friendly way. It does no API calls, it "
            "just analyzes the config files."
        ),
    )
    list_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_info)


def deployments_to_clusters(deployments: Collection[str]) -> Collection[str]:
    clusters = []
    for deployment in deployments:
        cluster, _ = deployment.split('.')
        clusters.append(cluster)
    return set(clusters)


def get_smartstack_endpoints(service, soa_dir):
    endpoints = []
    for name, config in get_all_namespaces_for_service(service, full_name=False, soa_dir=soa_dir):
        mode = config.get('mode', 'http')
        port = config.get('proxy_port')
        endpoints.append("{}://169.254.255.254:{} ({})".format(
            mode, port, name,
        ))
    return endpoints


def get_deployments_strings(service: str, soa_dir: str) -> List[str]:
    output = []
    try:
        deployments = get_actual_deployments(service, soa_dir)
    except NoDeploymentsAvailable:
        deployments = {}
    if deployments == {}:
        output.append(' - N/A: Not deployed to any PaaSTA Clusters')
    else:
        service_config = load_service_namespace_config(
            service=service, namespace='main', soa_dir=soa_dir,
        )
        service_mode = service_config.get_mode()
        for cluster in deployments_to_clusters(deployments):
            if service_mode == "tcp":
                service_port = service_config.get('proxy_port')
                link = PaastaColors.cyan('%s://paasta-%s.yelp:%d/' % (service_mode, cluster, service_port))
            elif service_mode == "http" or service_mode == "https":
                link = PaastaColors.cyan(f'{service_mode}://{service}.paasta-{cluster}.yelp/')
            else:
                link = "N/A"
            output.append(f' - {cluster} ({link})')
    return output


def get_dashboard_urls(service):
    output = [' - %s (Sensu Alerts)' % (PaastaColors.cyan('https://uchiwa.yelpcorp.com/#/events?q=%s' % service))]
    return output


def get_service_info(service, soa_dir):
    service_configuration = read_service_configuration(service, soa_dir)
    description = service_configuration.get('description', NO_DESCRIPTION_MESSAGE)
    external_link = service_configuration.get('external_link', NO_EXTERNAL_LINK_MESSAGE)
    smartstack_endpoints = get_smartstack_endpoints(service, soa_dir)
    git_url = get_git_url(service, soa_dir)

    output = []
    output.append('Service Name: %s' % service)
    output.append('Description: %s' % description)
    output.append('External Link: %s' % PaastaColors.cyan(external_link))
    output.append('Monitored By: team %s' % get_team(service=service, overrides={}, soa_dir=soa_dir))
    output.append('Runbook: %s' % PaastaColors.cyan(get_runbook(service=service, overrides={}, soa_dir=soa_dir)))
    output.append('Git Repo: %s' % git_url)
    output.append('Deployed to the following clusters:')
    output.extend(get_deployments_strings(service, soa_dir))
    if smartstack_endpoints:
        output.append('Smartstack endpoint(s):')
        for endpoint in smartstack_endpoints:
            output.append(' - %s' % endpoint)
    output.append('Dashboard(s):')
    output.extend(get_dashboard_urls(service))

    return '\n'.join(output)


def paasta_info(args):
    """Prints general information about a service"""
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir=soa_dir)
    paasta_print(get_service_info(service, soa_dir))
