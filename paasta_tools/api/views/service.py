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
PaaSTA service list (instances) etc.
"""
from pyramid.response import Response
from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import get_service_docker_registry
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import load_v2_deployments_json


@view_config(route_name="service.list", request_method="GET", renderer="json")
def list_instances(request):
    service = request.swagger_data.get("service")
    instances = list_all_instances_for_service(service, clusters=[settings.cluster])
    return {"instances": list(instances)}


@view_config(route_name="services", request_method="GET", renderer="json")
def list_services_for_cluster(request):
    services_for_cluster = get_services_for_cluster(cluster=settings.cluster)
    return {"services": services_for_cluster}


@view_config(route_name="service.deployments", request_method="GET", renderer="json")
def get_deployment_info(request):
    """Get deployment information for a service and deploy_group."""
    service = request.swagger_data.get("service")
    deploy_group = request.swagger_data.get("deploy_group")
    soa_dir = settings.soa_dir

    try:
        deployments = load_v2_deployments_json(service=service, soa_dir=soa_dir)

        try:
            deployment_dict = deployments.config_dict["deployments"][deploy_group]
        except KeyError:
            e = f"{service} not deployed to {deploy_group}"
            raise NoDeploymentsAvailable(e)

        registry_uri = get_service_docker_registry(service=service, soa_dir=soa_dir)
        docker_image = deployment_dict["docker_image"]
        image_url = f"{registry_uri}/{docker_image}"

        response_body = {
            "docker_image": deployment_dict["docker_image"],
            "git_sha": deployment_dict["git_sha"],
            # not all services are opted into no-commit redeploys
            # (at least, at the time this comment was written :p)
            "image_version": deployment_dict.get("image_version"),
            "image_url": image_url,
        }
        return Response(json_body=response_body, status_code=200)

    except NoDeploymentsAvailable as e:
        raise ApiFailure(str(e), 404)

    except KeyError as e:
        raise ApiFailure(str(e), 500)
