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
import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Tuple

from kubernetes.client import V1ConfigMap
from pyramid.response import Response
from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.kubernetes_tools import AUTOSCALING_OVERRIDES_CONFIGMAP_NAME
from paasta_tools.kubernetes_tools import AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE
from paasta_tools.kubernetes_tools import get_or_create_namespaced_configmap
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import patch_namespaced_configmap


log = logging.getLogger(__name__)


@view_config(route_name="service.autoscaler.get", request_method="GET", renderer="json")
def get_autoscaler_count(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    cluster = settings.cluster
    soa_dir = settings.soa_dir

    instance_config = get_instance_config(service, instance, cluster, soa_dir)
    if not isinstance(instance_config, (KubernetesDeploymentConfig)):
        error_message = (
            f"Autoscaling is not supported for {service}.{instance} because instance type is not "
            f"kubernetes."
        )
        raise ApiFailure(error_message, 501)

    response_body = {
        "desired_instances": instance_config.get_instances(),
        "calculated_instances": instance_config.get_instances(with_limit=False),
    }
    return Response(json_body=response_body, status_code=200)


@view_config(
    route_name="service.autoscaler.post", request_method="POST", renderer="json"
)
def update_autoscaler_count(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    cluster = settings.cluster
    soa_dir = settings.soa_dir
    desired_instances = request.swagger_data.get("json_body")["desired_instances"]
    if not isinstance(desired_instances, int):
        error_message = 'The provided body does not have an integer value for "desired_instances": {}'.format(
            request.swagger_data.get("json_body")
        )
        raise ApiFailure(error_message, 500)

    instance_config = get_instance_config(service, instance, cluster, soa_dir, True)
    if not isinstance(instance_config, (KubernetesDeploymentConfig)):
        error_message = (
            f"Autoscaling is not supported for {service}.{instance} because instance type is not "
            f"kubernetes."
        )
        raise ApiFailure(error_message, 501)

    max_instances = instance_config.get_max_instances()
    if max_instances is None:
        error_message = f"Autoscaling is not enabled for {service}.{instance}"
        raise ApiFailure(error_message, 404)
    min_instances = instance_config.get_min_instances()

    status = "SUCCESS"
    if desired_instances > max_instances:
        desired_instances = max_instances
        status = (
            "WARNING desired_instances is greater than max_instances %d" % max_instances
        )
    elif desired_instances < min_instances:
        desired_instances = min_instances
        status = (
            "WARNING desired_instances is less than min_instances %d" % min_instances
        )
    try:
        if isinstance(instance_config, KubernetesDeploymentConfig):
            instance_config.set_autoscaled_instances(
                instance_count=desired_instances, kube_client=settings.kubernetes_client
            )
        else:
            instance_config.set_autoscaled_instances(instance_count=desired_instances)
    except Exception as err:
        raise ApiFailure(err, 500)

    response_body = {"desired_instances": desired_instances, "status": status}
    return Response(json_body=response_body, status_code=202)


def get_or_create_autoscaling_overrides_configmap() -> Tuple[V1ConfigMap, bool]:
    return get_or_create_namespaced_configmap(
        AUTOSCALING_OVERRIDES_CONFIGMAP_NAME,
        namespace=AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE,
        kube_client=settings.kubernetes_client,
    )


@view_config(
    route_name="service_autoscaler.autoscaling_override.post",
    request_method="POST",
    renderer="json",
)
def set_autoscaling_override(request):
    """Set a temporary autoscaling override for a service/instance.

    This endpoint creates or updates a ConfigMap entry with override information
    including expiration time. The override will be applied by the autoscaler.

    Required parameters:
    - service: The service name
    - instance: The instance name
    - min_instances AND/OR max_instances: The minimum and/or maximum number of instances to enforce
    - expires_after: unix timestamp after which the override is no longer valid
    """
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    cluster = settings.cluster
    soa_dir = settings.soa_dir

    instance_config = get_instance_config(
        service, instance, cluster, soa_dir, load_deployments=False
    )
    if not isinstance(instance_config, KubernetesDeploymentConfig):
        error_message = (
            f"Autoscaling is not supported for {service}.{instance} because instance type is not "
            f"kubernetes."
        )
        raise ApiFailure(error_message, 501)

    json_body = request.swagger_data.get("json_body", {})
    min_instances_override = json_body.get("min_instances")
    max_instances_override = json_body.get("max_instances")
    expire_after = json_body["expire_after"]

    # ideally we'd enforce this in our api spec - but since we're using both swagger and oapi, that's not quite so simple
    if not min_instances_override and not max_instances_override:
        raise ApiFailure(
            "At least one of min_instances or max_instances must be provided", 400
        )

    # we can't currently enforce this in the API spec since the generator seems to have issues generating a validator
    # for a nullable integer field with a minimum value as we get errors like:
    # `TypeError: '<' not supported between instances of 'NoneType' and 'int'`
    # that said, unless someone is manually crafting requests, this should never happen
    # since the CLI includes a check for this as well
    if min_instances_override is not None and min_instances_override < 1:
        raise ApiFailure("min_instances must be a positive integer", 400)
    if max_instances_override is not None and max_instances_override < 1:
        raise ApiFailure("max_instances must be a positive integer", 400)

    # otherwise, we could have folks accidentally enable autoscaling (using cpu) for a non-autoscaled
    if instance_config.is_autoscaling_enabled() is None:
        raise ApiFailure(f"Autoscaling is not enabled for {service}.{instance}", 400)

    configmap, created = get_or_create_autoscaling_overrides_configmap()
    if created:
        log.info("Created new autoscaling overrides ConfigMap")
    # i dunno why this is necessary, but a newly created configmap doesn't have a data field
    # even when we set it in the create call
    if not configmap.data:
        configmap.data = {}

    service_instance = f"{service}.{instance}"
    existing_overrides = (
        json.loads(configmap.data[service_instance])
        if service_instance in configmap.data
        else {}
    )

    # this is slightly funky as there's a hierarchy of what value to pick:
    # 1. the override value provided in the request - as this should override any existing value
    # 2. the existing override value in the configmap - this is used if we previously only set an override for min or
    #    max instances and now want to set a value for the other
    # 3. the value from the instance config - this is used as a final fallback since this means no override is present
    min_instances = (
        min_instances_override
        or existing_overrides.get("min_instances")
        or instance_config.get_min_instances()
    )
    max_instances = (
        max_instances_override
        or existing_overrides.get("max_instances")
        or instance_config.get_max_instances()
    )

    # NOTE: the max_instances check is unnecessary here, but type-checkers can't see that the is_autoscaling_enabled()
    # check above ensures that max_instances is not None
    if max_instances is None or max_instances < min_instances:
        raise ApiFailure(
            f"min_instances ({min_instances_override}) cannot be greater than max_instances ({max_instances})",
            400,
        )

    override_data = {
        "min_instances": min_instances_override,
        "max_instances": max_instances_override,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expire_after": expire_after,
    }

    # NOTE: we need to strip out null values from the incoming overrides since otherwise we'd remove existing overrides
    # (since if only --set-min or --set-max is provided, the other will be sent as None)
    merged_overrides = {
        **existing_overrides,
        **{k: v for k, v in override_data.items() if v is not None},
    }
    serialized_overrides = json.dumps(merged_overrides)

    # note: we can only append with patching - if we ever want to remove overrides outside of the cleanup cronjob,
    # we would need to change this to use replace_namespaced_configmap
    patch_namespaced_configmap(
        name=AUTOSCALING_OVERRIDES_CONFIGMAP_NAME,
        namespace=AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE,
        # this should only update the single entry for the $service.$instance key
        # ain't k8s grand?
        body={"data": {service_instance: serialized_overrides}},
        kube_client=settings.kubernetes_client,
    )

    response_body = {
        "service": service,
        "instance": instance,
        "cluster": cluster,
        "min_instances": min_instances_override,
        "max_instances": max_instances_override,
        "expire_after": expire_after,
        "status": "SUCCESS",
    }
    # NOTE: this is an HTTP 202 since actually updating the HPA happens asynchronously
    # through setup_kubernetes_job
    # XXX: should we try to patch things here as well?
    return Response(json_body=response_body, status_code=202)
