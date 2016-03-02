#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
from kazoo.client import KazooClient
from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.utils import load_system_paasta_config

_autoscaling_methods = {}


def register_autoscaling_method(name):
    def outer(autoscaling_method):
        _autoscaling_methods[name] = autoscaling_method
        return autoscaling_method
    return outer


def get_autoscaling_method(function_name):
    return _autoscaling_methods[function_name]


def compose_autoscaling_zookeeper_root(service, instance):
    return '/autoscaling/%s/%s' % (service, instance)


def set_instances_for_marathon_service(service, instance, instance_count, soa_dir=DEFAULT_SOA_DIR):
    zookeeper_path = '%s/instances' % compose_autoscaling_zookeeper_root(service, instance)
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count))


def get_instances_from_zookeeper(service, instance):
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get('%s/instances' % compose_autoscaling_zookeeper_root(service, instance))
        return int(instances)


@register_autoscaling_method('bespoke')
def bespoke_autoscaling_method(*args, **kwargs):
    # do nothing, the service author has written their own scaling code
    return 0


@register_autoscaling_method('default')
def default_autoscaling_method(marathon_service_config):
    # not implemented yet
    return 0


def autoscale_marathon_instance(marathon_service_config):
    if marathon_service_config.get_max_instances() is None:
        return
    autoscaling_params = marathon_service_config.get_autoscaling_params()
    with ZookeeperPool():
        autoscale_amount = get_autoscaling_method(autoscaling_params['method'])(marathon_service_config)
        if autoscale_amount:
            current_instances = marathon_service_config.get_instances()
            instances = min(
                marathon_service_config.get_max_instances(),
                max(marathon_service_config.get_min_instances(),
                    current_instances + autoscale_amount),
            )
            if instances != current_instances:
                set_instances_for_marathon_service(
                    service=marathon_service_config.service,
                    instance=marathon_service_config.instance,
                    instance_count=instances,
                )


class ZookeeperPool(object):
    """
    A context manager that shares the same KazooClient with its children. The first nested contest manager
    creates and deletes the client and shares it with any of its children. This allows to place a context
    manager over a large number of zookeeper calls without opening and closing a connection each time.
    GIL makes this 'safe'.
    """
    counter = 0
    zk = None

    @classmethod
    def __enter__(cls):
        if cls.zk is None:
            cls.zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), read_only=True)
            cls.zk.start()
        cls.counter = cls.counter + 1
        return cls.zk

    @classmethod
    def __exit__(cls, *args, **kwargs):
        cls.counter = cls.counter - 1
        if cls.counter == 0:
            cls.zk.stop()
            cls.zk.close()
            cls.zk = None
