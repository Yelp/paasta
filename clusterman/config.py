# Copyright 2019 Yelp Inc.
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
import argparse
import os
from typing import Optional

import staticconf

CREDENTIALS_NAMESPACE = 'boto_cfg'
DEFAULT_CLUSTER_DIRECTORY = '/nail/srv/configs/clusterman-clusters'
LOG_STREAM_NAME = 'tmp_clusterman_autoscaler'
POOL_NAMESPACE = '{pool}.{scheduler}_config'


def _load_module_configs(env_config_path: str):
    staticconf.YamlConfiguration(env_config_path)
    for config in staticconf.read_list('module_config', default=[]):
        if 'file' in config:
            staticconf.YamlConfiguration(config['file'], namespace=config['namespace'])
        staticconf.DictConfiguration(config.get('config', {}), namespace=config['namespace'])
        if 'initialize' in config:
            path = config['initialize'].split('.')
            function = path.pop()
            module_name = '.'.join(path)
            module = __import__(module_name, globals(), locals(), [path[-1]])
            getattr(module, function)()


def setup_config(args: argparse.Namespace) -> None:
    # load_default_config merges the 'module_config' key from the first file
    # and the 'module_env_config' key from the second file to configure packages.
    # This allows us to configure packages differently in different hiera envs by
    # changing 'module_env_config'. We use the same file for both keys.
    _load_module_configs(args.env_config_path)

    signals_branch_or_tag = getattr(args, 'signals_branch_or_tag', None)
    cluster_config_directory = getattr(args, 'cluster_config_directory', None) or DEFAULT_CLUSTER_DIRECTORY
    staticconf.DictConfiguration({'cluster_config_directory': cluster_config_directory})

    aws_region = getattr(args, 'aws_region', None)
    cluster = getattr(args, 'cluster', None)
    pool = getattr(args, 'pool', None)
    scheduler = getattr(args, 'scheduler', None)
    if aws_region and cluster:
        raise argparse.ArgumentError(None, 'Cannot specify both cluster and aws_region')

    # If there is a cluster specified via --cluster, load cluster-specific attributes
    # into staticconf.  These values are not specified using hiera in srv-configs because
    # we might want to be operating on a cluster in one region while running from a
    # different region.
    elif cluster:
        aws_region = staticconf.read_string(f'clusters.{cluster}.aws_region', default=None)
        if pool:
            load_cluster_pool_config(cluster, pool, scheduler, signals_branch_or_tag)

    staticconf.DictConfiguration({'aws': {'region': aws_region}})

    boto_creds_file = staticconf.read_string('aws.access_key_file', default=None)
    if boto_creds_file:
        staticconf.JSONConfiguration(boto_creds_file, namespace=CREDENTIALS_NAMESPACE)

    if signals_branch_or_tag:
        staticconf.DictConfiguration({'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}})


def load_cluster_pool_config(cluster: str, pool: str, scheduler: str, signals_branch_or_tag: Optional[str]) -> None:
    pool_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=scheduler)
    pool_config_file = get_pool_config_path(cluster, pool, scheduler)

    staticconf.YamlConfiguration(pool_config_file, namespace=pool_namespace)
    if signals_branch_or_tag:
        staticconf.DictConfiguration(
            {'autoscale_signal': {'branch_or_tag': signals_branch_or_tag}},
            namespace=pool_namespace,
        )


def get_cluster_config_directory(cluster):
    return os.path.join(staticconf.read_string('cluster_config_directory'), cluster)


def get_pool_config_path(cluster: str, pool: str, scheduler: str) -> str:
    return os.path.join(get_cluster_config_directory(cluster), f'{pool}.{scheduler}')
