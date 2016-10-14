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
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoDeploymentsAvailable


def get_currently_deployed_sha(service, deploy_group, soa_dir=DEFAULT_SOA_DIR):
    """Tries to determine the currently deployed sha for a service and deploy_group,
    returns None if there isn't one ready yet"""
    try:
        deployments = load_v2_deployments_json(service=service, soa_dir=soa_dir)
        return deployments.get_git_sha_for_deploy_group(deploy_group=deploy_group)
    except NoDeploymentsAvailable:
        return None
