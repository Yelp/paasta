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
from paasta_tools.generate_deployments_for_service import get_latest_deployment_tag
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url


def get_currently_deployed_sha(service, deploy_group, soa_dir=DEFAULT_SOA_DIR):
    git_url = get_git_url(
        service=service,
        soa_dir=soa_dir,
    )
    remote_refs = list_remote_refs(git_url)
    _, old_git_sha = get_latest_deployment_tag(remote_refs, deploy_group)
    return old_git_sha
