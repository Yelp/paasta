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
Settings of the paasta-api server.
"""
import os
from typing import Optional

from paasta_tools import utils
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import SystemPaastaConfig

soa_dir: str = os.environ.get("PAASTA_API_SOA_DIR", DEFAULT_SOA_DIR)

# The following `type: ignore` mypy hints are there because variables below de
# juro have `Optional[T]` type, but de facto are always initialized to a value
# of the corresponding type after the application is started.
cluster: str = None  # type: ignore
hostname: str = utils.get_hostname()
marathon_clients: MarathonClients = None  # type: ignore
kubernetes_client: Optional[KubeClient] = None
system_paasta_config: Optional[SystemPaastaConfig]
