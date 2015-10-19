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

from paasta_tools.paasta_cli.utils import _run
from paasta_tools.utils import compose_job_id


def get_serviceinit_status(service, namespace):
    command = "paasta_serviceinit -v %s status" % compose_job_id(service, namespace)
    return _run(command, timeout=10)[1]


def get_context(service, namespace):
    """Tries to get more context about why a service might not be OK.
    returns a string useful for inserting into monitoring email alerts."""
    status = get_serviceinit_status(service, namespace)
    context = "\nMore context from paasta status:\n%s" % status
    return context
