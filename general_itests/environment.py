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
import shutil

from behave_pytest.hook import install_pytest_asserts

from paasta_tools.utils import get_docker_client


def before_all(context):
    install_pytest_asserts()


def after_scenario(context, scenario):
    if getattr(context, "tmpdir", None):
        shutil.rmtree(context.tmpdir)
    if getattr(context, "running_container_id", None):
        docker_client = get_docker_client()
        docker_client.stop(container=context.running_container_id)
        docker_client.remove_container(container=context.running_container_id)
    if getattr(context, "fake_http_server", None):
        context.fake_http_server.shutdown()
        context.fake_http_server = None
