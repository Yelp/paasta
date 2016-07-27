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
from behave import then
from pyramid import testing

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.api.views.instance import instance_status
from paasta_tools.api.views.instance import InstanceFailure
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config


@then(u'instance GET should return app_count "{app_count}" and an expected number of running instances for "{job_id}"')
def service_instance_status(context, app_count, job_id):
    marathon_config = marathon_tools.load_marathon_config()
    settings.marathon_client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )
    settings.cluster = load_system_paasta_config().get_cluster()
    settings.soa_dir = context.soa_dir

    (service, instance, _, __) = decompose_job_id(job_id)

    request = testing.DummyRequest()
    request.matchdict = {'service': service, 'instance': instance}
    response = instance_status(request)

    assert response['app_count'] == int(app_count), response
    assert response['marathon']['running_instance_count'] == response['marathon']['expected_instance_count'], response


@then(u'instance GET should return error code "{error_code}" for "{job_id}"')
def service_instance_status_error(context, error_code, job_id):
    marathon_config = marathon_tools.load_marathon_config()
    settings.marathon_client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )
    settings.cluster = load_system_paasta_config().get_cluster()
    settings.soa_dir = context.soa_dir

    (service, instance, _, __) = decompose_job_id(job_id)

    request = testing.DummyRequest()
    request.matchdict = {'service': service, 'instance': instance}

    response = None
    try:
        response = instance_status(request)
    except InstanceFailure as exc:
        print exc.msg
        assert exc.err == int(error_code)
    except:
        raise

    assert not response
