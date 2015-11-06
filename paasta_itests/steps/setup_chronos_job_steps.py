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

import copy
import sys

from behave import when, then

sys.path.append('../')
from paasta_tools import setup_chronos_job
from paasta_tools import chronos_tools

fake_service_name = 'test-service'
fake_instance_name = 'test-instance'
fake_job_id = 'fake_job_id'
fake_service_job_config = chronos_tools.ChronosJobConfig(
    fake_service_name,
    fake_instance_name,
    {},
    {'docker_image': 'test-image', 'desired_state': 'start'},
)

# TODO DRY out in PAASTA-1174
fake_service_config = {
    "retries": 1,
    "container": {
        "image": "localhost/fake_docker_url",
        "type": "DOCKER",
        "network": "BRIDGE",
        "volumes": [
            {'hostPath': u'/nail/etc/habitat', 'containerPath': '/nail/etc/habitat', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/datacenter', 'containerPath': '/nail/etc/datacenter', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/ecosystem', 'containerPath': '/nail/etc/ecosystem', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/rntimeenv', 'containerPath': '/nail/etc/rntimeenv', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/region', 'containerPath': '/nail/etc/region', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/sperregion', 'containerPath': '/nail/etc/sperregion', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/topology_env', 'containerPath': '/nail/etc/topology_env', 'mode': 'RO'},
            {'hostPath': u'/nail/srv', 'containerPath': '/nail/srv', 'mode': 'RO'},
            {'hostPath': u'/etc/boto_cfg', 'containerPath': '/etc/boto_cfg', 'mode': 'RO'},
        ],
    },
    "name": fake_job_id,
    "schedule": "R//PT10M",
    "mem": 128,
    "epsilon": "PT30S",
    "cpus": 0.1,
    "disabled": False,
    "command": "fake command",
    "owner": "fake_team",
    "async": True,
    "disk": 256,
}


# TODO DRY out in PAASTA-1174 and rename so it doesn't sound like the funcs in chronos_steps
@when(u'we create a complete chronos job')
def create_complete_job(context):
    return_tuple = setup_chronos_job.setup_job(
        fake_service_name,
        fake_instance_name,
        fake_service_job_config,
        fake_service_config,
        context.chronos_client,
        "fake_cluster",
    )
    assert return_tuple[0] == 0


@when(u'we run setup_chronos_job')
def setup_the_chronos_job(context):
    exit_code, output = setup_chronos_job.setup_job(
        service=fake_service_name,
        instance=fake_instance_name,
        chronos_job_config=context.chronos_job_config_obj,
        complete_job_config=context.chronos_job_config,
        client=context.chronos_client,
        cluster=context.cluster
    )
    print 'setup_chronos_job returned exitcode %s with output:\n%s\n' % (exit_code, output)


# TODO DRY out in PAASTA-1174
@then(u'we should see it in the list of jobs')
def see_it_in_list_of_jobs(context):
    jobs_with_our_name = [job for job in context.chronos_client.list() if job['name'] == fake_job_id]
    assert len(jobs_with_our_name) == 1
    assert jobs_with_our_name[0]["disabled"] is False


@when(u'{job_count} old jobs are left over from previous bounces')
def old_jobs_leftover(context, job_count):
    old_job = copy.deepcopy(fake_service_config)
    for n in xrange(0, int(job_count)):
        old_job["name"] = chronos_tools.compose_job_id(
            service=fake_service_name,
            instance=fake_instance_name,
            git_hash="git%d" % n,
            config_hash="config",
        )
        context.chronos_client.add(old_job)


@then(u'there should be {job_count} enabled jobs')
def should_be_enabled_jobs(context, job_count):
    enabled_jobs = chronos_tools.lookup_chronos_jobs(
        service=fake_service_name,
        instance=fake_instance_name,
        client=context.chronos_client,
        include_disabled=False,
    )
    assert len(enabled_jobs) == int(job_count)


@then(u'there should be {job_count} disabled jobs')
def should_be_disabled_jobs(context, job_count):
    all_related_jobs = chronos_tools.lookup_chronos_jobs(
        service=fake_service_name,
        instance=fake_instance_name,
        client=context.chronos_client,
        include_disabled=True,
    )
    disabled_jobs = [job for job in all_related_jobs if job["disabled"] is True]
    assert len(disabled_jobs) == int(job_count)
