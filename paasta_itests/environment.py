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
import os
import shutil
import time

from itest_utils import cleanup_file
from itest_utils import get_service_connection_string
from itest_utils import setup_mesos_cli_config
from itest_utils import wait_for_marathon
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from paasta_tools import marathon_tools


def before_all(context):
    context.cluster = "testcluster"
    context.mesos_cli_config = os.path.join(os.getcwd(), 'mesos-cli.json')
    wait_for_marathon()
    setup_mesos_cli_config(context.mesos_cli_config, context.cluster)


def after_all(context):
    cleanup_file(context.mesos_cli_config)


def _clean_up_marathon_apps(context):
    """If a marathon client object exists in our context, delete any apps in Marathon and wait until they die."""
    if hasattr(context, 'marathon_client'):
        while True:
            apps = marathon_tools.list_all_marathon_app_ids(context.marathon_client)
            if not apps:
                break
            print "after_scenario: Deleting %d apps to prep for the next scenario. %s" % (len(apps), ",".join(apps))
            for app in apps:
                if marathon_tools.is_app_id_running(app, context.marathon_client):
                    print "after_scenario: %s does look like it is running. Scaling down and killing it..." % app
                    context.marathon_client.scale_app(app, instances=0, force=True)
                    time.sleep(1)
                    context.marathon_client.delete_app(app, force=True)
                else:
                    print "after_scenario: %s showed up in the app_list, but doesn't look like it is running?" % app
            time.sleep(0.5)
        while context.marathon_client.list_deployments():
            print "after_scenario: There are still marathon deployments in progress. sleeping."
            time.sleep(0.5)


def _clean_up_chronos_jobs(context):
    """ If a chronos client object exists, delete any jobs and wait for them to die """
    if hasattr(context, 'chronos_client'):
        while len(context.chronos_client.list()) > 0:
            jobs = context.chronos_client.list()
            for job in jobs:
                print "after_scenario: Job %s is present in chronos. Deleting." % job['name']
                context.chronos_client.delete(job['name'])
            time.sleep(1)
    if hasattr(context, 'jobs'):
        context.jobs = {}


def _clean_up_mesos_cli_config(context):
    """If a mesos cli config file was written, clean it up."""
    if hasattr(context, 'mesos_cli_config_filename'):
        print 'Cleaning up %s' % context.mesos_cli_config_filename
        os.unlink(context.mesos_cli_config_filename)
        del context.mesos_cli_config_filename


def _clean_up_soa_dir(context):
    """If a yelpsoa-configs directory was written, clean it up."""
    if hasattr(context, 'soa_dir'):
        print 'Cleaning up %s' % context.soa_dir
        shutil.rmtree(context.soa_dir)
        del context.soa_dir


def _clean_up_zookeeper_autoscaling(context):
    """If max_instances was set for autoscaling, clean up zookeeper"""
    client = KazooClient(hosts='%s/mesos-testcluster' % get_service_connection_string('zookeeper'), read_only=True)
    client.start()
    try:
        client.delete('/autoscaling', recursive=True)
    except NoNodeError:
        pass
    client.stop()
    client.close()


def after_scenario(context, scenario):
    _clean_up_marathon_apps(context)
    _clean_up_chronos_jobs(context)
    _clean_up_mesos_cli_config(context)
    _clean_up_soa_dir(context)
