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
import asyncio
import os
import shutil
import time

import a_sync
import mock
import requests
from itest_utils import cleanup_file
from itest_utils import clear_mesos_tools_cache
from itest_utils import get_service_connection_string
from itest_utils import setup_mesos_cli_config
from itest_utils import wait_for_marathon
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools.mesos_maintenance import load_credentials
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.utils import paasta_print


def before_all(context):
    context.cluster = "testcluster"
    context.mesos_cli_config = os.path.join(os.getcwd(), "mesos-cli.json")
    wait_for_marathon()
    setup_mesos_cli_config(context.mesos_cli_config, context.cluster)


def after_all(context):
    cleanup_file(context.mesos_cli_config)


def _stop_deployd(context):
    if hasattr(context, "daemon"):
        paasta_print("Stopping deployd...")
        try:
            context.daemon.terminate()
            context.daemon.wait()
        except OSError:
            pass


def _clean_up_marathon_apps(context):
    """If a marathon client object exists in our context, delete any apps in Marathon and wait until they die."""
    if hasattr(context, "marathon_clients"):
        still_apps = True
        while still_apps:
            still_apps = False
            for client in context.marathon_clients.get_all_clients():
                apps = marathon_tools.list_all_marathon_app_ids(client)
                if apps:
                    still_apps = True
                else:
                    continue
                paasta_print(
                    "after_scenario: Deleting %d apps to prep for the next scenario. %s"
                    % (len(apps), ",".join(apps))
                )
                for app in apps:
                    if marathon_tools.is_app_id_running(app, client):
                        paasta_print(
                            "after_scenario: %s does look like it is running. Scaling down and killing it..."
                            % app
                        )
                        client.scale_app(app, instances=0, force=True)
                        time.sleep(1)
                        client.delete_app(app, force=True)
                    else:
                        paasta_print(
                            "after_scenario: %s showed up in the app_list, but doesn't look like it is running?"
                            % app
                        )
            time.sleep(0.5)
        for client in context.marathon_clients.get_all_clients():
            while client.list_deployments():
                paasta_print(
                    "after_scenario: There are still marathon deployments in progress. sleeping."
                )
                time.sleep(0.5)


def _clean_up_chronos_jobs(context):
    """ If a chronos client object exists, delete any jobs and wait for them to die """
    if hasattr(context, "chronos_client"):
        while len(context.chronos_client.list()) > 0:
            jobs = context.chronos_client.list()
            for job in jobs:
                paasta_print(
                    "after_scenario: Job %s is present in chronos. Deleting."
                    % job["name"]
                )
                context.chronos_client.delete(job["name"])
            time.sleep(1)
    if hasattr(context, "jobs"):
        context.jobs = {}


def _clean_up_mesos_cli_config(context):
    """If a mesos cli config file was written, clean it up."""
    if hasattr(context, "mesos_cli_config_filename"):
        paasta_print("Cleaning up %s" % context.mesos_cli_config_filename)
        os.unlink(context.mesos_cli_config_filename)
        del context.mesos_cli_config_filename


def _clean_up_soa_dir(context):
    """If a yelpsoa-configs directory was written, clean it up."""
    if hasattr(context, "soa_dir"):
        paasta_print("Cleaning up %s" % context.soa_dir)
        shutil.rmtree(context.soa_dir)
        del context.soa_dir


def _clean_up_etc_paasta(context):
    if hasattr(context, "etc_paasta"):
        paasta_print("Cleaning up %s" % context.etc_paasta)
        shutil.rmtree(context.etc_paasta)
        del context.etc_paasta


def _clean_up_zookeeper_autoscaling(context):
    """If max_instances was set for autoscaling, clean up zookeeper"""
    if "max_instances" in context:
        client = KazooClient(
            hosts="%s/mesos-testcluster" % get_service_connection_string("zookeeper"),
            read_only=True,
        )
        client.start()
        try:
            client.delete("/autoscaling", recursive=True)
        except NoNodeError:
            pass
        client.stop()
        client.close()


def _clean_up_paasta_native_frameworks(context):
    clear_mesos_tools_cache()
    # context.etc_paasta signals that we actually have configured the mesos-cli.json; without this, we don't know where
    # to connect to clean up paasta native frameworks.
    if hasattr(context, "etc_paasta"):
        for framework in a_sync.block(
            mesos_tools.get_mesos_master().frameworks, active_only=True
        ):
            if framework.name.startswith("paasta_native ") or framework.name == getattr(
                context, "framework_name", ""
            ):
                paasta_print("cleaning up framework %s" % framework.name)
                try:
                    mesos_tools.terminate_framework(framework.id)
                except requests.exceptions.HTTPError as e:
                    paasta_print(
                        f"Got exception when terminating framework {framework.id}: {e}"
                    )


def _clean_up_maintenance(context):
    """If a host is marked as draining/down for maintenance, bring it back up"""
    if hasattr(context, "at_risk_host"):
        with mock.patch(
            "paasta_tools.mesos_maintenance.get_principal", autospec=True
        ) as mock_get_principal, mock.patch(
            "paasta_tools.mesos_maintenance.get_secret", autospec=True
        ) as mock_get_secret:
            credentials = load_credentials(mesos_secrets="/etc/mesos-slave-secret")
            mock_get_principal.return_value = credentials.principal
            mock_get_secret.return_value = credentials.secret
            undrain([context.at_risk_host])
            del context.at_risk_host


def _clean_up_current_client(context):
    if hasattr(context, "current_client"):
        del context.current_client


def _clean_up_event_loop(context):
    if hasattr(context, "event_loop"):
        del context.event_loop


def after_scenario(context, scenario):
    _stop_deployd(context)
    _clean_up_marathon_apps(context)
    _clean_up_chronos_jobs(context)
    _clean_up_maintenance(context)
    _clean_up_mesos_cli_config(context)
    _clean_up_soa_dir(context)
    _clean_up_zookeeper_autoscaling(context)
    _clean_up_maintenance(context)
    _clean_up_paasta_native_frameworks(
        context
    )  # this must come before _clean_up_etc_paasta
    _clean_up_etc_paasta(context)
    _clean_up_current_client(context)
    _clean_up_event_loop(context)


def before_feature(context, feature):
    if "skip" in feature.tags:
        feature.skip("Marked with @skip")
        return


def before_scenario(context, scenario):
    context.event_loop = asyncio.get_event_loop()
    if "skip" in scenario.effective_tags:
        scenario.skip("Marked with @skip")
        return
