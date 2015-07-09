import os
import time

from itest_utils import wait_for_marathon
from itest_utils import cleanup_file
from itest_utils import setup_mesos_cli_config
from itest_utils import print_container_logs
from paasta_tools import marathon_tools


def before_all(context):
    context.cluster = "testcluster"
    context.mesos_cli_config = os.path.join(os.getcwd(), 'mesos-cli.json')
    wait_for_marathon()
    setup_mesos_cli_config(context.mesos_cli_config, context.cluster)


def after_all(context):
    cleanup_file(context.mesos_cli_config)


def after_scenario(context, scenario):
    """If a marathon client object exists in our context, delete any apps in Marathon and wait until they die."""
    if hasattr(context, 'client'):
        while True:
            apps = marathon_tools.list_all_marathon_app_ids(context.client)
            if not apps:
                break
            print "after_scenario: Deleting %d apps to prep for the next scenario. %s" % (len(apps), ",".join(apps))
            for app in apps:
                if marathon_tools.is_app_id_running(app, context.client):
                    print "after_scenario: %s does look like it is running. Scaling down and killing it..." % app
                    context.client.scale_app(app, instances=0, force=True)
                    time.sleep(1)
                    context.client.delete_app(app, force=True)
                else:
                    print "after_scenario: %s showed up in the app_list, but doesn't look like it is running?" % app
            time.sleep(0.5)
        while context.client.list_deployments():
            print "after_scenario: There are still marathon deployments in progress. sleeping."
            time.sleep(0.5)


def after_step(context, step):
    if step.status == "failed":
        print "Zookeeper container logs:"
        print_container_logs('zookeeper')
        print "Marathon container logs:"
        print_container_logs('marathon')
