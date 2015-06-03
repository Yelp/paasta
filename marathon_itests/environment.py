import time

from itest_utils import wait_for_marathon


def before_all(context):
    wait_for_marathon()


def after_scenario(context, scenario):
    """If a marathon client object exists in our context, delete any apps in Marathon and wait until they die."""
    if context.client:
        while True:
            apps = context.client.list_apps()
            if not apps:
                break
            for app in apps:
                context.client.delete_app(app.id, force=True)
            time.sleep(0.5)
        while context.client.list_deployments():
            time.sleep(0.5)
