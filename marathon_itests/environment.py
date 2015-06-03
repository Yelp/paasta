import sys
import threading
import time

import requests
from fig.cli import command


def no_marathon():
    print 'Failed to connect to marathon, canceling integration tests'
    sys.exit(1)


def before_all(context):
    """Waits for marathon to start. Maximum 30 seconds"""
    cmd = command.Command()
    project = cmd.get_project(cmd.get_config_path())
    service  = project.get_service('marathon')
    #get_local_port returns '0.0.0.0:<portnumber>'
    marathon_port = service.get_container().get_local_port(8080).split(':')[1]
    reqtimer = threading.Timer(30, no_marathon)
    reqtimer.start()
    while True:
        try:
            print 'Connecting on 127.0.0.1:%s' % marathon_port
            response = requests.get('http://127.0.0.1:%s/ping' % marathon_port, timeout=5)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout
        ):
            time.sleep(5)
            continue
        if response.status_code == 200:
            print "Marathon is up and running!"
            reqtimer.cancel()
            break


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
