from __future__ import absolute_import
from __future__ import unicode_literals

import errno
import fcntl
import os
import time
from subprocess import PIPE
from subprocess import Popen

import service_configuration_lib
from behave import given
from behave import then
from itest_utils import get_service_connection_string
from kazoo.exceptions import NodeExistsError

from paasta_tools.marathon_tools import list_all_marathon_app_ids
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import ZookeeperPool


@given('paasta-deployd is running')
def start_deployd(context):
    try:
        os.makedirs('/nail/etc/services')
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
    with ZookeeperPool() as zk:
        try:
            zk.create('/autoscaling')
        except NodeExistsError:
            pass
    context.zk_hosts = '%s/mesos-testcluster' % get_service_connection_string('zookeeper')
    context.soa_dir = '/nail/etc/services'
    if not hasattr(context, 'daemon'):
        context.daemon = Popen('paasta-deployd', stderr=PIPE)
    output = context.daemon.stderr.readline().decode('utf-8')
    start = time.time()
    timeout = start + 60
    while "Startup finished!" not in output:
        output = context.daemon.stderr.readline().decode('utf-8')
        print(output.rstrip('\n'))
        time.sleep(1)
        if time.time() > timeout:
            raise "deployd never ran"
    time.sleep(5)


@then('paasta-deployd can be stopped')
def stop_deployd(context):
    context.daemon.terminate()
    context.daemon.wait()


@then('a second deployd does not become leader')
def start_second_deployd(context):
    context.daemon1 = Popen('paasta-deployd', stderr=PIPE)
    output = context.daemon1.stderr.readline().decode('utf-8')
    fd = context.daemon1.stderr
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    for i in range(0, 5):
        try:
            output = context.daemon1.stderr.readline().decode('utf-8')
            print(output.rstrip('\n'))
            assert 'This node is elected as leader' not in output
        except IOError:
            pass
        time.sleep(1)


@then('a second deployd becomes leader')
def second_deployd_is_leader(context):
    try:
        output = context.daemon1.stderr.readline().decode('utf-8')
    except IOError:
        output = ''
    start = time.time()
    timeout = start + 60
    while "This node is elected as leader" not in output:
        try:
            output = context.daemon1.stderr.readline().decode('utf-8')
        except IOError:
            output = ''
        if output:
            print(output.rstrip('\n'))
        if time.time() > timeout:
            raise "Timed out waiting for second deployd leader"
        time.sleep(1)
    context.daemon1.terminate()
    context.daemon1.wait()


@then('we should see "{service_instance}" listed in marathon after {seconds:d} seconds')
def check_app_running(context, service_instance, seconds):
    service, instance, _, _ = decompose_job_id(service_instance)
    service_configuration_lib._yaml_cache = {}
    context.marathon_config = load_marathon_service_config_no_cache(service, instance, context.cluster)
    context.app_id = context.marathon_config.format_marathon_app_dict()['id']
    step = 5
    attempts = 0
    while (attempts * step) < seconds:
        if context.app_id in list_all_marathon_app_ids(context.marathon_client):
            break
        time.sleep(step)
        attempts += 1
    assert context.app_id in list_all_marathon_app_ids(context.marathon_client)
    context.old_app_id = context.app_id


@then('we should not see the old version listed in marathon after {seconds:d} seconds')
def check_app_not_running(context, seconds):
    step = 5
    attempts = 0
    while (attempts * step) < seconds:
        if context.old_app_id not in list_all_marathon_app_ids(context.marathon_client):
            return
        time.sleep(step)
        attempts += 1
    assert context.old_app_id not in list_all_marathon_app_ids(context.marathon_client)


@then('we set a new command for our service instance to {cmd}')
def set_cmd(context, cmd):
    context.cmd = cmd


@then('the appid for "{service_instance}" should have changed')
def check_sha_changed(context, service_instance):
    service, instance, _, _ = decompose_job_id(service_instance)
    service_configuration_lib._yaml_cache = {}
    context.marathon_config = load_marathon_service_config_no_cache(service, instance, context.cluster)
    assert context.app_id != context.marathon_config.format_marathon_app_dict()['id']
