import errno
import fcntl
import json
import os
import threading
import time
from subprocess import PIPE
from subprocess import Popen

import mock
import service_configuration_lib
from behave import given
from behave import then
from behave import when
from itest_utils import get_service_connection_string
from kazoo.exceptions import NodeExistsError
from steps.setup_steps import modify_configs

from paasta_tools.deployd.master import DEAD_DEPLOYD_WORKER_MESSAGE
from paasta_tools.marathon_tools import list_all_marathon_app_ids
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.util.config_loading import SystemPaastaConfig
from paasta_tools.util.names import decompose_job_id
from paasta_tools.util.zk import ZookeeperPool


@given("paasta-deployd is running")
def start_deployd(context):
    try:
        os.makedirs("/nail/etc/services")
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
    with ZookeeperPool() as zk:
        try:
            zk.create("/autoscaling")
        except NodeExistsError:
            pass
    context.zk_hosts = "%s/mesos-testcluster" % get_service_connection_string(
        "zookeeper"
    )
    context.soa_dir = "/nail/etc/services"
    if not hasattr(context, "daemon"):
        context.daemon = Popen("paasta-deployd", stderr=PIPE)
    output = context.daemon.stderr.readline().decode("utf-8")
    start = time.time()
    timeout = start + 60
    while "Startup finished!" not in output:
        output = context.daemon.stderr.readline().decode("utf-8")
        if not output:
            raise Exception("deployd exited prematurely")
        print(output.rstrip("\n"))
        if time.time() > timeout:
            raise Exception("deployd never ran")

    context.num_workers_crashed = 0

    def dont_let_stderr_buffer():
        while True:
            line = context.daemon.stderr.readline()
            if not line:
                return
            if DEAD_DEPLOYD_WORKER_MESSAGE.encode("utf-8") in line:
                context.num_workers_crashed += 1
            print(f"deployd stderr: {line}")

    threading.Thread(target=dont_let_stderr_buffer).start()
    time.sleep(5)


@then("no workers should have crashed")
def no_workers_should_crash(context):
    if context.num_workers_crashed > 0:
        raise Exception(
            f"Expected no workers to crash, found {context.num_workers_crashed} stderr lines matching {DEAD_DEPLOYD_WORKER_MESSAGE!r}"
        )


@then("paasta-deployd can be stopped")
def stop_deployd(context):
    context.daemon.terminate()
    context.daemon.wait()


@then("a second deployd does not become leader")
def start_second_deployd(context):
    context.daemon1 = Popen("paasta-deployd", stderr=PIPE)
    output = context.daemon1.stderr.readline().decode("utf-8")
    fd = context.daemon1.stderr
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    for i in range(0, 5):
        try:
            output = context.daemon1.stderr.readline().decode("utf-8")
            print(output.rstrip("\n"))
            assert "This node is elected as leader" not in output
        except IOError:
            pass
        time.sleep(1)


@then("a second deployd becomes leader")
def second_deployd_is_leader(context):
    try:
        output = context.daemon1.stderr.readline().decode("utf-8")
    except IOError:
        output = ""
    start = time.time()
    timeout = start + 60
    while "This node is elected as leader" not in output:
        try:
            output = context.daemon1.stderr.readline().decode("utf-8")
        except IOError:
            output = ""
        if output:
            print(output.rstrip("\n"))
        if time.time() > timeout:
            raise Exception("Timed out waiting for second deployd leader")
        time.sleep(1)
    context.daemon1.terminate()
    context.daemon1.wait()


@then('we should see "{service_instance}" listed in marathon after {seconds:d} seconds')
def check_app_running(context, service_instance, seconds):
    service, instance, _, _ = decompose_job_id(service_instance)
    service_configuration_lib._yaml_cache = {}
    context.marathon_config = load_marathon_service_config_no_cache(
        service, instance, context.cluster
    )
    context.app_id = context.marathon_config.format_marathon_app_dict()["id"]
    step = 5
    attempts = 0
    context.current_client = context.marathon_clients.get_current_client_for_service(
        context.marathon_config
    )
    while (attempts * step) < seconds:
        if context.app_id in list_all_marathon_app_ids(context.current_client):
            break
        time.sleep(step)
        attempts += 1
    assert context.app_id in list_all_marathon_app_ids(context.current_client)
    context.old_app_id = context.app_id


@then("we should not see the old version listed in marathon after {seconds:d} seconds")
def check_app_not_running(context, seconds):
    step = 5
    attempts = 0
    while (attempts * step) < seconds:
        if context.old_app_id not in list_all_marathon_app_ids(context.current_client):
            return
        time.sleep(step)
        attempts += 1
    assert context.old_app_id not in list_all_marathon_app_ids(context.current_client)


@then("we set a new command for our service instance to {cmd}")
def set_cmd(context, cmd):
    context.cmd = cmd


@then('the appid for "{service_instance}" should have changed')
def check_sha_changed(context, service_instance):
    service, instance, _, _ = decompose_job_id(service_instance)
    service_configuration_lib._yaml_cache = {}
    context.marathon_config = load_marathon_service_config_no_cache(
        service, instance, context.cluster
    )
    assert context.app_id != context.marathon_config.format_marathon_app_dict()["id"]


@given(
    'we have a secret called "{secret_name}" for the service "{service}" with signature "{signature}"'
)
def create_secret_json_file(context, secret_name, service, signature):
    secret = {
        "environments": {
            "devc": {"ciphertext": "ScrambledNonsense", "signature": signature}
        }
    }
    if not os.path.exists(os.path.join(context.soa_dir, service, "secrets")):
        os.makedirs(os.path.join(context.soa_dir, service, "secrets"))

    with open(
        os.path.join(context.soa_dir, service, "secrets", f"{secret_name}.json"), "w"
    ) as secret_file:
        json.dump(secret, secret_file)


@given(
    'we set the an environment variable called "{var}" to "{val}" for '
    'service "{service}" and instance "{instance}" for framework "{framework}"'
)
def add_env_var(context, var, val, service, instance, framework):
    field = "env"
    value = {var: val}
    modify_configs(context, field, framework, service, instance, value)


@when('we set some arbitrary data at "{zookeeper_path}" in ZK')
def zookeeper_write_bogus_key(context, zookeeper_path):
    with mock.patch.object(
        SystemPaastaConfig, "get_zk_hosts", autospec=True, return_value=context.zk_hosts
    ):
        with ZookeeperPool() as zookeeper_client:
            zookeeper_client.ensure_path(zookeeper_path)
            zookeeper_client.set(zookeeper_path, b"WHATEVER")


@given("we remove autoscaling ZK keys for test-service")
def zookeeper_rmr_keys(context):
    context.zk_hosts = "%s/mesos-testcluster" % get_service_connection_string(
        "zookeeper"
    )
    with mock.patch.object(
        SystemPaastaConfig, "get_zk_hosts", autospec=True, return_value=context.zk_hosts
    ):
        with ZookeeperPool() as zookeeper_client:
            zookeeper_client.delete("/autoscaling/test-service", recursive=True)
