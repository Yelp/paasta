import json
import os
import subprocess
import time

import psutil
import yaml
from behave import given
from behave import then

from paasta_tools.smartstack_tools import retrieve_haproxy_csv
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT


@given('a working synapse-tools configuration and a blank synapse config')
def working_synapse_tools_configuration(context):
    context.synapse_config_dir = '/etc/synapse'
    if not os.path.exists(context.synapse_config_dir):
        os.makedirs(context.synapse_config_dir)
    with open(os.path.join(context.synapse_config_dir, 'synapse-tools.conf.json'), 'w') as f:
        f.write(json.dumps({
            "config_file": "/etc/synapse/synapse.conf.json",
            "bind_addr": "0.0.0.0",
            "stats_port": 32123,
            "haproxy.defaults.inter": "10s",
            "synapse_restart_command": "/bin/true",
            "reload_cmd_fmt": "{haproxy_path} -f {haproxy_config_path} -p {haproxy_pid_file_path}"
        }))
    with open(os.path.join(context.synapse_config_dir, 'synapse.conf.json'), 'w') as f:
        f.write('')


@given('a zookeeper discovery file')
def zookeeper_discovery_file(context):
    context.zookeeper_discovery_dir = '/nail/etc/zookeeper_discovery/infrastructure'
    context.zookeeper_discovery_file = os.path.join(context.zookeeper_discovery_dir, 'local.yaml')
    if not os.path.exists(context.zookeeper_discovery_dir):
        os.makedirs(context.zookeeper_discovery_dir)
    with open(context.zookeeper_discovery_file, 'w') as f:
        yaml.dump(
            [["zookeeper", 2181]],
            stream=f,
        )


@given('environment_tools data')
def given_location_types_json(context):
    if not os.path.exists('/nail/etc/services'):
        os.makedirs('/nail/etc/services')
    with open('/nail/etc/services/location_types.json', 'w') as f:
        f.write(json.dumps(['region']))
    with open('/nail/etc/region', 'w') as f:
        f.write('fake_region')


@given('we have run configure_synapse')
def run_configure_synapse(context):
    subprocess.check_call(['configure_synapse', '--soa-dir=%s' % context.soa_dir])


@given('we have started synapse')
def start_synapse(context):
    context.synapse_process = subprocess.Popen(
        'synapse --config /etc/synapse/synapse.conf.json'.split(),
        env={"PATH": "/opt/rbenv/bin:" + os.environ['PATH']}
    )


@given('we wait for haproxy to start')
def wait_for_haproxy(context):
    for _ in xrange(600):
        try:
            with open('/var/run/synapse/haproxy.pid') as f:
                context.haproxy_pid = int(f.read())
        except IOError:
            time.sleep(0.1)
        else:
            assert psutil.pid_exists(context.haproxy_pid)
            return

    raise Exception("haproxy did not start before timeout")


@then(u'we should see {service_namespace} backend in the haproxy status')
def we_should_see_service_namespace_in_haproxy_status(context, service_namespace):
    reader = retrieve_haproxy_csv('localhost', 32123, DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT)

    for line in reader:
        if line['# pxname'] == service_namespace and line['svname'] == 'BACKEND':
            return

    raise Exception("Didn't see backend for %s in haproxy status" % service_namespace)
