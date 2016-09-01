import json
import multiprocessing
import os
import socket
import subprocess
import time

from behave import given
from behave import then

HEARTBEAT_PATH = "/var/run/nerve_tools_itest_heartbeat_path"

# Must be kept consistent with entries in zookeeper_discovery directory
ZOOKEEPER_CONNECT_STRING = "zookeeper_1:2181"


@given('some services to configure_nerve on')
def some_services_for_configure_nerve(context):
    context.SERVICES = [
        {
            'name': 'service_three.main',
            'path': '/nerve/region:sjc-dev/service_three.main',
            'host': 'servicethree_1',
            'port': 1024,
        },
        {
            'name': 'service_three.main',
            'path': '/nerve/region:uswest1-prod/service_three.main',
            'host': 'servicethree_1',
            'port': 1024,
        },
        {
            'name': 'service_one.main',
            'path': '/nerve/region:sjc-dev/service_one.main',
            'host': 'serviceone_1',
            'port': 1025,
        },
        {
            'name': 'scribe.main',
            'path': '/nerve/region:sjc-dev/scribe.main',
            'host': 'scribe_1',
            'port': 1464,
        },
    ]


@given('we forward health checks to the services')
def forward_health_checks(context):
    context.socat_procs = []
    for service in context.SERVICES:
        host = service['host']
        port = service['port']
        context.socat_procs.append(subprocess.Popen(
            ('socat TCP4-LISTEN:%d,fork TCP4:%s:%d' % (port, host, port)).split()))


@given('a hacheck process locally')
def local_hacheck_process(context):
    context.hacheck_process = subprocess.Popen('/usr/bin/hacheck -p 6666'.split())


@given('we run configure_nerve')
def run_configure_nerve(context):
    subprocess.check_call(
        ['configure_nerve', '-f', HEARTBEAT_PATH, '-s', '100', '--nerve-registration-delay-s', '0']
    )


@given('we start nerve')
def start_nerve(context):
    with open('/work/nerve.log', 'w') as fd:
        context.nerve_process = subprocess.Popen(
            'nerve --config /etc/nerve/nerve.conf.json'.split(),
            env={"PATH": "/opt/rbenv/bin:" + os.environ['PATH']},
            stdout=fd, stderr=fd)

        with open('/var/run/nerve.pid', 'w') as pid_fd:
            pid_fd.write(str(context.nerve_process.pid))

        # Give nerve a moment to register the service in Zookeeper
        time.sleep(10)


@then('we should see the expected services')
def check_expected_services(context):
    expected_services = [
        # HTTP service with extra advertisements
        'service_three.main.westcoast-dev.region:sjc-dev.1024.new',
        'service_three.main.westcoast-prod.region:uswest1-prod.1024.new',

        # TCP service
        'service_one.main.westcoast-dev.region:sjc-dev.1025.new',

        # Puppet-configured services
        'scribe.main.westcoast-dev.region:sjc-dev.1464.new',
        'mysql_read.main.westcoast-dev.region:sjc-dev.1464.new',
    ]

    with open('/etc/nerve/nerve.conf.json') as fd:
        nerve_config = json.load(fd)
    actual_services = nerve_config['services'].keys()

    assert set(expected_services) == set(actual_services)


@then('we check a single nerve service entry')
def test_nerve_service_config(setup):
    # Check a single nerve service entry
    expected_service_entry = {
        "check_interval": 2.0,
        "checks": [
            {
                "fall": 2,
                "host": "127.0.0.1",
                "port": 6666,
                "rise": 1,
                "timeout": 1.0,
                "open_timeout": 1.0,
                "type": "http",
                "uri": "/http/service_three.main/1024/status",
                "headers": {
                    "Host": "www.test.com"
                },
            }
        ],
        "host": my_ip_address(),
        "port": 1024,
        "weight": num_cpus(),
        "zk_hosts": [ZOOKEEPER_CONNECT_STRING],
        "zk_path": "/nerve/region:sjc-dev/service_three.main"
    }

    with open('/etc/nerve/nerve.conf.json') as fd:
        nerve_config = json.load(fd)
    actual_service_entry = \
        nerve_config['services'].get('service_three.main.westcoast-dev.region:sjc-dev.1024.new')

    assert expected_service_entry == actual_service_entry


def my_ip_address():
    return socket.gethostbyname(socket.gethostname())


def num_cpus():
    try:
        return max(multiprocessing.cpu_count(), 10)
    except NotImplementedError:
        return 10
