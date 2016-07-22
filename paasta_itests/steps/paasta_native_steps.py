import json
import os
import time
from tempfile import mkdtemp

import yaml
from behave import given
from behave import then
from behave import when

from paasta_tools import mesos_tools
from paasta_tools.native_mesos_scheduler import create_driver
from paasta_tools.native_mesos_scheduler import main
from paasta_tools.native_mesos_scheduler import PaastaNativeServiceConfig
from paasta_tools.native_mesos_scheduler import PaastaScheduler
from paasta_tools.utils import load_system_paasta_config


@given('a new paasta_native config to be deployed, with {num} instances')
def new_paasta_native_config(context, num):
    context.cluster = 'fake_cluster'
    context.instance = 'fake_instance'
    context.service = 'fake_service'

    context.new_config = PaastaNativeServiceConfig(
        cluster=context.cluster,
        instance=context.instance,
        service=context.service,
        config_dict={
            "cpus": 0.1,
            "mem": 100,
            "instances": int(num),
            "cmd": 'sleep 50',
        },
        branch_dict={
            'docker_image': 'busybox',
            'desired_state': 'start',
            'force_bounce': None,
        },
    )


@when('we start a paasta_native scheduler')
def start_paasta_native_framework(context):
    context.scheduler = PaastaScheduler(
        service_name=context.service,
        instance_name=context.instance,
        cluster=context.cluster,
        service_config=context.new_config,
    )

    context.driver = create_driver(
        service=context.service,
        instance=context.instance,
        scheduler=context.scheduler,
        system_paasta_config=load_system_paasta_config(),
    )

    context.driver.start()


@then('it should eventually start {num} tasks')
def should_eventually_start_num_tasks(context, num):
    num = int(num)

    for _ in xrange(20):
        if len(context.scheduler.running) >= num:
            return
        time.sleep(1)

    raise Exception("Expected %d tasks before timeout, saw %d" % (num, len(context.scheduler.tasks)))


@given('a fresh soa_dir')
def fresh_soa_dir(context):
    soa_dir = mkdtemp()
    context.soa_dir = soa_dir


@given(u'paasta_native-cluster.yaml and deployments.json files for service {service} with instance {instance}')
def write_paasta_native_cluster_yaml_files(context, service, instance):
    if not os.path.exists(os.path.join(context.soa_dir, service)):
        os.makedirs(os.path.join(context.soa_dir, service))
    with open(os.path.join(context.soa_dir, service, 'paasta_native-%s.yaml' % context.cluster), 'w') as f:
        f.write(yaml.safe_dump({
            instance: {
                'cmd': 'echo "Taking a nap..." && sleep 1m && echo "Nap time over, back to work"',
                'mem': 100,
                'cpus': 0.1,
                'instances': 1,
            }
        }))
    with open(os.path.join(context.soa_dir, service, 'deployments.json'), 'w') as f:
        json.dump({
            'v1': {
                '%s.%s' % (context.cluster, instance): {
                    'image': 'busybox',
                    'desired_state': 'start',
                    'force_bounce': None,
                }
            }
        }, f)


@when(u'we run native_mesos_scheduler.main()')
def run_native_mesos_scheduler_main(context):
    main(['--soa-dir', context.soa_dir, '--stay-alive-seconds', '10'])


@then(u'there should be a framework registered with id {framework_id}')
def should_be_framework_with_id(context, framework_id):
    try:
        del mesos_tools.master.CURRENT._cache
        print "cleared mesos_tools.master.CURRENT._cache"
    except AttributeError:
        pass

    assert framework_id in mesos_tools.list_frameworks()
