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
    clear_mesos_tools_cache()
    system_paasta_config = load_system_paasta_config()
    system_paasta_config['docker_registry'] = 'docker.io'  # so busybox runs.

    context.scheduler = PaastaScheduler(
        service_name=context.service,
        instance_name=context.instance,
        cluster=context.cluster,
        system_paasta_config=system_paasta_config,
        service_config=context.new_config,
    )

    context.driver = create_driver(
        service=context.service,
        instance=context.instance,
        scheduler=context.scheduler,
        system_paasta_config=system_paasta_config,
    )

    context.driver.start()

    if not hasattr(context, 'framework_ids'):
        context.framework_ids = []

    for _ in xrange(10):
        if context.scheduler.framework_id:
            context.framework_ids.append(context.scheduler.framework_id)
            break
        time.sleep(1)
    else:
        raise Exception("Expected scheduler to successfully register before timeout")


@then('it should eventually start {num} tasks')
def should_eventually_start_num_tasks(context, num):
    num = int(num)

    for _ in xrange(20):
        if len(context.scheduler.running) >= num:
            return
        time.sleep(1)

    raise Exception("Expected %d tasks before timeout, saw %d" % (num, len(context.scheduler.running)))


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
                    'docker_image': 'busybox',
                    'desired_state': 'start',
                    'force_bounce': None,
                }
            }
        }, f)


@when(u'we run native_mesos_scheduler.main()')
def run_native_mesos_scheduler_main(context):
    main(['--soa-dir', context.soa_dir, '--stay-alive-seconds', '10'])


@then(u'there should be a framework registered with name {name}')
def should_be_framework_with_id(context, name):
    clear_mesos_tools_cache()
    assert name in [f.name for f in mesos_tools.get_all_frameworks(active_only=True)]


@then(u'there should not be a framework registered with name {name}')
def should_not_be_framework_with_name(context, name):
    clear_mesos_tools_cache()
    assert name not in [f.name for f in mesos_tools.get_all_frameworks(active_only=True)]


@when(u'we terminate that framework')
def terminate_that_framework(context):
    try:
        print "terminating framework %s" % context.scheduler.framework_id
        mesos_tools.terminate_framework(context.scheduler.framework_id)
    except Exception as e:
        raise Exception(e.response.text)


@when(u'we stop that framework without terminating')
def stop_that_framework(context):
    context.driver.stop(True)
    context.driver.join()


@then(u'it should have the same ID as before')
def should_have_same_id(context):
    assert context.framework_ids[-2] == context.framework_ids[-1]


@then(u'it should have a different ID than before')
def should_have_different_id(context):
    assert context.framework_ids[-2] != context.framework_ids[-1]


@when(u'we sleep {wait} seconds')
def we_sleep_wait_seconds(context, wait):
    time.sleep(int(wait))


def clear_mesos_tools_cache():
    try:
        del mesos_tools.master.CURRENT._cache
        print "cleared mesos_tools.master.CURRENT._cache"
    except AttributeError:
        pass
