import json
import os
import time
from tempfile import mkdtemp

import mock
import yaml
from behave import given
from behave import then
from behave import when
from itest_utils import clear_mesos_tools_cache

from paasta_tools import drain_lib
from paasta_tools import mesos_tools
from paasta_tools.native_mesos_scheduler import create_driver
from paasta_tools.native_mesos_scheduler import LIVE_TASK_STATES
from paasta_tools.native_mesos_scheduler import main
from paasta_tools.native_mesos_scheduler import PaastaNativeServiceConfig
from paasta_tools.native_mesos_scheduler import PaastaScheduler
from paasta_tools.native_mesos_scheduler import TASK_RUNNING
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
            "mem": 50,
            "instances": int(num),
            "cmd": 'sleep 50',
            "drain_method": "test"
        },
        branch_dict={
            'docker_image': 'busybox',
            'desired_state': 'start',
            'force_bounce': None,
        },
        service_namespace_config=None,
    )


@when('we start a paasta_native scheduler with reconcile_backoff {reconcile_backoff}')
def start_paasta_native_framework(context, reconcile_backoff):
    clear_mesos_tools_cache()
    system_paasta_config = load_system_paasta_config()
    system_paasta_config['docker_registry'] = 'docker.io'  # so busybox runs.

    context.scheduler = PaastaScheduler(
        service_name=context.service,
        instance_name=context.instance,
        cluster=context.cluster,
        system_paasta_config=system_paasta_config,
        service_config=context.new_config,
        reconcile_backoff=int(reconcile_backoff),
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
        actual_num = len([p for p in context.scheduler.tasks_with_flags.values() if p.mesos_task_state == TASK_RUNNING])
        if actual_num >= num:
            return
        time.sleep(1)

    raise Exception("Expected %d tasks before timeout, saw %d" % (num, actual_num))


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
                '%s:paasta-%s.%s' % (service, context.cluster, instance): {
                    'docker_image': 'busybox',
                    'desired_state': 'start',
                    'force_bounce': None,
                }
            }
        }, f)


@when(u'we run native_mesos_scheduler.main()')
def run_native_mesos_scheduler_main(context):
    clear_mesos_tools_cache()
    context.main_schedulers = main([
        '--soa-dir', context.soa_dir,
        '--stay-alive-seconds', '10',
        '--periodic-interval', '1'
    ])


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
        print("terminating framework %s" % context.scheduler.framework_id)
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


@when(u'we change force_bounce')
def we_change_the_config(context):
    branch_dict = context.scheduler.service_config.branch_dict
    context.old_force_bounce = branch_dict['force_bounce']
    branch_dict['force_bounce'] = str(int(branch_dict['force_bounce'] or 0) + 1)


@when(u'we change force_bounce back')
def we_change_force_bounce_back(context):
    branch_dict = context.scheduler.service_config.branch_dict
    branch_dict['force_bounce'] = context.old_force_bounce


@then(u'it should eventually drain {num} tasks')
def it_should_drain_num_tasks(context, num):
    num = int(num)
    for _ in xrange(10):
        if len(drain_lib.TestDrainMethod.downed_task_ids) >= num:
            # set() to make a copy.
            context.drained_tasks = set(drain_lib.TestDrainMethod.downed_task_ids)
            return
        time.sleep(1)
    else:
        raise Exception("Expected %d tasks to drain before timeout, saw %d" % (
            num,
            len(drain_lib.TestDrainMethod.downed_task_ids)
        ))


@then(u'it should undrain {num_undrain_expected} tasks and drain {num_drain_expected} more')
def it_should_undrain_and_drain(context, num_undrain_expected, num_drain_expected):
    num_undrain_expected = int(num_undrain_expected)
    num_drain_expected = int(num_drain_expected)

    for _ in xrange(10):
        print("currently drained: %r" % drain_lib.TestDrainMethod.downed_task_ids)
        print("drained previously: %r" % context.drained_tasks)
        num_drained = len(drain_lib.TestDrainMethod.downed_task_ids - context.drained_tasks)
        num_undrained = len(context.drained_tasks - drain_lib.TestDrainMethod.downed_task_ids)
        if num_drained >= num_drain_expected and num_undrained >= num_undrain_expected:
            return
        time.sleep(1)
    else:
        raise Exception("Expected %d tasks to drain and %d to undrain, saw %d and %d" % (
            num_drain_expected,
            num_undrain_expected,
            num_drained,
            num_undrained,
        ))


@then(u'it should eventually have only {num} tasks')
def it_should_eventually_have_only_num_tasks(context, num):
    num = int(num)

    for _ in xrange(30):
        actual_num = len([p for p in context.scheduler.tasks_with_flags.values() if p.mesos_task_state == TASK_RUNNING])
        if actual_num <= num:
            return
        time.sleep(1)

    raise Exception("Expected <= %d tasks before timeout, saw %d" % (num, actual_num))


@when(u'we call periodic')
def we_call_periodic(context):
    with mock.patch.object(context.scheduler, 'load_config'):
        context.scheduler.periodic(context.driver)


@when(u'we change instances to {num}')
def we_change_instances_to_num(context, num):
    num = int(num)
    context.scheduler.service_config.config_dict['instances'] = num


@then(u'it should not start tasks for {num} seconds')
def should_not_start_tasks_for_num_seconds(context, num):
    time.sleep(int(num))

    assert [] == [p for p in context.scheduler.tasks_with_flags.values() if (p.mesos_task_state in LIVE_TASK_STATES)]


@then(u'periodic() should eventually be called')
def periodic_should_eventually_be_called(context):
    for _ in xrange(30):
        for scheduler in context.main_schedulers:
            if hasattr(scheduler, 'periodic_was_called'):
                return
    else:
        raise Exception("periodic() not called on all schedulers")
