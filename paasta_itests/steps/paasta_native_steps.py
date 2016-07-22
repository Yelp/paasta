import time

from behave import given
from behave import then
from behave import when

from paasta_tools.native_mesos_scheduler import create_driver
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
        config_dict={},
        branch_dict={'instances': int(num)},
    )


@when('we start a paasta_native scheduler')
def start_paasta_native_framework(context):
    context.scheduler = PaastaScheduler(
        service_name=context.service,
        instance_name=context.instance,
        cluster=context.cluster,
        config=context.config,
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

    for _ in xrange(60):
        if len(context.scheduler.tasks) >= num:
            return
        time.sleep(1)

    raise Exception("Expected %d tasks before timeout, saw %d" % (num, len(context.scheduler.tasks)))
