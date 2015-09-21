import sys
import time

from behave import when, then
import mock

sys.path.append('../')
import paasta_tools
from paasta_tools import marathon_serviceinit
from paasta_tools.utils import _run


@when(u'we run the marathon job test-service.main')
def run_marathon_test_service(context):
    trivial_app_config = {
        'id': 'test-service.main',
        'cmd': '/bin/sleep 1m',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app('test-service.main', trivial_app_config, context.marathon_client)


@when(u'we wait for it to be deployed')
def wait_for_deploy(context):
    print "Sleeping 10 seconds to wait for deployment..."
    time.sleep(10)


@then(u'marathon_serviceinit status_marathon_job should return "Healthy"')
def status_marathon_job_returns_healthy(context):
    normal_instance_count = 1
    app_id = 'test-service.main'
    service = 'test-service'
    instance = 'main'

    output = marathon_serviceinit.status_marathon_job(
        service,
        instance,
        app_id,
        normal_instance_count,
        context.marathon_client
    )
    assert "Healthy" in output


@then(u'marathon_serviceinit restart should get new task_ids')
def marathon_restart_gets_new_task_ids(context):
    normal_instance_count = 1
    cluster = context.system_paasta_config['cluster']
    app_id = 'test-service.main'
    service = 'test-service'
    instance = 'main'

    old_tasks = context.marathon_client.get_app(app_id).tasks
    marathon_serviceinit.restart_marathon_job(
        service,
        instance,
        app_id,
        normal_instance_count,
        context.marathon_client,
        cluster
    )
    print "Sleeping 5 seconds to wait for test-service to be restarted."
    time.sleep(5)
    new_tasks = context.marathon_client.get_app(app_id).tasks
    print "Tasks before the restart: %s" % old_tasks
    print "Tasks after  the restart: %s" % new_tasks
    assert old_tasks != new_tasks


@then(u"paasta_serviceinit status exits with return code 0 and the correct output")
def chronos_status_returns_healthy(context):
    cmd = '../paasta_tools/paasta_serviceinit.py --soa-dir %s test-service.job status' % context.soa_dir
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0
    assert "Disabled" in output
    assert "New" in output


@when(u"we paasta_serviceinit emergency-stop the chronos job")
def chronos_emergency_stop_job(context):
    cmd = '../paasta_tools/paasta_serviceinit.py --soa-dir %s test-service.job stop' % context.soa_dir
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


@when(u"we paasta_serviceinit emergency-start the chronos job")
def chronos_emergency_start_job(context):
    cmd = '../paasta_tools/paasta_serviceinit.py --soa-dir %s test-service.job start' % context.soa_dir
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


@when(u"we paasta_serviceinit emergency-restart the chronos job")
def chronos_emergency_restart_job(context):
    cmd = '../paasta_tools/paasta_serviceinit.py --soa-dir %s test-service.job restart' % context.soa_dir
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
