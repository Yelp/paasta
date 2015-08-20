import os
import shutil
import tempfile

from behave import given, when, then
from service_configuration_lib import read_services_configuration

from paasta_tools.utils import _run


@given(u'a fake yelpsoa-config-root')
def step_impl_given(context):
    # Cleaned up in after_scenario()
    context.tmpdir = tempfile.mkdtemp('paasta_tools_fsm_itest')
    context.fake_yelpsoa_configs = os.path.join(
        context.tmpdir,
        'yelpsoa-configs',
    )
    fake_yelpsoa_configs_src = os.path.join(
        'fake_soa_configs_fsm_wizard',
    )
    shutil.copytree(
        fake_yelpsoa_configs_src,
        context.fake_yelpsoa_configs,
    )


def _load_yelpsoa_configs(context, service_name):
    all_services = read_services_configuration(soa_dir=context.fake_yelpsoa_configs)
    context.my_config = all_services[service_name]


@when(u'we fsm a new service with --auto')
def step_impl_when_fsm_auto(context):
    service_name = "new_paasta_service"
    cmd = (
        "../paasta_tools/paasta_cli/paasta_cli.py fsm "
        "--yelpsoa-config-root %s "
        "--auto "
        "--service-name %s "
        "--team paasta"
        % (context.fake_yelpsoa_configs, service_name)
    )
    print "Running cmd %s" % cmd
    (returncode, output) = _run(cmd)
    print "Got returncode %s with output:" % returncode
    print output
    _load_yelpsoa_configs(context, service_name)


@then(u'the new yelpsoa-configs directory has the expected smartstack proxy_port')
def step_impl_then_proxy_port(context):
    # Largest proxy_port in fake_yelpsoa_configs + 1
    assert context.my_config['smartstack']['main']['proxy_port'] == 20667
