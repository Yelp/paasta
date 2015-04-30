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

@when(u'we fsm a new service with --auto')
def step_impl_when_auto(context):
    cmd = ("../paasta_tools/paasta_cli/paasta_cli.py fsm "
           "--yelpsoa-config-root %s "
           "--auto "
           "--service-name new_paasta_service "
           "--team paasta_team"
           % context.fake_yelpsoa_configs
    )
    print "Running cmd %s" % cmd
    (returncode, output) = _run(cmd)
    print "Got returncode %s with output:" % returncode
    print output

@then(u'the new yelpsoa-configs directory has sane values')
def step_impl_then(context):
    all_services = read_services_configuration(soa_dir=context.fake_yelpsoa_configs)
    print "all_services %s" % all_services['new_paasta_service']
