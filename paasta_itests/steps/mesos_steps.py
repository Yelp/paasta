import sys

import contextlib
import mock
from behave import given, when, then
import mesos.cli.master

from itest_utils import get_service_connection_string

sys.path.append('../')
from paasta_tools import check_mesos_resource_utilization

@when(u'we check mesos utilization with a threshold of {percent} percent')
def check_mesos_utilization(context, percent):
    config = {
        "master": "%s" % get_service_connection_string('mesosmaster'),
        "scheme": "http"
    }

    with contextlib.nested(
        mock.patch('paasta_tools.check_mesos_resource_utilization.send_event'),
        mock.patch.object(mesos.cli.master, 'CFG', config),
    ) as (
        mock_events,
        mock_cfg,
    ):
        context.mesos_util_check = check_mesos_resource_utilization.check_thresholds(int(percent))

@then(u'the result is {result}')
def mesos_util_result(context, result):
    assert result in context.mesos_util_check
