import sys

import mock
from behave import given, when, then
import mesos.cli.master

from itest_utils import get_service_connection_string

sys.path.append('../')
from paasta_tools import check_mesos_resource_utilization


@then(u'we should get {result} when checking mesos utilization {percent} percent')
def ok_mesos_utilization(context, result, percent):
    config = {
        "master": "%s" % get_service_connection_string('mesosmaster'),
        "scheme": "http"
    }

    with mock.patch('paasta_tools.check_mesos_resource_utilization.send_event') as mock_events:
        with mock.patch.object(mesos.cli.master, 'CFG', config) as mock_cfg:
            assert result in  check_mesos_resource_utilization.check_thresholds(int(percent))
