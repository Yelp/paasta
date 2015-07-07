import sys

from behave import given, when, then

import chronos
from itest_utils import get_service_connection_string


@given('a working chronos instance')
def working_chronos(context):
    if not hasattr(context, 'chronos_client'):
        connection_string = "http://%s" % \
            get_service_connection_string('chronos')
        context.chronos_client = chronos.connect(connection_string)
    else:
        print "Chronos connection already established"


@then(u'we should be able to list jobs')
def list_chronos_jobs(context):
    pass
