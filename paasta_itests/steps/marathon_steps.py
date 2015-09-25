import os
import sys
from tempfile import mkdtemp

from behave import given, when, then
import json
import mock
import yaml

sys.path.append('../')
import paasta_tools


@when(u'we create a trivial marathon app')
def create_trivial_marathon_app(context):
    app_config = {
        'id': 'test--marathon--app',
        'cmd': '/bin/true',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app(app_config['id'], app_config, context.marathon_client)


@then(u'we should see it running in marathon')
def list_marathon_apps_has_trivial_app(context):
    assert 'test--marathon--app' in paasta_tools.marathon_tools.list_all_marathon_app_ids(context.marathon_client)
    assert context.marathon_client.get_app('/test--marathon--app')
