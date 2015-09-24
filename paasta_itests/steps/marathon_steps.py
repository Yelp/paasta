import os
import sys
from tempfile import mkdtemp

from behave import when, then
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


@given(u'I have a deployments.json for the service "{service}" with marathon instance "{instance}"')
def marathon_app_deployments_json(context, service, instance):
    with open(os.path.join(context.soa_dir, service, 'deployments.json'), 'w') as dp:
        dp.write(json.dumps({
            'v1': {
                '%s:%s' % (service, paasta_tools.utils.get_default_branch(context.cluster, instance)): {
                    'docker_image': 'test-image-foobar%d' % context.tag_version,
                    'desired_state': 'start',
                }
            }
        }))


@given(u'I have yelpsoa-configs for the service "{service}" with marathon instance "{instance}"')
def marathon_app_yelpsoa_configs(context, service, instance):
    soa_dir = mkdtemp()
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))
    with open(os.path.join(soa_dir, service, 'marathon-%s.yaml' % context.cluster), 'w') as f:
        f.write(yaml.dump({
            "%s" % instance: {
                'schedule': 'R/2000-01-01T16:20:00z/PT60S',
                'command': 'echo "Taking a nap..." && sleep 1m && echo "Nap time over, back to work"',
                'monitoring': {'team': 'fake_team'},
                'disabled': False,
            }
        }))
    context.soa_dir = soa_dir
