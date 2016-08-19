import os

from behave import given


@given('a fake haproxy pid file')
def fake_haproxy_pid_file(context):
    os.makedirs('/var/run/synapse')
    with open('/var/run/synapse/haproxy.pid', 'w') as f:
        f.write('65535')
