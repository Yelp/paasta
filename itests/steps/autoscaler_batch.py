from argparse import ArgumentParser

import behave
import mock
import staticconf.testing
from botocore.exceptions import ClientError
from hamcrest import assert_that
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import has_string

from clusterman.exceptions import ClustermanSignalError
from clusterman.util import Status

try:
    from clusterman.batch.autoscaler import AutoscalerBatch
except ImportError:
    pass


def _check_sensu_args(call_args, *, name=None, app_name=None, status=Status.OK):
    __, args = call_args
    signal_sensu_config = staticconf.read_list('sensu_config', default=[{}], namespace='bar.mesos_config').pop()
    service_sensu_config = staticconf.read_list('sensu_config', default=[{}]).pop()
    if app_name:
        team = signal_sensu_config['team'] if signal_sensu_config else service_sensu_config['team']
    else:
        team = service_sensu_config['team']

    assert_that(args['status'], equal_to(status.value))
    assert_that(args['team'], equal_to(team))


@behave.fixture
def autoscaler_patches(context):
    with mock.patch('clusterman.batch.autoscaler.PoolManager'), \
            mock.patch('clusterman.autoscaler.autoscaler.get_monitoring_client'), \
            mock.patch('clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient'), \
            mock.patch('clusterman.autoscaler.autoscaler.Signal') as signal_class, \
            mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.autoscaler.autoscaler.dynamodb'), \
            mock.patch('clusterman.batch.autoscaler.splay_event_time', return_value=0), \
            mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', mock.PropertyMock(
                side_effect=[True, False],
            )), staticconf.testing.PatchConfiguration({'autoscaling': {'default_signal_role': 'bar'}}), \
            mock.patch('clusterman.util._get_sensu') as sensu:
        context.signal_class = signal_class
        context.sensu = sensu.return_value
        yield


@behave.given('the autoscaler batch')
def autoscaler_batch(context):
    behave.use_fixture(autoscaler_patches, context)
    args = ['--cluster', 'mesos-test', '--pool', 'bar']
    parser = ArgumentParser()
    context.batch = AutoscalerBatch()
    context.batch.parse_args(parser)
    context.batch.options = parser.parse_args(args)
    context.batch.options.instance_name = 'foo'
    context.batch.configure_initial()


@behave.when('the autoscaler fails')
def autoscaler_fails(context):
    # If the signal doesn't return a ClustermanSignalError it's a problem with the autoscaler
    expected_error = 'signal evaluation failed'
    context.batch.autoscaler.signal.evaluate.side_effect = ValueError(expected_error)
    try:
        context.batch.run()
    except ValueError as e:
        # Make sure that we're not masking an error somewhere
        assert_that(e, has_string(expected_error))


@behave.when('the (?P<signal_type>application|default) signal fails')
def signal_fails(context, signal_type):
    expected_error = f'the {signal_type} signal failed'
    response = {'cpus': None}

    eval_error = ClustermanSignalError(expected_error)
    context.batch.autoscaler.signal.evaluate.side_effect = (
        eval_error
        if signal_type == 'application'
        else response
    )
    context.batch.autoscaler.default_signal.evaluate.return_value = (
        response
        if signal_type == 'application'
        else eval_error
    )
    try:
        context.batch.run()
    except ClustermanSignalError as e:
        # Make sure that we're not masking an error somewhere
        assert_that(e, has_string(expected_error))


@behave.when('signal evaluation succeeds')
def signals_ok(context):
    context.batch.run()


@behave.when('a RequestLimitExceeded error occurs')
def rle(context):
    context.batch.autoscaler._compute_target_capacity = mock.Mock(
        side_effect=ClientError({'Error': {'Code': 'RequestLimitExceeded'}}, 'foo'),
    )
    context.batch.run()


@behave.then('the application owner should (?P<not_>not )?get warned for initialization')
def check_warn_app_owner(context, not_):
    if not_:
        # If there's no warning then we just expect two checkins
        assert_that(context.sensu.send_event.call_args_list, has_length(2))
    else:
        _check_sensu_args(context.sensu.send_event.call_args_list[0], app_name='bar', status=Status.WARNING)


@behave.then(
    'the (?P<thing>application|service) owner should (?P<not_>not )?get paged for (?P<stage>initialization|evaluation)'
)
def check_who_got_paged(context, thing, not_, stage):
    app_name = 'bar' if thing == 'application' else None
    index = -2 if thing == 'application' else -1  # We call the signal sensu check and then the service sensu check
    status = Status.OK if not_ else Status.CRITICAL
    _check_sensu_args(context.sensu.send_event.call_args_list[index], app_name=app_name, status=status)
