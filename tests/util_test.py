# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys

import arrow
import mock
import pytest
import staticconf.testing
from colorama import Fore
from colorama import Style

from clusterman.util import any_of
from clusterman.util import ask_for_choice
from clusterman.util import ask_for_confirmation
from clusterman.util import autoscaling_is_paused
from clusterman.util import color_conditions
from clusterman.util import get_cluster_name_list
from clusterman.util import get_pool_name_list
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string
from clusterman.util import sensu_checkin
from clusterman.util import Status


@pytest.fixture(autouse=True)
def mock_clog():
    sys.modules['clog'] = mock.Mock()  # clog is imported in the main function so this is how we mock it
    yield


@pytest.mark.parametrize('inp,response', [('\n', True), ('\n', False), ('yE', True), ('n', False)])
def test_ask_for_confirmation(inp, response):
    with mock.patch('builtins.input', side_effect=inp):
        assert ask_for_confirmation(default=response) == response


def test_ask_for_confirmation_invalid_input():
    with mock.patch('builtins.input', side_effect=['asdf', 'yes']) as mock_input:
        assert ask_for_confirmation() is True
        assert mock_input.call_count == 2


def test_ask_for_choice():
    with mock.patch('builtins.input', side_effect=['asdf', '-1', '20', '2']) as mock_input:
        assert ask_for_choice('Pick something', ['a', 'b', 'c']) == 'c'
        assert mock_input.call_count == 4


@pytest.mark.parametrize('input_str,color', [
    ('foo', Fore.GREEN),
    ('baz', Fore.BLUE),
    ('hjkl', Fore.RED),
    ('qwerty', '')
])
def test_color_conditions(input_str, color):
    assert color_conditions(
        input_str,
        green=any_of('foo', 'bar'),
        blue=any_of('baz', 'asdf'),
        red=any_of('hjkl',)
    ) == color + input_str + Style.RESET_ALL


def test_parse_time_string_without_tz():
    t = parse_time_string('2017-08-01 00:00', tz='US/Eastern')
    assert t.timestamp == 1501560000


def test_parse_time_string_with_tz():
    # Ignore the tz argument here and use the '+04:00' in the string
    t = parse_time_string('2017-08-01T00:00:00+04:00', tz='US/Eastern')
    assert t.timestamp == 1501531200


def test_parse_time_string_non_arrow():
    t = parse_time_string('one hour ago', tz='US/Eastern')

    # This has potential to be a little flaky so there's a little wiggle room here
    actual_timestamp = arrow.now().replace(tzinfo='US/Eastern').shift(hours=-1).timestamp
    assert abs(actual_timestamp - t.timestamp) <= 1


def test_parse_time_interval_seconds():
    assert parse_time_interval_seconds('5m') == 60 * 5


def test_parse_time_interval_seconds_invalid():
    with pytest.raises(ValueError):
        parse_time_interval_seconds('asdf')


@mock.patch('clusterman.util._get_sensu')
class TestSensu:
    def _sensu_output(self, output, source, pool=None, app=None, scheduler=None):
        return ''.join([
            f'{output}\n\n',
            'This check came from:\n',
            f'- Cluster/region: {source}\n',
            f'- Pool: {pool}.{scheduler}\n' if pool else '',
            f'- App: {app}\n' if app else '',
        ])

    @pytest.mark.parametrize('noop', [True, False])
    def test_sensu_checkin(self, mock_sensu, noop):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            noop=noop,
        )

        if noop:
            assert mock_sensu.return_value.send_event.call_count == 0
        else:
            assert mock_sensu.return_value.send_event.call_args == mock.call(
                name='my_check',
                output=self._sensu_output('output', 'my_source'),
                source='my_source',
                status=Status.OK.value,
                runbook='y/my-runbook',
                team='my_team',
                page=True,
            )

    @pytest.mark.parametrize('app,pool,scheduler', [(None, None, None), ('bar', 'foo', 'mesos')])
    def test_args_overrides_config(self, mock_sensu, app, pool, scheduler):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            team='a_different_team',
            app=app,
            pool=pool,
            scheduler=scheduler,
        )

        assert mock_sensu.return_value.send_event.call_args == mock.call(
            name='my_check',
            source='my_source',
            output=self._sensu_output('output', 'my_source', pool, app, scheduler),
            status=Status.OK.value,
            runbook='y/my-runbook' if not app else 'y/their-runbook',
            team='a_different_team',
            page=True,
        )

    def test_fallback(self, mock_sensu):
        sensu_checkin(
            check_name='my_check',
            output='output',
            source='my_source',
            app='non-existent',
        )
        assert mock_sensu.return_value.send_event.call_args == mock.call(
            name='my_check',
            output=self._sensu_output('output', 'my_source', app='non-existent'),
            source='my_source',
            status=Status.OK.value,
            runbook='y/my-runbook',
            team='my_team',
            page=True,
        )


def test_get_cluster_name_list():
    with staticconf.testing.MockConfiguration(
        {
            'clusters': {
                'cluster-A': {
                    'mesos_api_url': 'service.leader',
                },
                'cluster-B': {
                    'mesos_api_url': 'service.leader',
                },
            },
        },
        namespace=staticconf.config.DEFAULT,
    ):
        assert set(get_cluster_name_list()) == {'cluster-A', 'cluster-B'}


@mock.patch('clusterman.util.get_cluster_config_directory')
@mock.patch('os.listdir')
def test_get_pool_name_list(mock_listdir, mock_get_cluster_config_directory):
    mock_get_cluster_config_directory.return_value = '/tmp/somedir/cluster-A'
    mock_listdir.return_value = ['pool-A.mesos', 'pool-B.xml', 'pool-C.mesos', 'pool-D', 'pool-F.kubernetes']
    assert set(get_pool_name_list('cluster-A', 'mesos')) == {'pool-A', 'pool-C'}
    assert set(get_pool_name_list('cluster-A', 'kubernetes')) == {'pool-F'}
    assert mock_get_cluster_config_directory.call_args == mock.call('cluster-A')
    assert mock_listdir.call_args == mock.call('/tmp/somedir/cluster-A')


def test_is_paused_no_data_for_cluster():
    with mock.patch('clusterman.util.dynamodb') as mock_dynamo:
        mock_dynamo.get_item.return_value = {'ResponseMetadata': {'foo': 'asdf'}}
        assert not autoscaling_is_paused('mesos-test', 'bar', 'mesos', arrow.get(300))


@pytest.mark.parametrize('exp_timestamp', [None, '100', '400'])
def test_is_paused_with_expiration_timestamp(exp_timestamp):
    with mock.patch('clusterman.util.dynamodb') as mock_dynamo:
        mock_dynamo.get_item.return_value = {
            'ResponseMetadata': {'foo': 'asdf'},
            'Item': {
                'state': {'S': 'autoscaler_paused'},
                'entity': {'S': 'mesos-test.bar.mesos'},
            }
        }
        if exp_timestamp:
            mock_dynamo.get_item.return_value['Item']['expiration_timestamp'] = {'N': exp_timestamp}
        assert autoscaling_is_paused('mesos-test', 'bar', 'mesos', arrow.get(300)) == (
            not exp_timestamp or
            int(exp_timestamp) > 300
        )
