# Copyright 2015 Yelp Inc.
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
import argparse
import contextlib
import datetime
from StringIO import StringIO

from mock import MagicMock
from mock import patch
from pytest import mark

from paasta_tools.chronos_tools import EXECUTION_DATE_FORMAT
from paasta_tools.cli.cmds.rerun import add_subparser
from paasta_tools.cli.cmds.rerun import paasta_rerun


_user_supplied_execution_date = '2016-04-08T02:37:27'
_list_clusters = ['cluster1', 'cluster2']
_actual_deployments = {'cluster1.instance1': 'this_is_a_sha'}
_planned_deployments = ['cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1']


@mark.parametrize('test_case', [
    [
        ['a_service', 'instance1', 'cluster1', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'successfully created job',
    ],
    [
        ['a_service', 'instance1', 'cluster1', None],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'successfully created job',
    ],
    [
        ['a_service', 'instance1', 'cluster1', None],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, True,
        'please supply a `--execution_date` argument',  # job uses time variables interpolation
    ],
    [
        ['a_service', 'instance1', 'cluster1,cluster2', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'successfully created job',
    ],
    [
        ['a_service', 'instance1', None, _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'cluster: cluster1',  # success
    ],
    [
        ['a_service', 'instance1', 'cluster3', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        '"cluster3" does not look like a valid cluster',
    ],
    [
        ['a_service', 'instance1', 'cluster2', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'service "a_service" has not been deployed to "cluster2" yet',
    ],
    [
        ['a_service', 'instanceX', 'cluster1', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        'instance "instanceX" is either invalid',
    ],
    [
        ['a_service', 'instance2', 'cluster1', _user_supplied_execution_date],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments, False,
        ' or has not been deployed to "cluster1" yet',
    ]
])
def test_rerun_validations(test_case):
    with contextlib.nested(
        patch('sys.stdout', new_callable=StringIO),
        patch('paasta_tools.cli.cmds.rerun.figure_out_service_name', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.list_clusters', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.get_actual_deployments', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.get_planned_deployments', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.execute_chronos_rerun_on_remote_master', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.chronos_tools.load_chronos_job_config', autospec=True),
        patch('paasta_tools.cli.cmds.rerun.chronos_tools.uses_time_variables', autospec=True),
        patch('paasta_tools.cli.cmds.rerun._get_default_execution_date', autospec=True),
    ) as (
        mock_stdout,
        mock_figure_out_service_name,
        mock_list_clusters,
        mock_get_actual_deployments,
        mock_get_planned_deployments,
        mock_execute_rerun_remote,
        mock_load_chronos_job_config,
        mock_uses_time_variables,
        mock_get_default_execution_date,
    ):

        (rerun_args,
         mock_figure_out_service_name.return_value,
         mock_list_clusters.return_value,
         mock_get_actual_deployments.return_value,
         mock_get_planned_deployments.return_value,
         mock_uses_time_variables.return_value,
         expected_output) = test_case

        mock_load_chronos_job_config.return_value = {}
        default_date = datetime.datetime(2002, 2, 2, 2, 2, 2, 2)
        mock_get_default_execution_date.return_value = default_date
        mock_execute_rerun_remote.return_value = (0, '')

        args = MagicMock()
        args.service = rerun_args[0]
        args.instance = rerun_args[1]
        args.clusters = rerun_args[2]
        if rerun_args[3]:
            args.execution_date = datetime.datetime.strptime(rerun_args[3], EXECUTION_DATE_FORMAT)
        else:
            args.execution_date = None
        args.verbose = 0

        paasta_rerun(args)

        # No --execution_date argument, but that's ok: the job doesn't use time vars interpolation.
        # Check if the backend rerun command was called with the default date.
        if args.execution_date is None and not mock_uses_time_variables.return_value:
            assert mock_execute_rerun_remote.call_args[1]['execution_date'] \
                == default_date.strftime(EXECUTION_DATE_FORMAT)

        # The job does use time vars interpolation. Make sure the User supplied date was used.
        if args.execution_date is not None and mock_uses_time_variables.return_value:
            assert mock_execute_rerun_remote.call_args[1]['execution_date'] == _user_supplied_execution_date

        output = mock_stdout.getvalue()
        assert expected_output in output


@mark.parametrize('test_case', [
    [['rerun'], True],
    [['rerun', '-s', 'a_service'], True],
    [['rerun', '-s', 'a_service', '-i', 'an_instance'], False],
    [['rerun', '-s', 'a_service', '-i', 'an_instance', '-d', _user_supplied_execution_date], False],
    [['rerun', '-s', 'a_service', '-i', 'an_instance', '-d', 'not_a_date'], True],
    [['rerun', '-v', '-v', '-s', 'a_service', '-i', 'an_instance', '-d', _user_supplied_execution_date], False],
])
def test_rerun_argparse(test_case):
    argv, should_exit = test_case
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_subparser(subparsers)

    exited = False
    rc = None
    args = None
    try:
        args = parser.parse_args(argv)
    except SystemExit as sys_exit:
        exited = True
        rc = sys_exit.code

    assert exited == should_exit
    if should_exit:
        assert rc == 2

    if args:
        if args.verbose:
            assert args.verbose == 2  # '-v' yields a verbosity level...
        if args.execution_date:
            assert isinstance(args.execution_date, datetime.datetime)
