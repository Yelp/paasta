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

from paasta_tools.cli.cmds.rerun import add_subparser
from paasta_tools.cli.cmds.rerun import EXECUTION_DATE_FORMAT
from paasta_tools.cli.cmds.rerun import paasta_rerun


_list_clusters = ['cluster1', 'cluster2']
_actual_deployments = {'cluster1.instance1': 'this_is_a_sha'}
_planned_deployments = ['cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1']


@mark.parametrize('test_case', [
    [
        ['a_service', 'instance1', 'cluster1', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        'success',
    ],
    [
        ['a_service', 'instance1', 'cluster1,cluster2', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        'success',
    ],
    [
        ['a_service', 'instance1', None, '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        'cluster: cluster1',  # success
    ],
    [
        ['a_service', 'instance1', 'cluster3', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        '"cluster3" does not look like a valid cluster',
    ],
    [
        ['a_service', 'instance1', 'cluster2', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        'service "a_service" has not been deployed to "cluster2" yet',
    ],
    [
        ['a_service', 'instanceX', 'cluster1', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
        'instance "instanceX" is either invalid',
    ],
    [
        ['a_service', 'instance2', 'cluster1', '2016-04-08T02:37:27'],
        'a_service',
        _list_clusters, _actual_deployments, _planned_deployments,
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
    ) as (
        mock_stdout,
        mock_figure_out_service_name,
        mock_list_clusters,
        mock_get_actual_deployments,
        mock_get_planned_deployments,
        mock_execute_rerun_remote,
    ):

        (rerun_args,
         mock_figure_out_service_name.return_value,
         mock_list_clusters.return_value,
         mock_get_actual_deployments.return_value,
         mock_get_planned_deployments.return_value,
         expected_output) = test_case

        mock_execute_rerun_remote.return_value = (0, '')

        args = MagicMock()
        args.service = rerun_args[0]
        args.instance = rerun_args[1]
        args.clusters = rerun_args[2]
        args.execution_date = datetime.datetime.strptime(rerun_args[3], EXECUTION_DATE_FORMAT)
        args.verbose = 0

        paasta_rerun(args)

        output = mock_stdout.getvalue()
        assert expected_output in output


@mark.parametrize('test_case', [
    [['rerun'], True],
    [['rerun', '-s', 'a_service'], True],
    [['rerun', '-s', 'a_service', '-i', 'an_instance'], True],
    [['rerun', '-s', 'a_service', '-i', 'an_instance', '-d', '2016-04-08T02:37:27'], False],
    [['rerun', '-s', 'a_service', '-i', 'an_instance', '-d', 'not_a_date'], True],
    [['rerun', '-v', '-v', '-s', 'a_service', '-i', 'an_instance', '-d', '2016-04-08T02:37:27'], False],
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
        assert isinstance(args.execution_date, datetime.datetime)
