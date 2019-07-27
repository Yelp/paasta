# Copyright 2015-2016 Yelp Inc.
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
import datetime

from mock import MagicMock
from mock import mock
from mock import patch
from pytest import mark

from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.chronos_tools import EXECUTION_DATE_FORMAT
from paasta_tools.cli.cmds.rerun import add_subparser
from paasta_tools.cli.cmds.rerun import paasta_rerun


_user_supplied_execution_date = "2016-04-08T02:37:27"
_list_clusters = ["cluster1", "cluster2"]
_actual_deployments = {
    "cluster1.instance1": "this_is_a_sha",
    "cluster1.dependent_instance1": "this_is_a_sha",
    "cluster1.dependent_instance2": "this_is_a_sha",
}
_planned_deployments = [
    "cluster1.instance1",
    "cluster1.instance2",
    "cluster2.instance1",
]
_service_name = "a_service"


@mark.parametrize(
    "test_case",
    [
        [
            [
                _service_name,
                "instance1",
                "cluster1",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [_service_name, "instance1", "cluster1", None, None, None],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [_service_name, "instance1", "cluster1", None, None, None],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            True,
            "please supply a `--execution_date` argument",
            False,  # job uses time variables interpolation
        ],
        [
            [
                _service_name,
                "instance1",
                "cluster1,cluster2",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [
                _service_name,
                "instance1",
                None,
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "cluster: cluster1",
            True,  # success
        ],
        [
            [
                _service_name,
                "instance1",
                "cluster3",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            '"cluster3" does not look like a valid cluster',
            False,
        ],
        [
            [
                _service_name,
                "instance1",
                "cluster2",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            f'service "{_service_name}" has not been deployed to "cluster2" yet',
            False,
        ],
        [
            [
                _service_name,
                "instanceX",
                "cluster1",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            'instance "instanceX" is either invalid',
            False,
        ],
        [
            [
                _service_name,
                "dependent_instance1",
                "cluster1",
                _user_supplied_execution_date,
                None,
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "Please specify the rerun policy via --rerun-type argument",
            False,
        ],
        [
            [
                _service_name,
                "dependent_instance1",
                "cluster1",
                _user_supplied_execution_date,
                "instance",
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [
                _service_name,
                "dependent_instance2",
                "cluster1",
                _user_supplied_execution_date,
                "graph",
                None,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,  # TODO: make assertion more selective -> check started instances
        ],
        [
            [
                _service_name,
                "instance1",
                "cluster1",
                _user_supplied_execution_date,
                None,
                True,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [
                _service_name,
                "instance1",
                "cluster1",
                _user_supplied_execution_date,
                None,
                False,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [
                _service_name,
                "dependent_instance1",
                "cluster1",
                _user_supplied_execution_date,
                "instance",
                True,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
        [
            [
                _service_name,
                "dependent_instance2",
                "cluster1",
                _user_supplied_execution_date,
                "graph",
                True,
            ],
            _service_name,
            _list_clusters,
            _actual_deployments,
            _planned_deployments,
            False,
            "successfully created job",
            True,
        ],
    ],
)
def test_rerun_validations(test_case, capfd, system_paasta_config):
    with patch(
        "paasta_tools.cli.cmds.rerun.figure_out_service_name", autospec=True
    ) as mock_figure_out_service_name, patch(
        "paasta_tools.cli.cmds.rerun.list_clusters", autospec=True
    ) as mock_list_clusters, patch(
        "paasta_tools.cli.cmds.rerun.get_actual_deployments", autospec=True
    ) as mock_get_actual_deployments, patch(
        "paasta_tools.cli.cmds.rerun.get_planned_deployments", autospec=True
    ) as mock_get_planned_deployments, patch(
        "paasta_tools.cli.cmds.rerun.execute_chronos_rerun_on_remote_master",
        autospec=True,
    ) as mock_execute_rerun_remote, patch(
        "paasta_tools.cli.cmds.rerun.chronos_tools.load_chronos_job_config",
        autospec=True,
    ) as mock_load_chronos_job_config, patch(
        "paasta_tools.cli.cmds.rerun.chronos_tools.uses_time_variables", autospec=True
    ) as mock_uses_time_variables, patch(
        "paasta_tools.cli.cmds.rerun._get_default_execution_date", autospec=True
    ) as mock_get_default_execution_date, patch(
        "paasta_tools.cli.cmds.rerun.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.cli.cmds.rerun._log_audit", autospec=True
    ) as mock_log_audit, patch(
        "paasta_tools.chronos_tools.read_chronos_jobs_for_service", autospec=True
    ) as mock_read_chronos_jobs_for_service, patch(
        "service_configuration_lib.read_services_configuration", autospec=True
    ) as mock_read_services_configuration:
        (
            rerun_args,
            mock_figure_out_service_name.return_value,
            mock_list_clusters.return_value,
            mock_get_actual_deployments.return_value,
            mock_get_planned_deployments.return_value,
            mock_uses_time_variables.return_value,
            expected_output,
            call_execute_rerun_remote,
        ) = test_case

        def fake_load_chronos_jobs_config(service, instance, cluster, *args, **kwargs):
            mock = MagicMock(spec=ChronosJobConfig)
            if instance == "dependent_instance2":
                mock.get_parents.return_value = [
                    "{}.{}".format(_service_name, "dependent_instance1")
                ]
            else:
                mock.get_parents.return_value = []
            return mock

        mock_load_chronos_job_config.side_effect = fake_load_chronos_jobs_config
        default_date = datetime.datetime(2002, 2, 2, 2, 2, 2, 2)
        mock_get_default_execution_date.return_value = default_date
        mock_execute_rerun_remote.return_value = (0, "")
        mock_load_system_paasta_config.return_value = system_paasta_config

        mock_read_chronos_jobs_for_service.return_value = {
            "instance1": {},
            "dependent_instance1": {},
            "dependent_instance2": {
                "parents": ["{}.{}".format(_service_name, "dependent_instance1")]
            },
        }

        mock_read_services_configuration.return_value = [_service_name]

        args = MagicMock()
        args.service = rerun_args[0]
        args.instance = rerun_args[1]
        args.clusters = rerun_args[2]
        if rerun_args[3]:
            args.execution_date = datetime.datetime.strptime(
                rerun_args[3], EXECUTION_DATE_FORMAT
            )
        else:
            args.execution_date = None
        args.rerun_type = rerun_args[4]
        args.force_disabled = rerun_args[5]
        args.verbose = 0

        paasta_rerun(args)

        # No --execution_date argument, but that's ok: the job doesn't use time vars interpolation.
        # Check if the backend rerun command was called with the default date.
        if args.execution_date is None and not mock_uses_time_variables.return_value:
            assert mock_execute_rerun_remote.call_args[1][
                "execution_date"
            ] == default_date.strftime(EXECUTION_DATE_FORMAT)

        if call_execute_rerun_remote:
            execution_date = (
                args.execution_date if args.execution_date else default_date
            )
            mock_execute_rerun_remote.assert_called_once_with(
                service=args.service,
                instancename=args.instance,
                cluster=mock.ANY,
                verbose=args.verbose,
                execution_date=execution_date.strftime(EXECUTION_DATE_FORMAT),
                run_all_related_jobs=bool(
                    args.rerun_type and args.rerun_type == "graph"
                ),
                force_disabled=args.force_disabled,
                system_paasta_config=mock_load_system_paasta_config.return_value,
            )

            mock_log_audit.assert_called_once_with(
                action="chronos-rerun",
                action_details={
                    "rerun_type": args.rerun_type,
                    "execution_date": execution_date.strftime(EXECUTION_DATE_FORMAT),
                },
                service=args.service,
                cluster=mock.ANY,
                instance=args.instance,
            )

        # The job does use time vars interpolation. Make sure the User supplied date was used.
        # TODO: this if statement is never true
        # if args.execution_date is not None and mock_uses_time_variables.return_value:
        #    assert mock_execute_rerun_remote.call_args[1]['execution_date'] == _user_supplied_execution_date

        output, _ = capfd.readouterr()
        assert expected_output in output


@mark.parametrize(
    "test_case",
    [
        [["rerun"], True],
        [["rerun", "-s", _service_name], True],
        [["rerun", "-s", _service_name, "-i", "an_instance"], False],
        [
            [
                "rerun",
                "-s",
                _service_name,
                "-i",
                "an_instance",
                "-d",
                _user_supplied_execution_date,
            ],
            False,
        ],
        [["rerun", "-s", _service_name, "-i", "an_instance", "-d", "not_a_date"], True],
        [
            [
                "rerun",
                "-v",
                "-v",
                "-s",
                _service_name,
                "-i",
                "an_instance",
                "-d",
                _user_supplied_execution_date,
            ],
            False,
        ],
        [
            [
                "rerun",
                "-s",
                _service_name,
                "-i",
                "an_instance",
                "-t",
                "not_a_valid_type",
            ],
            True,
        ],
        [["rerun", "-s", _service_name, "-i", "an_instance", "-t", "instance"], False],
        [["rerun", "-s", _service_name, "-i", "an_instance", "-t", "graph"], False],
        [["rerun", "-s", _service_name, "-i", "an_instance", "-f"], False],
        [
            ["rerun", "-s", _service_name, "-i", "an_instance", "-t", "graph", "-f"],
            False,
        ],
    ],
)
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
