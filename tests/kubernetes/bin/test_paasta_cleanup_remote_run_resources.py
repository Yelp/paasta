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
from datetime import datetime
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

from paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources import (
    clean_namespace,
)
from paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources import main


def _create_mock_kube_resource(name: str, creation_time: datetime):
    mock_resource = MagicMock()
    mock_resource.metadata.name = name
    mock_resource.metadata.creation_timestamp = creation_time
    return mock_resource


@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_remote_run_jobs",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_remote_run_role_bindings",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_remote_run_roles",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_remote_run_service_accounts",
    autospec=True,
)
def test_clean_namespace(mock_get_sa, mock_get_roles, mock_get_bindings, mock_get_jobs):
    mock_client = MagicMock()
    mock_get_sa.return_value = [
        _create_mock_kube_resource("foobar", datetime(2025, 1, 1, 0, 0, 0)),
        _create_mock_kube_resource("remote-run-abc", datetime(2025, 1, 1, 0, 0, 0)),
    ]
    mock_get_roles.return_value = [
        _create_mock_kube_resource("remote-run-abc", datetime(2025, 1, 1, 2, 0, 0)),
        _create_mock_kube_resource("remote-run-def", datetime(2025, 1, 1, 0, 0, 0)),
    ]
    mock_get_bindings.return_value = [
        _create_mock_kube_resource("whatever", datetime(2025, 1, 1, 0, 0, 0)),
    ]
    mock_get_jobs.return_value = [
        _create_mock_kube_resource(
            "remote-run-who-what", datetime(2024, 12, 1, 0, 0, 0)
        ),
    ]
    clean_namespace(
        mock_client, "abc", datetime(2025, 1, 1, 1, 1, 1), datetime(2025, 1, 1, 1, 1, 1)
    )
    mock_client.core.delete_namespaced_service_account.assert_has_calls(
        [call("remote-run-abc", "abc")]
    )
    mock_client.rbac.delete_namespaced_role.assert_has_calls(
        [call("remote-run-def", "abc")]
    )
    mock_client.rbac.delete_namespaced_role_binding.assert_not_called()
    mock_client.batches.delete_namespaced_job.assert_called_once_with(
        "remote-run-who-what", "abc"
    )


@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_max_job_duration_limit",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.get_all_managed_namespaces",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.clean_namespace",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.KubeClient",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.parse_args",
    autospec=True,
)
@patch(
    "paasta_tools.kubernetes.bin.paasta_cleanup_remote_run_resources.datetime",
    autospec=True,
)
def test_main(
    mock_datetime,
    mock_parse_args,
    mock_kube,
    mock_clean,
    mock_get_namespaces,
    mock_get_max_duration,
):
    mock_parse_args.return_value.max_age = 60
    mock_get_max_duration.return_value = 3600
    mock_parse_args.return_value.dry_run = False
    mock_datetime.now.return_value = datetime(2025, 1, 1, 0, 1, 0)
    mock_get_namespaces.return_value = ["a", "b", "c"]
    main()
    mock_clean.assert_has_calls(
        [
            call(
                mock_kube.return_value,
                "a",
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2024, 12, 31, 23, 1),
                False,
            ),
            call(
                mock_kube.return_value,
                "b",
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2024, 12, 31, 23, 1),
                False,
            ),
            call(
                mock_kube.return_value,
                "c",
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2024, 12, 31, 23, 1),
                False,
            ),
        ]
    )
