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
import mock

from paasta_tools import generate_deployments_for_service
from paasta_tools.long_running_service_tools import LongRunningServiceConfig


def test_get_deploy_group_mappings():
    fake_service = "fake_service"
    fake_soa_dir = "/no/yes/maybe"

    fake_service_configs = [
        LongRunningServiceConfig(
            service=fake_service,
            cluster="clusterA",
            instance="main",
            branch_dict=None,
            config_dict={"deploy_group": "no_thanks"},
        ),
        LongRunningServiceConfig(
            service=fake_service,
            cluster="clusterB",
            instance="main",
            branch_dict=None,
            config_dict={"deploy_group": "try_me"},
        ),
        LongRunningServiceConfig(
            service=fake_service,
            cluster="clusterC",
            instance="main",
            branch_dict=None,
            config_dict={"deploy_group": "but-why"},
        ),
    ]

    fake_remote_refs = {
        "refs/tags/paasta-try_me-20160308T053933-deploy": "123456",
        "refs/tags/paasta-clusterB.main-123-stop": "123456",
        "refs/tags/paasta-okay-20160308T053933-deploy": "ijowarg",
        "refs/tags/paasta-no_thanks-20160308T053933-deploy": "789009",
        "refs/tags/paasta-nah-20160308T053933-deploy": "j8yiomwer",
        "refs/tags/paasta-but-why+extrastuff-20220308T053933-deploy": "123456",
    }

    expected = {
        "fake_service:paasta-clusterA.main": {
            "docker_image": "services-fake_service:paasta-789009",
            "desired_state": "start",
            "force_bounce": None,
        },
        "fake_service:paasta-clusterB.main": {
            "docker_image": "services-fake_service:paasta-123456",
            "desired_state": "stop",
            "force_bounce": "123",
        },
        "fake_service:paasta-clusterC.main": {
            "docker_image": "services-fake_service:paasta-123456-extrastuff",
            "desired_state": "start",
            "force_bounce": None,
        },
    }
    expected_v2 = {
        "deployments": {
            "try_me": {
                "docker_image": "services-fake_service:paasta-123456",
                "git_sha": "123456",
                "image_version": None,
            },
            "no_thanks": {
                "docker_image": "services-fake_service:paasta-789009",
                "git_sha": "789009",
                "image_version": None,
            },
            "but-why": {
                "docker_image": "services-fake_service:paasta-123456-extrastuff",
                "git_sha": "123456",
                "image_version": "extrastuff",
            },
        },
        "controls": {
            "fake_service:clusterA.main": {
                "desired_state": "start",
                "force_bounce": None,
            },
            "fake_service:clusterB.main": {
                "desired_state": "stop",
                "force_bounce": "123",
            },
            "fake_service:clusterC.main": {
                "desired_state": "start",
                "force_bounce": None,
            },
        },
    }
    with mock.patch(
        "paasta_tools.generate_deployments_for_service.get_instance_configs_for_service",
        return_value=fake_service_configs,
        autospec=True,
    ) as get_instance_configs_for_service_patch, mock.patch(
        "paasta_tools.remote_git.list_remote_refs",
        return_value=fake_remote_refs,
        autospec=True,
    ) as list_remote_refs_patch:
        actual, actual_v2 = generate_deployments_for_service.get_deploy_group_mappings(
            fake_soa_dir, fake_service
        )
        get_instance_configs_for_service_patch.assert_called_once_with(
            soa_dir=fake_soa_dir, service=fake_service
        )
        assert list_remote_refs_patch.call_count == 1
        assert expected == actual
        assert expected_v2 == actual_v2


def test_main():
    fake_soa_dir = "/etc/true/null"
    file_mock = mock.mock_open()
    with mock.patch(
        "paasta_tools.generate_deployments_for_service.parse_args",
        return_value=mock.Mock(
            verbose=False, soa_dir=fake_soa_dir, service="fake_service"
        ),
        autospec=True,
    ) as parse_patch, mock.patch(
        "os.path.abspath", return_value="ABSOLUTE", autospec=True
    ) as abspath_patch, mock.patch(
        "paasta_tools.generate_deployments_for_service.get_deploy_group_mappings",
        return_value=(
            {"MAP": {"docker_image": "PINGS", "desired_state": "start"}},
            mock.sentinel.v2_mappings,
        ),
        autospec=True,
    ) as mappings_patch, mock.patch(
        "os.path.join", return_value="JOIN", autospec=True
    ) as join_patch, mock.patch(
        "builtins.open", file_mock, autospec=None
    ) as open_patch, mock.patch(
        "json.dump", autospec=True
    ) as json_dump_patch, mock.patch(
        "json.load", return_value={"OLD_MAP": "PINGS"}, autospec=True
    ) as json_load_patch, mock.patch(
        "paasta_tools.generate_deployments_for_service.atomic_file_write", autospec=True
    ) as atomic_file_write_patch:
        generate_deployments_for_service.main()
        parse_patch.assert_called_once_with()
        abspath_patch.assert_called_once_with(fake_soa_dir)
        mappings_patch.assert_called_once_with(
            soa_dir="ABSOLUTE", service="fake_service"
        ),

        join_patch.assert_any_call(
            "ABSOLUTE", "fake_service", generate_deployments_for_service.TARGET_FILE
        ),
        assert join_patch.call_count == 2

        atomic_file_write_patch.assert_called_once_with("JOIN")
        open_patch.assert_called_once_with("JOIN", "r")
        json_dump_patch.assert_called_once_with(
            {
                "v1": {"MAP": {"docker_image": "PINGS", "desired_state": "start"}},
                "v2": mock.sentinel.v2_mappings,
            },
            atomic_file_write_patch.return_value.__enter__.return_value,
        )
        json_load_patch.assert_called_once_with(
            file_mock.return_value.__enter__.return_value
        )

        # test no update to file if content unchanged
        json_load_patch.return_value = {
            "v1": {"MAP": {"docker_image": "PINGS", "desired_state": "start"}},
            "v2": mock.sentinel.v2_mappings,
        }
        json_dump_patch.reset_mock()
        generate_deployments_for_service.main()
        assert not json_dump_patch.called

        # test IOError path
        open_patch.side_effect = IOError
        generate_deployments_for_service.main()
        assert json_dump_patch.called


def test_get_deployments_dict():
    branch_mappings = {
        "app1": {
            "docker_image": "image1",
            "desired_state": "start",
            "force_bounce": "1418951213",
        },
        "app2": {
            "docker_image": "image2",
            "desired_state": "stop",
            "force_bounce": "1412345678",
        },
    }

    v2_mappings = mock.sentinel.v2_mappings

    assert generate_deployments_for_service.get_deployments_dict_from_deploy_group_mappings(
        branch_mappings, v2_mappings
    ) == {
        "v1": branch_mappings,
        "v2": mock.sentinel.v2_mappings,
    }


def test_get_desired_state_understands_tags():
    remote_refs = {
        "refs/heads/master": "7894E99E6805E9DC8C1D8EB26229E3E2243878C9",
        "refs/remotes/origin/HEAD": "EE8796C4E4295B7D4087E3EB73662B99218DAD94",
        "refs/remotes/origin/master": "5F7C10B320A4EDBC4773C5FEFB1CD7B7A84FCB69",
        "refs/tags/paasta-paasta-cluster.instance-20150721T183905-start": "4EF01B5A574B519AB546309E89F72972A33B6B75",
        "refs/tags/paasta-paasta-cluster.instance-20151106T233211-stop": "A5AB2A012DC238D4F6DD269C40A4BD3A99D52B1F",
        "refs/tags/paasta-cluster.instance-20160202T233805-start": "BE68473F98F619F26FD7824B8F56F9A7ABAEB860",
        "refs/tags/paasta-cluster2.someinstance-20160202T233805-start": "D6B9A0F86DC54A132FBB7747460F53F48C9AEEAD",
        "refs/tags/paasta-cluster2.someinstance-20160205T182601-stop": "9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711",
        "refs/tags/paasta-cluster.instance-20160308T053933-deploy": "4EF01B5A574B519AB546309E89F72972A33B6B75",
        "refs/tags/paasta-cluster2.someinstance-20160308T053933-deploy": "9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711",
    }
    branch = "cluster2.someinstance"
    sha = "9085FD67ED1BB5FADAFA7F2AFAF8DEDEE7342711"
    expected_desired_state = ("stop", "20160205T182601")
    actual = generate_deployments_for_service.get_desired_state_by_branch_and_sha(
        remote_refs
    )[(branch, sha)]

    assert actual == expected_desired_state
