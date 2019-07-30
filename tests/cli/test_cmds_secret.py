# Copyright 2015-2018 Yelp Inc.
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
from pytest import raises

from paasta_tools.cli.cmds import secret


def test_add_subparser():
    mock_subparsers = mock.Mock()
    secret.add_subparser(mock_subparsers)
    assert mock_subparsers.add_parser.called
    mock_subparsers.add_parser.return_value.set_defaults.assert_called_with(
        command=secret.paasta_secret
    )


def test_secret_name_for_env():
    assert secret.secret_name_for_env("test-secret2") == "TEST_SECRET2"
    assert secret.secret_name_for_env("test.secret.foo") == "TEST_SECRET_FOO"


def test_print_paasta_helper():
    secret.print_paasta_helper("/blah/what", "keepithidden", False)
    secret.print_paasta_helper("/blah/what", "keepithidden", True)


def test_get_plaintext_input():
    with mock.patch("sys.stdin", autospec=True) as mock_stdin, mock.patch(
        "paasta_tools.cli.cmds.secret.input", autospec=False
    ) as mock_input:
        mock_args = mock.Mock(plain_text=False, stdin=True)
        mock_stdin.buffer.read.return_value = b"SECRET_SQUIRREL"
        assert secret.get_plaintext_input(mock_args) == b"SECRET_SQUIRREL"

        mock_args = mock.Mock(plain_text="SECRET_CAT", stdin=False)
        assert secret.get_plaintext_input(mock_args) == b"SECRET_CAT"

        mock_args = mock.Mock(plain_text=False, stdin=False)
        mock_input.side_effect = ["DANGER_DOG", EOFError]
        assert secret.get_plaintext_input(mock_args) == b"DANGER_DOG"


def test_is_service_folder():
    with mock.patch("os.path.isfile", autospec=True) as mock_is_file:
        mock_is_file.return_value = True
        assert secret.is_service_folder(soa_dir="/nail", service_name="universe")
        mock_is_file.assert_called_with("/nail/universe/service.yaml")

        mock_is_file.return_value = False
        assert not secret.is_service_folder(soa_dir="/nail", service_name="universe")


def test__get_secret_provider_for_service():
    with mock.patch("os.getcwd", autospec=True) as mock_getcwd, mock.patch(
        "paasta_tools.cli.cmds.secret.is_service_folder", autospec=True
    ) as mock_is_service_folder, mock.patch(
        "paasta_tools.cli.cmds.secret.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.cli.cmds.secret.list_clusters", autospec=True
    ) as mock_list_clusters, mock.patch(
        "paasta_tools.cli.cmds.secret.get_secret_provider", autospec=True
    ) as mock_get_secret_provider:
        mock_config = mock.Mock()
        mock_load_system_paasta_config.return_value = mock_config
        mock_is_service_folder.return_value = False
        with raises(SystemExit):
            secret._get_secret_provider_for_service("universe")
        mock_is_service_folder.return_value = True

        ret = secret._get_secret_provider_for_service(
            "universe", cluster_names="mesosstage,norcal-devc"
        )
        assert ret == mock_get_secret_provider.return_value
        mock_get_secret_provider.assert_called_with(
            secret_provider_name=mock_config.get_secret_provider_name.return_value,
            soa_dir=mock_getcwd.return_value,
            service_name="universe",
            cluster_names=["mesosstage", "norcal-devc"],
            secret_provider_kwargs={
                "vault_cluster_config": mock_config.get_vault_cluster_config.return_value
            },
        )

        ret = secret._get_secret_provider_for_service("universe", cluster_names=None)
        assert ret == mock_get_secret_provider.return_value
        mock_get_secret_provider.assert_called_with(
            secret_provider_name=mock_config.get_secret_provider_name.return_value,
            soa_dir=mock_getcwd.return_value,
            service_name="universe",
            cluster_names=mock_list_clusters.return_value,
            secret_provider_kwargs={
                "vault_cluster_config": mock_config.get_vault_cluster_config.return_value
            },
        )


def test_paasta_secret():
    with mock.patch(
        "paasta_tools.cli.cmds.secret._get_secret_provider_for_service", autospec=True
    ) as mock_get_secret_provider_for_service, mock.patch(
        "paasta_tools.cli.cmds.secret.decrypt_secret", autospec=True
    ) as mock_decrypt_secret, mock.patch(
        "paasta_tools.cli.cmds.secret.get_plaintext_input", autospec=True
    ) as mock_get_plaintext_input, mock.patch(
        "paasta_tools.cli.cmds.secret._log_audit", autospec=True
    ) as mock_log_audit:
        mock_secret_provider = mock.Mock(secret_dir="/nail/blah")
        mock_get_secret_provider_for_service.return_value = mock_secret_provider
        mock_args = mock.Mock(
            action="add",
            secret_name="theonering",
            service="middleearth",
            clusters="mesosstage",
            shared=False,
        )
        secret.paasta_secret(mock_args)
        mock_get_secret_provider_for_service.assert_called_with(
            "middleearth", cluster_names="mesosstage"
        )
        mock_secret_provider.write_secret.assert_called_with(
            action="add",
            secret_name="theonering",
            plaintext=mock_get_plaintext_input.return_value,
        )
        mock_log_audit.assert_called_with(
            action="add-secret",
            action_details={"secret_name": "theonering", "clusters": "mesosstage"},
            service="middleearth",
        )

        mock_args = mock.Mock(
            action="update",
            secret_name="theonering",
            service="middleearth",
            clusters="mesosstage",
            shared=False,
        )
        secret.paasta_secret(mock_args)
        mock_get_secret_provider_for_service.assert_called_with(
            "middleearth", cluster_names="mesosstage"
        )
        mock_secret_provider.write_secret.assert_called_with(
            action="update",
            secret_name="theonering",
            plaintext=mock_get_plaintext_input.return_value,
        )
        mock_log_audit.assert_called_with(
            action="update-secret",
            action_details={"secret_name": "theonering", "clusters": "mesosstage"},
            service="middleearth",
        )

        mock_args = mock.Mock(
            action="decrypt",
            secret_name="theonering",
            service="middleearth",
            clusters="mesosstage",
            shared=False,
        )
        secret.paasta_secret(mock_args)
        mock_get_secret_provider_for_service.assert_called_with(
            "middleearth", cluster_names="mesosstage"
        )
        mock_decrypt_secret.assert_called_with(
            secret_provider=mock_secret_provider, secret_name="theonering"
        )

        mock_args = mock.Mock(
            action="add",
            secret_name="theonering",
            service=None,
            clusters="mesosstage",
            shared=True,
        )
        secret.paasta_secret(mock_args)
        mock_get_secret_provider_for_service.assert_called_with(
            secret.SHARED_SECRET_SERVICE, cluster_names="mesosstage"
        )
        mock_decrypt_secret.assert_called_with(
            secret_provider=mock_secret_provider, secret_name="theonering"
        )
        mock_log_audit.assert_called_with(
            action="add-secret",
            action_details={"secret_name": "theonering", "clusters": "mesosstage"},
            service="_shared",
        )

        mock_args = mock.Mock(
            action="add",
            secret_name="theonering",
            service=None,
            clusters=None,
            shared=True,
        )
        with raises(SystemExit):
            secret.paasta_secret(mock_args)


def test_decrypt_secret():
    mock_secret_provider = mock.Mock(cluster_names=["mesosstage", "devc"])
    with raises(SystemExit):
        secret.decrypt_secret(mock_secret_provider, "theonering")
    mock_secret_provider = mock.Mock(cluster_names=["mesosstage"])
    assert (
        secret.decrypt_secret(mock_secret_provider, "theonering")
        == mock_secret_provider.decrypt_secret.return_value
    )
    mock_secret_provider.decrypt_secret.assert_called_with("theonering")
