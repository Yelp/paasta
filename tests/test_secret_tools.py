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
from json.decoder import JSONDecodeError

import mock

from paasta_tools.secret_tools import decrypt_secret
from paasta_tools.secret_tools import decrypt_secret_environment_for_service
from paasta_tools.secret_tools import decrypt_secret_environment_variables
from paasta_tools.secret_tools import decrypt_secret_volumes
from paasta_tools.secret_tools import get_hmac_for_secret
from paasta_tools.secret_tools import get_secret_hashes
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import SecretVolume


def test_is_secret_ref():
    assert is_secret_ref("SECRET(aaa-bbb-222_111)")
    assert not is_secret_ref("SECRET(#!$)")
    # herein is a lesson on how tests are hard:
    assert not is_secret_ref("anything_else")
    assert not is_secret_ref("")
    # this is just incase a non string leaks in somewhere
    # if it is not a string it can't be a secret ref
    # so this checks that we are catching the TypeError
    assert not is_secret_ref(None)
    assert not is_secret_ref(3)  # type: ignore


def test_is_secret_ref_shared():
    assert is_secret_ref("SHARED_SECRET(foo)")


def test_get_secret_name_from_ref():
    assert get_secret_name_from_ref("SECRET(aaa-bbb-222_111)") == "aaa-bbb-222_111"


def test_get_shared_secret_name_from_ref():
    assert (
        get_secret_name_from_ref("SHARED_SECRET(aaa-bbb-222_111)") == "aaa-bbb-222_111"
    )


def test_get_hmac_for_secret():
    with mock.patch(
        "paasta_tools.secret_tools.open", autospec=False
    ) as mock_open, mock.patch(
        "json.load", autospec=True
    ) as mock_json_load, mock.patch(
        "paasta_tools.secret_tools.get_secret_name_from_ref", autospec=True
    ) as mock_get_secret_name_from_ref:
        mock_json_load.return_value = {
            "environments": {"dev": {"signature": "notArealHMAC"}}
        }
        mock_get_secret_name_from_ref.return_value = "secretsquirrel"

        ret = get_hmac_for_secret(
            "SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev"
        )
        mock_get_secret_name_from_ref.assert_called_with("SECRET(secretsquirrel)")
        mock_open.assert_called_with(
            "/nail/blah/service-name/secrets/secretsquirrel.json", "r"
        )
        assert ret == "notArealHMAC"

        ret = get_hmac_for_secret(
            "SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev-what"
        )
        assert ret is None

        mock_open.side_effect = IOError
        ret = get_hmac_for_secret(
            "SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev"
        )
        assert ret is None

        ret = get_hmac_for_secret(
            "SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev"
        )
        assert ret is None

        mock_open.side_effect = None
        mock_json_load.side_effect = JSONDecodeError("", "", 1)
        ret = get_hmac_for_secret(
            "SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev"
        )
        assert ret is None


def test_get_hmac_for_shared_secret():
    with mock.patch(
        "paasta_tools.secret_tools.open", autospec=False
    ) as mock_open, mock.patch(
        "json.load", autospec=True
    ) as mock_json_load, mock.patch(
        "paasta_tools.secret_tools.get_secret_name_from_ref", autospec=True
    ) as mock_get_secret_name_from_ref:
        mock_json_load.return_value = {
            "environments": {"dev": {"signature": "notArealHMAC"}}
        }
        mock_get_secret_name_from_ref.return_value = "secretsquirrel"

        ret = get_hmac_for_secret(
            "SHARED_SECRET(secretsquirrel)", "service-name", "/nail/blah", "dev"
        )
        mock_get_secret_name_from_ref.assert_called_with(
            "SHARED_SECRET(secretsquirrel)"
        )
        mock_open.assert_called_with(
            f"/nail/blah/{SHARED_SECRET_SERVICE}/secrets/secretsquirrel.json", "r"
        )
        assert ret == "notArealHMAC"


def test_get_secret_provider():
    with mock.patch(
        "paasta_tools.secret_providers.SecretProvider", autospec=True
    ) as mock_secret_provider:
        ret = get_secret_provider(
            secret_provider_name="paasta_tools.secret_providers",
            soa_dir="/nail/blah",
            service_name="test-service",
            cluster_names=["norcal-devc"],
            secret_provider_kwargs={"some": "thing"},
        )
        mock_secret_provider.assert_called_with(
            soa_dir="/nail/blah",
            service_name="test-service",
            cluster_names=["norcal-devc"],
            some="thing",
        )
        assert ret == mock_secret_provider.return_value


def test_get_secret_hashes():
    with mock.patch(
        "paasta_tools.secret_tools.is_secret_ref", autospec=True, return_value=False
    ) as mock_is_secret_ref, mock.patch(
        "paasta_tools.secret_tools.get_hmac_for_secret", autospec=True
    ) as mock_get_hmac_for_secret:
        env = {"SOME_VAR": "SOME_VAL"}

        assert get_secret_hashes(env, "dev", "service", DEFAULT_SOA_DIR) == {}
        mock_is_secret_ref.assert_called_with("SOME_VAL")
        assert not mock_get_hmac_for_secret.called

        mock_is_secret_ref.return_value = True
        expected = {"SOME_VAL": mock_get_hmac_for_secret.return_value}
        assert get_secret_hashes(env, "dev", "service", DEFAULT_SOA_DIR) == expected
        mock_is_secret_ref.assert_called_with("SOME_VAL")
        mock_get_hmac_for_secret.assert_called_with(
            env_var_val="SOME_VAL",
            service="service",
            soa_dir=DEFAULT_SOA_DIR,
            secret_environment="dev",
        )


@mock.patch("paasta_tools.secret_tools.is_secret_ref", autospec=True)
@mock.patch("paasta_tools.secret_tools.is_shared_secret", autospec=True)
@mock.patch(
    "paasta_tools.secret_tools.decrypt_secret_environment_for_service",
    autospec=True,
)
def test_decrypt_secret_environment_variables(
    mock_decrypt_for_service, mock_is_shared_secret, mock_is_secret_ref
):
    mock_environment = {
        "MY": "aaa",
        "SECRET": "SECRET(123)",
        "SECRET_SHARED": "SHARED_SECRET(abc)",
    }
    mock_is_secret_ref.side_effect = lambda val: "SECRET" in val
    mock_is_shared_secret.side_effect = lambda val: "SHARED" in val
    mock_decrypt_for_service.side_effect = [{"SECRET": "123"}, {"SECRET_SHARED": "abc"}]

    ret = decrypt_secret_environment_variables(
        secret_provider_name="vault",
        environment=mock_environment,
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
    )
    assert ret == {"SECRET": "123", "SECRET_SHARED": "abc"}

    assert mock_decrypt_for_service.call_args_list == [
        mock.call(
            {"SECRET": "SECRET(123)"},
            "universe",
            "vault",
            "/nail/blah",
            "mesosstage",
            {"some": "config", "vault_num_uses": 2},
        ),
        mock.call(
            {"SECRET_SHARED": "SHARED_SECRET(abc)"},
            SHARED_SECRET_SERVICE,
            "vault",
            "/nail/blah",
            "mesosstage",
            {"some": "config", "vault_num_uses": 2},
        ),
    ]


@mock.patch("paasta_tools.secret_tools.get_secret_provider", autospec=True)
def test_decrypt_secret_environment_for_service(mock_get_secret_provider):
    mock_secret_env = {"SECRET": "SECRET(123)"}
    mock_secret_provider = mock.Mock()
    mock_get_secret_provider.return_value = mock_secret_provider
    ret = decrypt_secret_environment_for_service(
        secret_env_vars=mock_secret_env,
        service_name="universe",
        secret_provider_name="vault",
        soa_dir="/nail/blah",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
    )
    mock_get_secret_provider.assert_called_with(
        secret_provider_name="vault",
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_names=["mesosstage"],
        secret_provider_kwargs={"some": "config"},
    )
    mock_secret_provider.decrypt_environment.assert_called_with(
        {"SECRET": "SECRET(123)"}
    )
    assert ret == mock_secret_provider.decrypt_environment.return_value


@mock.patch("paasta_tools.secret_tools.get_secret_provider", autospec=True)
def test_decrypt_secret_volumes_multiple_files(mock_get_secret_provider):
    mock_secret_provider = mock.Mock()
    mock_get_secret_provider.return_value = mock_secret_provider

    mock_secret_volumes_config = [
        SecretVolume(
            container_path="/the/container/path/",
            items=[
                {"key": "the_secret_name1", "path": "the_secret_filename1"},
                {"key": "the_secret_name2", "path": "the_secret_filename2"},
            ],
        )
    ]
    mock_secret_provider.decrypt_secret_raw.side_effect = [
        "the_secret_contents1",
        "the_secret_contents2",
    ]
    ret = decrypt_secret_volumes(
        secret_provider_name="vault",
        secret_volumes_config=mock_secret_volumes_config,
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
    )
    assert ret == {
        "/the/container/path/the_secret_filename1": "the_secret_contents1",
        "/the/container/path/the_secret_filename2": "the_secret_contents2",
    }


@mock.patch("paasta_tools.secret_tools.get_secret_provider", autospec=True)
def test_decrypt_secret_volumes_single_file(mock_get_secret_provider):
    mock_secret_provider = mock.Mock()
    mock_get_secret_provider.return_value = mock_secret_provider

    mock_secret_volumes_config = [
        SecretVolume(
            container_path="/the/container/path/",
            secret_name="the_secret_name",
        )
    ]
    mock_secret_provider.decrypt_secret_raw.side_effect = ["the_secret_contents"]
    ret = decrypt_secret_volumes(
        secret_provider_name="vault",
        secret_volumes_config=mock_secret_volumes_config,
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
    )
    assert ret == {
        "/the/container/path/the_secret_name": "the_secret_contents",
    }


@mock.patch("paasta_tools.secret_tools.get_secret_provider", autospec=True)
def test_decrypt_secret_decode_true(mock_get_secret_provider):
    mock_secret_provider = mock.Mock()
    mock_get_secret_provider.return_value = mock_secret_provider

    ret = decrypt_secret(
        secret_provider_name="vault",
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
        secret_name="the_secret_name",
        decode=True,
    )
    assert ret

    mock_secret_provider.decrypt_secret.assert_called()
    mock_secret_provider.decrypt_secret.assert_called_with("the_secret_name")


@mock.patch("paasta_tools.secret_tools.get_secret_provider", autospec=True)
def test_decrypt_secret_decode_false(mock_get_secret_provider):
    mock_secret_provider = mock.Mock()
    mock_get_secret_provider.return_value = mock_secret_provider

    ret = decrypt_secret(
        secret_provider_name="vault",
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_name="mesosstage",
        secret_provider_kwargs={"some": "config"},
        secret_name="the_secret_name",
        decode=False,
    )
    assert ret

    mock_secret_provider.decrypt_secret_raw.assert_called()
    mock_secret_provider.decrypt_secret_raw.assert_called_with("the_secret_name")
