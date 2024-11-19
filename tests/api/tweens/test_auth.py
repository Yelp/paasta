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
import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from pyramid.httpexceptions import HTTPForbidden

from paasta_tools.api.tweens import auth


@pytest.fixture
def mock_auth_tween():
    with patch.dict(
        os.environ,
        {
            "PAASTA_API_AUTH_ENFORCE": "1",
            "PAASTA_API_AUTH_ENDPOINT": "http://localhost:31337",
        },
    ):
        with patch("paasta_tools.api.tweens.auth.requests"):
            yield auth.AuthTweenFactory(MagicMock(), MagicMock())


def test_call(mock_auth_tween):
    mock_request = MagicMock(
        path="/something",
        method="post",
        headers={"Authorization": "Bearer aaa.bbb.ccc"},
    )
    with patch.object(mock_auth_tween, "is_request_authorized") as mock_is_authorized:
        mock_is_authorized.return_value = auth.AuthorizationOutcome(True, "Ok")
        mock_auth_tween(mock_request)
        mock_is_authorized.assert_called_once_with("/something", "aaa.bbb.ccc", "post")
        mock_auth_tween.handler.assert_called_once_with(mock_request)


def test_call_deny(mock_auth_tween):
    mock_request = MagicMock(
        path="/something",
        method="post",
        headers={"Authorization": "Bearer aaa.bbb.ccc"},
    )
    with patch.object(mock_auth_tween, "is_request_authorized") as mock_is_authorized:
        mock_is_authorized.return_value = auth.AuthorizationOutcome(False, "Denied")
        response = mock_auth_tween(mock_request)
        assert isinstance(response, HTTPForbidden)
        assert response.headers.get("X-Auth-Failure-Reason") == "Denied"


def test_is_request_authorized(mock_auth_tween):
    mock_auth_tween.session.post.return_value.json.return_value = {
        "result": {"allowed": True, "reason": "User allowed"}
    }
    assert mock_auth_tween.is_request_authorized(
        "/allowed", "aaa.bbb.ccc", "get"
    ) == auth.AuthorizationOutcome(True, "User allowed")
    mock_auth_tween.session.post.assert_called_once_with(
        url="http://localhost:31337",
        json={
            "input": {
                "path": "/allowed",
                "backend": "paasta",
                "token": "aaa.bbb.ccc",
                "method": "get",
            }
        },
        timeout=2,
    )


def test_is_request_authorized_fail(mock_auth_tween):
    mock_auth_tween.session.post.side_effect = Exception
    assert mock_auth_tween.is_request_authorized(
        "/allowed", "eee.ddd.fff", "get"
    ) == auth.AuthorizationOutcome(False, "Auth backend error")


def test_is_request_authorized_malformed(mock_auth_tween):
    mock_auth_tween.session.post.return_value.json.return_value = {"foo": "bar"}
    assert mock_auth_tween.is_request_authorized(
        "/allowed", "eee.ddd.fff", "post"
    ) == auth.AuthorizationOutcome(False, "Malformed auth response")
