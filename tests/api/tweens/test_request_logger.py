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

import mock
import pytest
from pyramid.request import Request
from pyramid.response import Response

from paasta_tools.api.tweens import request_logger


@pytest.fixture(autouse=True)
def mock_clog():
    with mock.patch.object(request_logger, "clog", autospec=True) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_settings():
    with mock.patch(
        "paasta_tools.api.settings.cluster",
        "a_cluster",
        autospec=False,
    ), mock.patch(
        "paasta_tools.api.settings.hostname",
        "a_hostname",
        autospec=False,
    ):
        yield


@pytest.fixture
def mock_handler():
    return mock.Mock()


@pytest.fixture
def mock_registry():
    reg = mock.Mock()
    reg.settings = {}
    yield reg
    reg.settings.clear()


@pytest.fixture
def mock_factory(mock_handler, mock_registry):
    mock_registry.settings = {"request_log_name": "request_logs"}
    yield request_logger.request_logger_tween_factory(
        mock_handler,
        mock_registry,
    )


def test_request_logger_tween_factory_init(mock_factory):
    assert mock_factory.log_name == "request_logs"


@mock.patch("time.time", mock.Mock(return_value="a_time"), autospec=False)
@mock.patch(
    "paasta_tools.utils.get_hostname",
    mock.Mock(return_value="a_hostname"),
    autospec=False,
)
def test_request_logger_tween_factory_log(mock_clog, mock_factory):
    mock_factory._log(
        timestamp=datetime.fromtimestamp(1589828049),
        level="ERROR",
        additional_fields={"additional_key": "additional_value"},
    )
    assert mock_clog.log_line.call_args_list == [
        mock.call(
            "request_logs",
            (
                '{"additional_key": "additional_value", '
                '"cluster": "a_cluster", '
                '"hostname": "a_hostname", '
                '"human_timestamp": "2020-05-18T18:54:09", '
                '"level": "ERROR", '
                '"unix_timestamp": 1589828049.0}'
            ),
        ),
    ]


@mock.patch.object(request_logger, "datetime", autospec=True)
@mock.patch(
    "traceback.format_exc",
    mock.Mock(return_value="an_exc_body"),
    autospec=False,
)
@pytest.mark.parametrize(
    "status_code,exc,expected_lvl,extra_expected_response",
    [
        (200, None, "INFO", {}),
        (300, None, "WARNING", {}),
        (400, None, "ERROR", {"body": "a_body"}),
        (
            500,
            Exception(),
            "ERROR",
            {"exc_type": "Exception", "exc_info": "an_exc_body"},
        ),
    ],
)
def test_request_logger_tween_factory_call(
    mock_datetime,
    mock_handler,
    mock_factory,
    status_code,
    exc,
    expected_lvl,
    extra_expected_response,
):
    req = Request.blank("/path/to/something")
    mock_handler.return_value = Response(
        body="a_body",
        status=status_code,
    )
    if exc is not None:
        mock_handler.side_effect = exc
    mock_factory._log = mock.Mock()
    mock_datetime.now = mock.Mock(
        side_effect=[datetime.fromtimestamp(0), datetime.fromtimestamp(57)],
    )

    try:
        mock_factory(req)
    except Exception as e:
        if exc is None:
            pytest.fail(f"Got unexpected exception: {e}")

    expected_response = {
        "status_code": status_code,
        "response_time_ms": 57000.0,
    }
    expected_response.update(extra_expected_response)
    assert mock_factory._log.call_args_list == [
        mock.call(
            timestamp=datetime.fromtimestamp(0),
            level=expected_lvl,
            additional_fields={
                # most of these are default for a blank request
                "request": {
                    "path": "/path/to/something",
                    "params": {},
                    "client_addr": None,
                    "http_method": "GET",
                    "headers": {"Host": "localhost:80"},
                },
                "response": expected_response,
            },
        ),
    ]
