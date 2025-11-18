import contextlib
from unittest import mock
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

from paasta_tools import hacheck


@contextlib.contextmanager
def mock_ClientSession(**fake_session_kwargs):
    fake_session = MagicMock(name="session", **fake_session_kwargs)

    class FakeClientSession:
        def __init__(self, *args, **kwargs):
            ...

        async def __aenter__(*args):
            return fake_session

        async def __aexit__(*args):
            pass

    with mock.patch("aiohttp.ClientSession", new=FakeClientSession, autospec=False):
        yield


@pytest.mark.asyncio
async def test_get_spool():
    fake_response = mock.Mock(
        status=503,
        text=AsyncMock(
            return_value="Service service in down state since 1435694078.778886 "
            "until 1435694178.780000: Drained by Paasta"
        ),
    )
    fake_task = mock.Mock(host="fake_host", ports=[54321])

    with mock_ClientSession(
        get=Mock(
            return_value=MagicMock(__aenter__=AsyncMock(return_value=fake_response))
        )
    ):
        actual = await hacheck.get_spool(fake_task)

    expected = {
        "service": "service",
        "state": "down",
        "reason": "Drained by Paasta",
        "since": 1435694078.778886,
        "until": 1435694178.780000,
    }
    assert actual == expected


@pytest.mark.asyncio
async def test_get_spool_handles_no_ports():
    actual = await hacheck.get_spool(None)
    assert actual is None
