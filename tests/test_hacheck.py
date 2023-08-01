import asyncio
import unittest
from unittest import mock

from paasta_tools import hacheck


class FakeClientSession:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestHacheck(unittest.IsolatedAsyncioTestCase):

    async def mock_ClientSession(self, **fake_session_kwargs):
        return FakeClientSession()

    async def test_get_spool(self):
        fake_response = mock.Mock(
            status=503,
            text=mock.CoroutineMock(
                return_value="Service service in down state since 1435694078.778886 "
                             "until 1435694178.780000: Drained by Paasta"
            ),
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])

        with mock.patch("aiohttp.ClientSession", new=self.mock_ClientSession):
            actual = await hacheck.get_spool(fake_task)

        expected = {
            "service": "service",
            "state": "down",
            "reason": "Drained by Paasta",
            "since": 1435694078.778886,
            "until": 1435694178.780000,
        }
        self.assertEqual(actual, expected)

    async def test_get_spool_handles_no_ports(self):
        actual = await hacheck.get_spool(None)
        self.assertIsNone(actual)


if __name__ == '__main__':
    unittest.main()
