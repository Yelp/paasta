import mock

from paasta_tools import drain_lib


def test_register_drain_method():

    with mock.patch('drain_lib._drain_methods'):
        @drain_lib.register_drain_method('REMOVEME')
        class REMOVEMEDrainMethod(drain_lib.DrainMethod):
            pass

        assert type(drain_lib.get_drain_method('REMOVEME', 'srv', 'inst', 'ns')) == REMOVEMEDrainMethod


class TestHacheckDrainMethod(object):
    drain_method = drain_lib.HacheckDrainMethod("srv", "inst", "ns", hacheck_port=12345)

    def test_spool_url(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        actual = self.drain_method.spool_url(fake_task)
        expected = 'http://fake_host:12345/spool/srv/54321/status'
        assert actual == expected

    def test_get_spool(self):
        fake_response = mock.Mock(
            status_code=503,
            text="Service service in down state since 1435694078.778886 until 1435694178.780000: Drained by Paasta",
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock.patch('requests.get', return_value=fake_response):
            actual = self.drain_method.get_spool(fake_task)

        expected = {
            'service_name': 'service',
            'state': 'down',
            'reason': 'Drained by Paasta',
            'since': 1435694078.778886,
            'until': 1435694178.780000,
        }
        assert actual == expected
