import contextlib

import mock

from paasta_tools import native_mesos_scheduler


def test_main():
    with contextlib.nested(
        mock.patch('paasta_tools.native_mesos_scheduler.get_paasta_native_jobs_for_cluster',
                   return_value=[('service1', 'instance1'), ('service2', 'instance2')],
                   autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.create_driver', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.sleep', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.PaastaScheduler', autospec=True),
    ):
        native_mesos_scheduler.main()
