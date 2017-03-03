from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from paasta_tools import native_mesos_scheduler
from paasta_tools import utils


def test_main():
    with mock.patch(
            'paasta_tools.native_mesos_scheduler.get_paasta_native_jobs_for_cluster',
            return_value=[('service1', 'instance1'), ('service2', 'instance2')], autospec=True
        ), mock.patch(
            'paasta_tools.native_mesos_scheduler.create_driver', autospec=True
        ), mock.patch(
            'paasta_tools.native_mesos_scheduler.sleep', autospec=True
        ), mock.patch(
            'paasta_tools.native_mesos_scheduler.load_system_paasta_config', autospec=True
        ), mock.patch(
            'paasta_tools.native_mesos_scheduler.compose_job_id', autospec=True
        ), mock.patch(
            'paasta_tools.native_mesos_scheduler.NativeScheduler', autospec=True):
        native_mesos_scheduler.main(["--stay-alive-seconds=0"])
