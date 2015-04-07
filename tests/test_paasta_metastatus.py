#!/usr/bin/env python

from StringIO import StringIO
from mock import patch
from paasta_tools import paasta_metastatus


@patch('paasta_tools.paasta_metastatus.fetch_mesos_stats')
@patch('sys.stdout', new_callable=StringIO)
def test_get_mesos_status(
    mock_stdout,
    mock_fetch_mesos_stats,
):
    mock_fetch_mesos_stats.return_value = {
        'master/cpus_total': 3,
        'master/cpus_used': 2,
        'master/mem_total': 10240,
        'master/mem_used': 2048,
        'master/tasks_running': 3,
        'master/tasks_staging': 4,
        'master/tasks_starting': 0,
        'master/slaves_active': 4,
        'master/slaves_inactive': 0,
    }
    expected_output = \
        "Mesos:\n" \
        "    cpus: 3 total => 2 used, 1 available\n" \
        "    memory: 10.00 GB total => 2.00 GB used, 8.00 GB available\n" \
        "    tasks: 3 running, 4 staging, 0 starting\n" \
        "    slaves: 4 active, 0 inactive\n"
    paasta_metastatus.get_mesos_status()
    output = mock_stdout.getvalue()

    assert mock_fetch_mesos_stats.called_once()
    assert expected_output == output
