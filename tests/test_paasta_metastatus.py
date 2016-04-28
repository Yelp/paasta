#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
import contextlib

from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools import paasta_metastatus
from paasta_tools.chronos_tools import ChronosNotConfigured
from paasta_tools.marathon_tools import MarathonConfig
from paasta_tools.marathon_tools import MarathonNotConfigured
from paasta_tools.utils import PaastaColors


def test_ok_check_threshold():
    assert paasta_metastatus.check_threshold(10, 30)


def test_fail_check_threshold():
    assert not paasta_metastatus.check_threshold(80, 30)


def test_get_mesos_cpu_status():
    fake_metrics = {
        'master/cpus_total': 3,
        'master/cpus_used': 1,
    }
    total, used, available = paasta_metastatus.get_mesos_cpu_status(fake_metrics)
    assert total == 3
    assert used == 1
    assert available == 2


def test_ok_cpu_health():
    ok_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 1,
    }
    ok_output, ok_health = paasta_metastatus.assert_cpu_health(ok_metrics)
    assert ok_health
    assert "CPUs: 1.00 / 10 in use (%s)" % PaastaColors.green("10.00%") in ok_output


def test_bad_cpu_health():
    failure_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 9,
    }
    failure_output, failure_health = paasta_metastatus.assert_cpu_health(failure_metrics)
    assert not failure_health
    assert PaastaColors.red("CRITICAL: Less than 10% CPUs available. (Currently using 90.00% of 10)") in failure_output


def test_assert_memory_health():
    ok_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 512,
    }
    ok_output, ok_health = paasta_metastatus.assert_memory_health(ok_metrics)
    assert ok_health
    assert "Memory: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_memory_health():
    failure_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 1000,
    }
    failure_output, failure_health = paasta_metastatus.assert_memory_health(failure_metrics)
    assert not failure_health
    assert PaastaColors.red(
        "CRITICAL: Less than 10% memory available. (Currently using 97.66% of 1.00GB)") in failure_output


def test_assert_disk_health():
    ok_metrics = {
        'master/disk_total': 1024,
        'master/disk_used': 512,
    }
    ok_output, ok_health = paasta_metastatus.assert_disk_health(ok_metrics)
    assert ok_health
    assert "Disk: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_disk_health():
    failure_metrics = {
        'master/disk_total': 1024,
        'master/disk_used': 1000,
    }
    failure_output, failure_health = paasta_metastatus.assert_disk_health(failure_metrics)
    assert not failure_health
    assert PaastaColors.red("CRITICAL: Less than 10% disk available. (Currently using 97.66%)") in failure_output


def assert_cpu_health_mesos_reports_zero():
    mesos_metrics = {
        'master/cpus_total': 0,
        'master/cpus_used': 1,
    }
    failure_output, failure_health = paasta_metastatus.assert_cpu_health(mesos_metrics)
    assert failure_output == "Error reading total available cpu from mesos!"
    assert failure_health is False


def assert_memory_health_mesos_reports_zero():
    mesos_metrics = {
        'master/mem_total': 0,
        'master/mem_used': 1,
    }
    failure_output, failure_health = paasta_metastatus.assert_memory_health(mesos_metrics)
    assert failure_output == "Error reading total available memory from mesos!"
    assert failure_health is False


def assert_disk_health_mesos_reports_zero():
    mesos_metrics = {
        'master/disk_total': 0,
        'master/disk_used': 1,
    }
    failure_output, failure_health = paasta_metastatus.assert_disk_health(mesos_metrics)
    assert failure_output == "Error reading total available disk from mesos!"
    assert failure_health is False


def test_assert_no_duplicate_frameworks():
    state = {
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework2',
            },
            {
                'name': 'test_framework3',
            },
            {
                'name': 'test_framework4',
            },
        ]
    }
    output, ok = paasta_metastatus.assert_no_duplicate_frameworks(state)

    expected_output = "\n".join(["frameworks:"] +
                                map(lambda x: '    framework: %s count: 1' % x['name'], state['frameworks']))
    assert output == expected_output
    assert ok


def test_duplicate_frameworks():
    state = {
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework2',
            },
        ]
    }
    output, ok = paasta_metastatus.assert_no_duplicate_frameworks(state)
    assert PaastaColors.red(
        "    CRITICAL: Framework test_framework1 has 3 instances running--expected no more than 1."
    ) in output
    assert not ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_ok_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2"
    ]
    output, ok = paasta_metastatus.assert_marathon_apps(client)
    assert "marathon apps: 2" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_no_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = []
    output, ok = paasta_metastatus.assert_marathon_apps(client)
    assert PaastaColors.red("CRITICAL: No marathon apps running") in output
    assert not ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_marathon_tasks(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_tasks.return_value = ["MarathonTask:1"]
    output, ok = paasta_metastatus.assert_marathon_tasks(client)
    assert "marathon tasks: 1" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_assert_marathon_deployments(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_deployments.return_value = ["MarathonDeployment:1"]
    output, ok = paasta_metastatus.assert_marathon_deployments(client)
    assert "marathon deployments: 1" in output
    assert ok


def test_assert_slave_health():
    fake_slave_info = {
        'master/slaves_active': 10,
        'master/slaves_inactive': 10
    }
    output, ok = paasta_metastatus.assert_slave_health(fake_slave_info)
    assert "slaves: active: 10 inactive: 10" in output
    assert ok


def test_assert_tasks_running():
    fake_tasks_info = {
        'master/tasks_running': 20,
        'master/tasks_staging': 10,
        'master/tasks_starting': 10,
    }
    output, ok = paasta_metastatus.assert_tasks_running(fake_tasks_info)
    assert "tasks: running: 20 staging: 10 starting: 10" in output
    assert ok


@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
def test_healthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 5
    mock_quorum_size.return_value = 3
    output, health = paasta_metastatus.assert_quorum_size({})
    assert health
    assert 'quorum: masters: 5 configured quorum: 3 ' in output


@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
def test_unhealthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 1
    mock_quorum_size.return_value = 3
    output, health = paasta_metastatus.assert_quorum_size({})
    assert not health
    assert "CRITICAL: Number of masters (1) less than configured quorum(3)." in output


@patch('socket.getfqdn', autospec=True)
@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
@patch('paasta_tools.paasta_metastatus.get_mesos_stats')
def test_get_mesos_status(
    mock_get_mesos_stats,
    mock_get_num_masters,
    mock_get_configured_quorum_size,
    mock_getfqdn,
):
    mock_getfqdn.return_value = 'fakename'
    mock_get_mesos_stats.return_value = {
        'master/cpus_total': 10,
        'master/cpus_used': 8,
        'master/mem_total': 10240,
        'master/mem_used': 2048,
        'master/disk_total': 10240,
        'master/disk_used': 3072,
        'master/tasks_running': 3,
        'master/tasks_staging': 4,
        'master/tasks_starting': 0,
        'master/slaves_active': 4,
        'master/slaves_inactive': 0,
    }
    mesos_state = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        },
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
        ]
    }
    mock_get_num_masters.return_value = 5
    mock_get_configured_quorum_size.return_value = 3
    expected_cpus_output = "CPUs: 8.00 / 10 in use (%s)" % PaastaColors.green("80.00%")
    expected_mem_output = \
        "Memory: 2.00 / 10.00GB in use (%s)" % PaastaColors.green("20.00%")
    expected_disk_output = "Disk: 3.00 / 10.00GB in use (%s)" % PaastaColors.green("30.00%")
    expected_tasks_output = \
        "tasks: running: 3 staging: 4 starting: 0"
    expected_duplicate_frameworks_output = \
        "frameworks:\n%s" % \
        PaastaColors.red("    CRITICAL: Framework test_framework1 has 2 instances running--expected no more than 1.")
    expected_slaves_output = \
        "slaves: active: 4 inactive: 0"
    expected_masters_quorum_output = \
        "quorum: masters: 5 configured quorum: 3 "

    results = paasta_metastatus.get_mesos_status(mesos_state, verbosity=0)

    assert mock_get_mesos_stats.called_once()
    assert (expected_masters_quorum_output, True) in results
    assert (expected_cpus_output, True) in results
    assert (expected_mem_output, True) in results
    assert (expected_disk_output, True) in results
    assert (expected_tasks_output, True) in results
    assert (expected_duplicate_frameworks_output, False) in results
    assert (expected_slaves_output, True) in results


@patch('paasta_tools.paasta_metastatus.marathon_tools.get_marathon_client', autospec=True)
@patch('paasta_tools.paasta_metastatus.marathon_tools.load_marathon_config', autospec=True)
def test_get_marathon_status(
    mock_load_marathon_config,
    mock_get_marathon_client,
):
    mock_load_marathon_config.return_value = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    }, '/fake_config/fake_marathon.json')
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2"
    ]
    client.list_deployments.return_value = [
        "MarathonDeployment::1",
    ]
    client.list_tasks.return_value = [
        "MarathonTask::1",
        "MarathonTask::2",
        "MarathonTask::3"
    ]
    expected_apps_output = ("marathon apps: 2", True)
    expected_deployment_output = ("marathon deployments: 1", True)
    expected_tasks_output = ("marathon tasks: 3", True)

    results = paasta_metastatus.get_marathon_status(client)

    assert expected_apps_output in results
    assert expected_deployment_output in results
    assert expected_tasks_output in results


def test_get_marathon_client():
    fake_config = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    }, '/fake_config/fake_marathon.json')
    client = paasta_metastatus.get_marathon_client(fake_config)
    assert client.servers == ['fakeurl']
    assert client.auth == ('fakeuser', 'fakepass')


def test_assert_chronos_scheduled_jobs():
    mock_client = Mock()
    mock_client.list.return_value = [
        {'name': 'myjob', 'disabled': False},
        {'name': 'myjob', 'disabled': True},
    ]
    results = paasta_metastatus.assert_chronos_scheduled_jobs(mock_client)
    assert results == ('Enabled chronos jobs: 1', True)


def test_get_chronos_status():
    client = Mock()
    client.list.return_value = [
        {'name': 'fake_job1', 'disabled': False},
        {'name': 'fake_job2', 'disabled': False},
    ]
    expected_jobs_output = ("Enabled chronos jobs: 2", True)
    results = paasta_metastatus.get_chronos_status(client)

    assert expected_jobs_output in results


def test_main_no_marathon_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_state_from_leader', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.get_marathon_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.parse_args', autospec=True),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        load_get_mesos_state_from_leader_patch,
        load_get_mesos_status_patch,
        load_get_marathon_status_patch,
        parse_args_patch,
    ):
        fake_args = Mock(
            verbose=0,
        )
        parse_args_patch.return_value = fake_args
        load_marathon_config_patch.side_effect = MarathonNotConfigured
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0


def test_main_no_chronos_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_state_from_leader', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.get_marathon_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.parse_args', autospec=True),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        load_get_mesos_state_from_leader_patch,
        load_get_mesos_status_patch,
        load_get_marathon_status_patch,
        parse_args_patch,
    ):

        fake_args = Mock(
            verbose=0,
        )
        parse_args_patch.return_value = fake_args
        load_chronos_config_patch.side_effect = ChronosNotConfigured
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0


def test_assert_extra_slave_data_no_slaves():
    fake_mesos_state = {'slaves': [], 'frameworks': [], 'tasks': []}
    expected = 'No mesos slaves registered on this cluster!'
    actual = paasta_metastatus.assert_extra_slave_data(fake_mesos_state)[0]
    assert expected == actual.strip()


def test_assert_extra_attribute_data_no_slaves():
    fake_mesos_state = {'slaves': [], 'frameworks': [], 'tasks': []}
    expected = 'No mesos slaves registered on this cluster!'
    actual = paasta_metastatus.assert_extra_attribute_data(fake_mesos_state)[0]
    assert expected == actual.strip()


def test_assert_extra_attribute_data_slaves_attributes():
    fake_mesos_state = {
        'slaves': [
            {
                'id': 'test-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 1000,
                },
                'attributes': {
                    'habitat': 'test-habitat',
                },
            },
            {
                'id': 'test-slave2',
                'hostname': 'test2.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 1000,
                },
                'attributes': {
                    'habitat': 'test-habitat-2',
                },
            },
        ],
        'frameworks': [],
    }
    assert paasta_metastatus.assert_extra_attribute_data(fake_mesos_state)[1]


def test_assert_extra_attribute_data_slaves_no_attributes():
    fake_mesos_state = {
        'slaves': [
            {
                'id': 'test-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 1000,
                },
                'attributes': {
                },
            },
            {
                'id': 'test-slave2',
                'hostname': 'test2.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 1000,
                },
                'attributes': {
                },
            },
        ],
        'frameworks': [],
    }
    assert paasta_metastatus.assert_extra_attribute_data(fake_mesos_state)[1]


def test_status_for_results():
    assert paasta_metastatus.status_for_results([('message', True), ('message', False)]) == [True, False]


def test_generate_summary_for_results_ok():
    assert (paasta_metastatus.generate_summary_for_check("Myservice", True) ==
            "Myservice Status: %s" % PaastaColors.green("OK"))


def test_generate_summary_for_results_critical():
    assert (paasta_metastatus.generate_summary_for_check("Myservice", False) ==
            "Myservice Status: %s" % PaastaColors.red("CRITICAL"))


def test_critical_events_in_outputs():
    assert (paasta_metastatus.critical_events_in_outputs([('myservice', True), ('myservice_false', False)]) ==
            [('myservice_false', False)])


def test_filter_mesos_state_metrics():
    test_resource_dictionary = {
        'cpus': 0,
        'mem': 1,
        'MEM': 2,
        'garbage_data': 3,
    }
    expected = {
        'cpus': 0,
        'mem': 1,
    }
    assert paasta_metastatus.filter_mesos_state_metrics(test_resource_dictionary) == expected


def test_get_mesos_slave_data():
    mesos_state = {
        'slaves': [
            {
                'id': 'test-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 1000,
                },
            },
        ],
        'frameworks': [
            {
                'tasks': [
                    {
                        'slave_id': 'test-slave',
                        'resources': {
                            'cpus': 50,
                            'disk': 100,
                            'mem': 0,
                            'something-bogus': 25,
                        },
                    },
                ],
            },
        ],
    }
    expected_free_resources = [
        {
            'cpus': 0,
            'disk': 100,
            'mem': 1000,
        },
    ]
    extra_mesos_slave_data = paasta_metastatus.get_extra_mesos_slave_data(mesos_state)
    assert (len(extra_mesos_slave_data) == len(mesos_state['slaves']))
    assert ([slave['free_resources'] for slave in extra_mesos_slave_data] == expected_free_resources)


def test_get_mesos_habitat_data():
    mesos_state = {
        'slaves': [
            {
                'id': 'test-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 75,
                    'disk': 250,
                    'mem': 1000,
                },
                'attributes': {
                    'habitat': 'test-habitat',
                },
            },
            {
                'id': 'test-slave2',
                'hostname': 'test2.somewhere.www',
                'resources': {
                    'cpus': 50,
                    'disk': 200,
                    'mem': 750,
                },
                'attributes': {
                    'habitat': 'test-habitat-2',
                },
            },
            {
                'id': 'test-slave3',
                'hostname': 'test3.somewhere.www',
                'resources': {
                    'cpus': 22,
                    'disk': 201,
                    'mem': 920,
                },
                'attributes': {
                    'habitat': 'test-habitat-2',
                },
            },
        ],
        'frameworks': [
            {
                'tasks': [
                    {
                        'slave_id': 'test-slave',
                        'resources': {
                            'cpus': 50,
                            'disk': 100,
                            'mem': 0,
                            'something-bogus': 25,
                        },
                    },
                ],
            },
        ],
        'cluster': 'fake_cluster',
    }
    expected_free_resources = (
        (
            'habitat',
            {
                'free':
                {
                    'test-habitat': {
                        'cpus': 25,
                        'disk': 150,
                        'mem': 1000,
                    },
                    'test-habitat-2': {
                        'cpus': 72,
                        'disk': 401,
                        'mem': 1670,
                    },
                },
                'total':
                    {
                    'test-habitat': {
                        'cpus': 75,
                        'disk': 250,
                        'mem': 1000,
                    },
                    'test-habitat-2': {
                        'cpus': 72,
                        'disk': 401,
                        'mem': 1670,
                    },
                }
            }
        ),
    )
    extra_mesos_habitat_data = paasta_metastatus.get_extra_mesos_attribute_data(mesos_state)

    assert (tuple(extra_mesos_habitat_data) == expected_free_resources)


def test_get_mesos_habitat_data_humanized():
    mesos_state = {
        'slaves': [
            {
                'id': 'somenametest-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 75,
                    'disk': 250,
                    'mem': 100,
                },
                'attributes': {
                    'habitat': 'somenametest-habitat',
                },
            },
            {
                'id': 'somenametest-slave2',
                'hostname': 'test2.somewhere.www',
                'resources': {
                    'cpus': 500,
                    'disk': 200,
                    'mem': 750,
                },
                'attributes': {
                    'habitat': 'somenametest-habitat-2',
                },
            },
        ],

        'frameworks': [
            {
                'tasks': [
                    {
                        'slave_id': 'somenametest-slave',
                        'resources': {
                            'cpus': 50,
                            'disk': 100,
                            'mem': 80,
                            'something-bogus': 25,
                        },
                    },
                ],
            },
        ],
        'cluster': 'fake_cluster',
    }
    expected_slave_humanize_output = """  Hostname             CPU (free/total)  RAM (free/total)  Disk (free/total)
    test.somewhere.www   25.00/75.00       20.0M/100.0M      150.0M/250.0M
    test2.somewhere.www  500.00/500.00     750.0M/750.0M     200.0M/200.0M"""
    expected_attribute_humanize_output = """  Habitat                 CPU (free/total)  RAM (free/total)  Disk (free/total)
    somenametest-habitat    25.00/75.00       20.0M/100.0M      150.0M/250.0M
    somenametest-habitat-2  500.00/500.00     750.0M/750.0M     200.0M/200.0M"""

    extra_slave_data = paasta_metastatus.assert_extra_slave_data(mesos_state,
                                                                 humanize_output=True)
    extra_attribute_data = paasta_metastatus.assert_extra_attribute_data(mesos_state,
                                                                         humanize_output=True)

    assert extra_slave_data[0] == expected_slave_humanize_output
    assert extra_attribute_data[0] == expected_attribute_humanize_output


def test_get_mesos_habitat_data_nonhumanized():
    mesos_state = {
        'slaves': [
            {
                'id': 'somenametest-slave',
                'hostname': 'test.somewhere.www',
                'resources': {
                    'cpus': 75,
                    'disk': 250,
                    'mem': 100,
                },
                'attributes': {
                    'habitat': 'somenametest-habitat',
                },
            },
            {
                'id': 'somenametest-slave2',
                'hostname': 'test2.somewhere.www',
                'resources': {
                    'cpus': 500,
                    'disk': 200,
                    'mem': 750,
                },
                'attributes': {
                    'habitat': 'somenametest-habitat-2',
                },
            },
        ],

        'frameworks': [
            {
                'tasks': [
                    {
                        'slave_id': 'somenametest-slave',
                        'resources': {
                            'cpus': 50,
                            'disk': 100,
                            'mem': 80,
                            'something-bogus': 25,
                        },
                    },
                ],
            },
        ],
        'cluster': 'fake_cluster',
    }

    expected_slave_output = """  Hostname             CPU (free/total)  RAM (free/total)  Disk (free/total)
    test.somewhere.www   25.00/75.00       20.00/100.00      150.00/250.00
    test2.somewhere.www  500.00/500.00     750.00/750.00     200.00/200.00"""
    expected_attribute_output = """  Habitat                 CPU (free/total)  RAM (free/total)  Disk (free/total)
    somenametest-habitat    25.00/75.00       20.00/100.00      150.00/250.00
    somenametest-habitat-2  500.00/500.00     750.00/750.00     200.00/200.00"""

    extra_slave_data = paasta_metastatus.assert_extra_slave_data(mesos_state, humanize_output=False)
    extra_attribute_data = paasta_metastatus.assert_extra_attribute_data(mesos_state, humanize_output=False)

    assert extra_slave_data[0] == expected_slave_output
    assert extra_attribute_data[0] == expected_attribute_output
