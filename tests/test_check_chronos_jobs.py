import pysensu_yelp
from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools import check_chronos_jobs
from paasta_tools import chronos_tools
from paasta_tools import utils


@patch('paasta_tools.check_chronos_jobs.monitoring_tools.get_runbook')
def test_compose_monitoring_overrides_for_service(mock_get_runbook):
    mock_get_runbook.return_value = 'myrunbook'
    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service='myservice',
            get_monitoring=Mock(return_value={}),
        ),
        'soa_dir'
    ) == {
        'alert_after': '2m',
        'check_every': '1m',
        'runbook': 'myrunbook'
    }


@patch('paasta_tools.check_chronos_jobs.monitoring_tools.get_runbook')
def test_compose_monitoring_overrides_for_service_respects_alert_after(mock_get_runbook):
    mock_get_runbook.return_value = 'myrunbook'
    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service='myservice',
            get_monitoring=Mock(return_value={'alert_after': '10m'}),
        ),
        'soa_dir'
    ) == {
        'alert_after': '10m',
        'check_every': '1m',
        'runbook': 'myrunbook'
    }


def test_compose_check_name_for_job():
    expected_check = 'check-chronos-jobs.myservice.myinstance'
    assert chronos_tools.compose_check_name_for_service_instance('check-chronos-jobs',
                                                                 'myservice', 'myinstance') == expected_check


@patch('paasta_tools.chronos_tools.monitoring_tools.send_event')
def test_send_event_to_sensu(mock_send_event):
    check_chronos_jobs.send_event(
        service='myservice',
        instance='myinstance',
        monitoring_overrides={},
        soa_dir='soadir',
        status_code=0,
        message='this is great',
    )
    assert mock_send_event.called_once_with(
        'myservice',
        'check-chronos-jobs.myservice.myinstance',
        {},
        0,
        'this is great',
        'soadir',
    )


@patch('paasta_tools.check_chronos_jobs.chronos_tools.get_status_last_run')
def test_last_run_state_for_jobs(mock_status_last_run):
    mock_status_last_run.side_effect = [
        ('faketimestamp', chronos_tools.LastRunState.Success),
        ('faketimestamp', chronos_tools.LastRunState.Fail),
        ('faketimestamp', chronos_tools.LastRunState.NotRun),
    ]
    assert check_chronos_jobs.last_run_state_for_jobs([{}, {}, {}]) == [
        ({}, chronos_tools.LastRunState.Success),
        ({}, chronos_tools.LastRunState.Fail),
        ({}, chronos_tools.LastRunState.NotRun),
    ]


def test_sensu_event_for_last_run_state_success():
    result = check_chronos_jobs.sensu_event_for_last_run_state(chronos_tools.LastRunState.Success)
    assert result == pysensu_yelp.Status.OK


def test_sensu_event_for_last_run_state_fail():
    result = check_chronos_jobs.sensu_event_for_last_run_state(chronos_tools.LastRunState.Fail)
    assert result == pysensu_yelp.Status.CRITICAL


def test_sensu_event_for_last_run_state_not_run():
    result = check_chronos_jobs.sensu_event_for_last_run_state(chronos_tools.LastRunState.NotRun)
    assert result == pysensu_yelp.Status.OK


def test_sensu_event_for_last_run_state_invalid():
    with raises(ValueError):
        check_chronos_jobs.sensu_event_for_last_run_state(100)


@patch('paasta_tools.check_chronos_jobs.chronos_tools.lookup_chronos_jobs', autospec=True)
@patch('paasta_tools.check_chronos_jobs.chronos_tools.filter_enabled_jobs', autospec=True)
@patch('paasta_tools.check_chronos_jobs.chronos_tools.get_status_last_run', autospec=True)
def test_build_service_job_mapping(mock_last_run_state, mock_filter_enabled_jobs, mock_lookup_chronos_jobs):
    # iter() is a workaround
    # (http://lists.idyll.org/pipermail/testing-in-python/2013-April/005527.html)
    # for a bug in mock (http://bugs.python.org/issue17826)
    fake_jobs = [[
        {
            'name': 'foo'
        },
        {
            'name': 'foo'
        },
        {
            'name': 'foo'
        }
    ] for x in range(0, 3)]
    mock_lookup_chronos_jobs.side_effect = iter(fake_jobs)
    mock_filter_enabled_jobs.side_effect = iter([[{}, {}, {}] for x in range(0, 3)])
    mock_last_run_state.side_effect = iter([
        ('faketimestamp', chronos_tools.LastRunState.Success),
        ('faketimestamp', chronos_tools.LastRunState.Fail),
        ('faketimestamp', chronos_tools.LastRunState.NotRun),
    ] * 3)

    fake_configured_jobs = [('service1', 'main'), ('service2', 'main'), ('service3', 'main')]
    fake_client = Mock(list=Mock(return_value=[('service1', 'main'), ('service2', 'main'), ('service3', 'main')]))

    expected_job_states = [
        ({'name': 'foo'}, chronos_tools.LastRunState.Success),
        ({'name': 'foo'}, chronos_tools.LastRunState.Fail),
        ({'name': 'foo'}, chronos_tools.LastRunState.NotRun),
    ]

    expected = {
        ('service1', 'main'): expected_job_states,
        ('service2', 'main'): expected_job_states,
        ('service3', 'main'): expected_job_states,
    }
    assert check_chronos_jobs.build_service_job_mapping(fake_client, fake_configured_jobs) == expected


def test_message_for_status_fail():
    expected_output = (
        "Last run of job myservice.myinstance failed.\n"
        "You can view the logs for the job with:\n"
        "paasta logs -s myservice -i myinstance -c mycluster .\n"
        "If your job didn't manage to start up, you can view the stdout and stderr of your job using:\n"
        "paasta status -s myservice -i myinstance -c mycluster -vv .\n"
        "If you need to rerun your job for the datetime it was started, you can do so with:\n"
        "paasta rerun -s myservice -i myinstance -c mycluster -d {datetime} .\n"
        "See the docs on paasta rerun here:\n"
        "https://paasta.readthedocs.io/en/latest/workflow.html#re-running-failed-jobs for more details."
    )
    assert check_chronos_jobs.message_for_status(
        status=pysensu_yelp.Status.CRITICAL,
        service='myservice',
        instance='myinstance',
        cluster='mycluster',
    ) == expected_output


def test_message_for_status_success():
    assert check_chronos_jobs.message_for_status(pysensu_yelp.Status.OK, 'service', 'instance', 'full_job_id') == \
        'Last run of job service%sinstance Succeded' % utils.SPACER


def test_message_for_status_unknown():
    assert check_chronos_jobs.message_for_status(pysensu_yelp.Status.UNKNOWN, 'service', 'instance', 'full_job_id') == \
        'Last run of job service%sinstance Unknown' % utils.SPACER


def test_sensu_message_status_for_jobs_too_many():
    fake_job_state_pairs = [({'name': 'full_job_id', 'disabled': False}, chronos_tools.LastRunState.Success),
                            ({}, chronos_tools.LastRunState.Success)]
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        Mock(), 'myservice', 'myinstance', 'mycluster', fake_job_state_pairs)
    expected_output = (
        "Unknown: somehow there was more than one enabled job for myservice.myinstance. "
        "Talk to the PaaSTA team as this indicates a bug"
    )
    assert output == expected_output
    assert status == pysensu_yelp.Status.UNKNOWN


def test_sensu_message_status_ok():
    fake_job_state_pairs = [({'name': 'full_job_id', 'disabled': False}, chronos_tools.LastRunState.Success)]
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        Mock(), 'myservice', 'myinstance', 'cluster', fake_job_state_pairs)
    expected_output = "Last run of job myservice.myinstance Succeded"
    assert output == expected_output
    assert status == pysensu_yelp.Status.OK


@patch('paasta_tools.check_chronos_jobs.message_for_status')
def test_sensu_message_status_fail(mock_message_for_status):
    mock_message_for_status.return_value = 'my failure message'
    fake_job_id = 'full_job_id'
    fake_job_state_pairs = [({'name': fake_job_id, 'disabled': False}, chronos_tools.LastRunState.Fail)]
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        Mock(), 'myservice', 'myinstance', 'mycluster', fake_job_state_pairs)
    assert output == 'my failure message'
    assert status == pysensu_yelp.Status.CRITICAL


def test_sensu_message_status_no_run():
    fake_job_state_pairs = []
    with patch('paasta_tools.check_chronos_jobs.load_chronos_job_config', autospec=True,
               return_value=Mock(get_disabled=Mock(return_value=False))):
        output, status = check_chronos_jobs.sensu_message_status_for_jobs(
            Mock(get_disabled=Mock(return_value=False)), 'myservice', 'myinstance', 'mycluster', fake_job_state_pairs)
    expected_output = "Warning: myservice.myinstance isn't in chronos at all, which means it may not be deployed yet"
    assert output == expected_output
    assert status == pysensu_yelp.Status.WARNING


def test_sensu_message_status_no_run_disabled():
    fake_job_state_pairs = []
    with patch('paasta_tools.check_chronos_jobs.load_chronos_job_config', autospec=True,
               return_value=Mock(get_disabled=Mock(return_value=True))):
        output, status = check_chronos_jobs.sensu_message_status_for_jobs(
            Mock(), 'myservice', 'myinstance', 'mycluster', fake_job_state_pairs)
    expected_output = "Job myservice.myinstance is disabled - ignoring status."
    assert output == expected_output
    assert status == pysensu_yelp.Status.OK


def test_sensu_message_status_disabled():
    fake_job_state_pairs = [({'name': 'fake_job_id', 'disabled': True}, chronos_tools.LastRunState.Fail)]
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        Mock(), 'myservice', 'myinstance', 'mycluster', fake_job_state_pairs)
    expected_output = "Job myservice.myinstance is disabled - ignoring status."
    assert output == expected_output
    assert status == pysensu_yelp.Status.OK
