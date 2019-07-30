from datetime import datetime
from datetime import timedelta

import pysensu_yelp
import pytz
from mock import Mock
from mock import patch
from pytest import fixture
from pytest import raises

from paasta_tools import check_chronos_jobs
from paasta_tools import chronos_rerun
from paasta_tools import chronos_tools
from paasta_tools import utils


@fixture
def mock_chronos_job_config():
    return Mock(
        service="myservice",
        instance="myinstance",
        cluster="mycluster",
        get_schedule_interval_in_seconds=Mock(return_value=60 * 60 * 24),
        get_schedule=Mock(return_value="0 1 * * *"),
        get_schedule_time_zone=Mock(return_value="America/Los_Angeles"),
    )


@fixture
def mock_chronos_client():
    c = Mock()
    c.job_stat.return_value = {"histogram": {"99thPercentile": 95158}}
    return c


@patch("paasta_tools.check_chronos_jobs.monitoring_tools.get_runbook", autospec=True)
def test_compose_monitoring_overrides_for_service(mock_get_runbook):
    mock_get_runbook.return_value = "myrunbook"
    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service="myservice",
            get_monitoring=Mock(return_value={}),
            get_schedule_interval_in_seconds=Mock(return_value=28800),
            soa_dir="soa_dir",
        )
    ) == {
        "alert_after": "15m",
        "check_every": "1m",
        "runbook": "myrunbook",
        "realert_every": 480,
    }


@patch("paasta_tools.check_chronos_jobs.monitoring_tools.get_runbook", autospec=True)
def test_compose_monitoring_overrides_for_service_respects_alert_after(
    mock_get_runbook
):
    mock_get_runbook.return_value = "myrunbook"
    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service="myservice",
            get_monitoring=Mock(return_value={"alert_after": "10m"}),
            get_schedule_interval_in_seconds=Mock(return_value=28800),
            soa_dir="soa_dir",
        )
    ) == {
        "alert_after": "10m",
        "check_every": "1m",
        "runbook": "myrunbook",
        "realert_every": 480,
    }


@patch(
    "paasta_tools.check_chronos_jobs.monitoring_tools.service_configuration_lib.read_service_configuration",
    autospec=True,
)
@patch(
    "paasta_tools.check_chronos_jobs.monitoring_tools.read_monitoring_config",
    autospec=True,
)
def test_compose_monitoring_overrides_for_realert_every(
    mock_read_monitoring, mock_read_service_config
):
    mock_read_monitoring.return_value = {"runbook": "myrunbook"}
    mock_read_service_config.return_value = {}

    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service="myservice",
            get_monitoring=Mock(return_value={"realert_every": 5}),
            get_schedule_interval_in_seconds=Mock(return_value=28800),
            soa_dir="soa_dir",
        )
    ) == {
        "alert_after": "15m",
        "check_every": "1m",
        "runbook": "myrunbook",
        "realert_every": 5,
    }

    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service="myservice",
            get_monitoring=Mock(return_value={}),
            get_schedule_interval_in_seconds=Mock(return_value=None),
            soa_dir="soa_dir",
        )
    ) == {
        "alert_after": "15m",
        "check_every": "1m",
        "runbook": "myrunbook",
        "realert_every": -1,
    }

    mock_read_monitoring.return_value = {"runbook": "myrunbook", "realert_every": 10}
    assert check_chronos_jobs.compose_monitoring_overrides_for_service(
        Mock(
            service="myservice",
            get_monitoring=Mock(return_value={}),
            get_schedule_interval_in_seconds=Mock(return_value=None),
            soa_dir="soa_dir",
        )
    ) == {
        "alert_after": "15m",
        "check_every": "1m",
        "runbook": "myrunbook",
        "realert_every": 10,
    }


def test_compose_check_name_for_job():
    expected_check = "check-chronos-jobs.myservice.myinstance"
    assert (
        chronos_tools.compose_check_name_for_service_instance(
            "check-chronos-jobs", "myservice", "myinstance"
        )
        == expected_check
    )


@patch(
    "paasta_tools.check_chronos_jobs.compose_monitoring_overrides_for_service",
    autospec=True,
)
@patch("paasta_tools.chronos_tools.monitoring_tools.send_event", autospec=True)
def test_send_event_with_no_realert_every_to_sensu(
    mock_send_event, mock_compose_monitoring_overrides_for_service
):
    mock_compose_monitoring_overrides_for_service.return_value = {}
    check_chronos_jobs.send_event(
        Mock(service="myservice", instance="myinstance", soa_dir="soadir"),
        status_code=0,
        output="this is great",
    )
    mock_send_event.assert_called_once_with(
        service="myservice",
        check_name="check_chronos_jobs.myservice.myinstance",
        overrides={},
        status=0,
        output="this is great",
        soa_dir="soadir",
    )


@patch(
    "paasta_tools.check_chronos_jobs.compose_monitoring_overrides_for_service",
    autospec=True,
)
@patch("paasta_tools.chronos_tools.monitoring_tools.send_event", autospec=True)
def test_send_event_with_realert_every_to_sensu(
    mock_send_event, mock_compose_monitoring_overrides_for_service
):
    mock_compose_monitoring_overrides_for_service.return_value = {"realert_every": 150}
    check_chronos_jobs.send_event(
        Mock(service="myservice", instance="myinstance", soa_dir="soadir"),
        status_code=0,
        output="this is great",
    )
    mock_send_event.assert_called_once_with(
        service="myservice",
        check_name="check_chronos_jobs.myservice.myinstance",
        overrides={"realert_every": 150},
        status=0,
        output="this is great\n\nThis check realerts every 2h30m.",
        soa_dir="soadir",
    )


def test_sensu_event_for_last_run_state_success():
    result = check_chronos_jobs.sensu_event_for_last_run_state(
        chronos_tools.LastRunState.Success
    )
    assert result == pysensu_yelp.Status.OK


def test_sensu_event_for_last_run_state_fail():
    result = check_chronos_jobs.sensu_event_for_last_run_state(
        chronos_tools.LastRunState.Fail
    )
    assert result == pysensu_yelp.Status.CRITICAL


def test_sensu_event_for_last_run_state_not_run():
    result = check_chronos_jobs.sensu_event_for_last_run_state(
        chronos_tools.LastRunState.NotRun
    )
    assert result is None


def test_sensu_event_for_last_run_state_invalid():
    with raises(ValueError):
        check_chronos_jobs.sensu_event_for_last_run_state(100)


@patch(
    "paasta_tools.check_chronos_jobs.chronos_tools.filter_chronos_jobs", autospec=True
)
def test_respect_latest_run_after_rerun(mock_filter_chronos_jobs):
    fake_job = {
        "name": "service1 test-job",
        "lastSuccess": "2016-07-26T22:00:00+00:00",
        "lastError": "2016-07-26T22:01:00+00:00",
    }
    mock_filter_chronos_jobs.side_effect = [[fake_job]]

    fake_configured_jobs = [("service1", "chronos_job")]
    fake_client = Mock()

    assert check_chronos_jobs.build_service_job_mapping(
        fake_client, fake_configured_jobs
    ) == {("service1", "chronos_job"): fake_job}
    fake_client.list.assert_called_once_with()

    # simulate a re-run where we now pass
    reran_job = {
        "name": "service1 test-job",
        "lastSuccess": "2016-07-26T22:12:00+00:00",
    }
    reran_job = chronos_rerun.set_tmp_naming_scheme(reran_job)
    mock_filter_chronos_jobs.side_effect = [[fake_job, reran_job]]
    fake_client = Mock()
    assert check_chronos_jobs.build_service_job_mapping(
        fake_client, fake_configured_jobs
    ) == {("service1", "chronos_job"): reran_job}
    fake_client.list.assert_called_once_with()


@patch(
    "paasta_tools.check_chronos_jobs.chronos_tools.filter_chronos_jobs", autospec=True
)
@patch(
    "paasta_tools.check_chronos_jobs.chronos_tools.filter_enabled_jobs", autospec=True
)
def test_build_service_job_mapping(mock_filter_enabled_jobs, mock_filter_chronos_jobs):
    services = ["service1", "service2", "service3"]
    latest_time = "2016-07-26T22:03:00+00:00"
    fake_jobs = [
        [
            {"name": service + " foo", "lastSuccess": "2016-07-26T22:02:00+00:00"},
            {"name": service + " foo", "lastError": latest_time},
            {"name": service + " foo"},
        ]
        for service in services
    ]
    fake_jobs.append(
        [
            {
                "name": "tmp-2017-06-13T123738942755 service4 foo",
                "lastError": latest_time,
            },
            {"name": "service4 foo", "lastSuccess": "2016-07-26T22:02:00+00:00"},
            {"name": "service4 foo"},
        ]
    )
    mock_filter_chronos_jobs.side_effect = fake_jobs
    mock_filter_enabled_jobs.side_effect = [[{}, {}, {}] for _ in range(0, 4)]

    fake_configured_jobs = [
        ("service1", "main"),
        ("service2", "main"),
        ("service3", "main"),
        ("service4", "main"),
    ]
    fake_client = Mock()

    expected = {
        ("service1", "main"): {"name": "service1 foo", "lastError": latest_time},
        ("service2", "main"): {"name": "service2 foo", "lastError": latest_time},
        ("service3", "main"): {"name": "service3 foo", "lastError": latest_time},
        ("service4", "main"): {
            "name": "tmp-2017-06-13T123738942755 service4 foo",
            "lastError": latest_time,
        },
    }
    assert (
        check_chronos_jobs.build_service_job_mapping(fake_client, fake_configured_jobs)
        == expected
    )
    fake_client.list.assert_called_once_with()


def test_message_for_status_fail(mock_chronos_job_config):
    actual = check_chronos_jobs.message_for_status(
        status=pysensu_yelp.Status.CRITICAL, chronos_job_config=mock_chronos_job_config
    )
    assert "Last run of job myservice.myinstance failed.\n" in actual
    # Assert that there are helpful action items in the output
    assert "paasta logs -s myservice -i myinstance -c mycluster\n" in actual
    assert "paasta status -s myservice -i myinstance -c mycluster -vv\n" in actual
    assert (
        "paasta rerun -s myservice -i myinstance -c mycluster -d {datetime}\n" in actual
    )


def test_message_for_status_success(mock_chronos_job_config):
    assert (
        check_chronos_jobs.message_for_status(
            pysensu_yelp.Status.OK, mock_chronos_job_config
        )
        == "Last run of job myservice%smyinstance Succeeded" % utils.SPACER
    )


def test_message_for_status_unknown(mock_chronos_job_config):
    assert (
        check_chronos_jobs.message_for_status(
            pysensu_yelp.Status.UNKNOWN, mock_chronos_job_config
        )
        == "Last run of job myservice%smyinstance Unknown" % utils.SPACER
    )


@patch("paasta_tools.check_chronos_jobs.job_is_stuck", autospec=True)
def test_sensu_message_status_ok(mock_job_is_stuck, mock_chronos_job_config):
    mock_job_is_stuck.return_value = False
    fake_job = {
        "name": "full_job_id",
        "disabled": False,
        "lastSuccess": "2016-07-26T22:02:00+00:00",
    }
    mock_chronos_job_config.get_schedule_interval_in_seconds.return_value = 1
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        mock_chronos_job_config, fake_job, 0
    )
    assert output == "Last run of job myservice.myinstance Succeeded"
    assert status == pysensu_yelp.Status.OK


@patch("paasta_tools.check_chronos_jobs.job_is_stuck", autospec=True)
def test_sensu_message_rerun_status_ok(mock_job_is_stuck, mock_chronos_job_config):
    mock_job_is_stuck.return_value = False
    fake_job = {
        "name": "tmp-2017-06-13T123738942755 full_job_id",
        "disabled": True,
        "lastSuccess": "2016-07-26T22:02:00+00:00",
    }
    mock_chronos_job_config.get_schedule_interval_in_seconds.return_value = 1
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        mock_chronos_job_config, fake_job, 0
    )
    assert output == "Last run of job myservice.myinstance Succeeded"
    assert status == pysensu_yelp.Status.OK


@patch("paasta_tools.check_chronos_jobs.job_is_stuck", autospec=True)
@patch("paasta_tools.check_chronos_jobs.message_for_status", autospec=True)
def test_sensu_message_status_fail(
    mock_message_for_status, mock_job_is_stuck, mock_chronos_job_config
):
    mock_job_is_stuck.return_value = False
    mock_message_for_status.return_value = "my failure message"
    fake_job = {
        "name": "full_job_id",
        "disabled": False,
        "lastError": "2016-07-26T22:03:00+00:00",
    }
    mock_chronos_job_config.get_schedule_interval_in_seconds.return_value = 1
    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        mock_chronos_job_config, fake_job, 0
    )
    assert output == "my failure message"
    assert status == pysensu_yelp.Status.CRITICAL


def test_sensu_message_status_no_run(mock_chronos_job_config):
    fake_job = None
    mock_chronos_job_config.get_disabled.return_value = False
    with patch(
        "paasta_tools.check_chronos_jobs.load_chronos_job_config",
        autospec=True,
        return_value=Mock(get_disabled=Mock(return_value=False)),
    ):
        output, status = check_chronos_jobs.sensu_message_status_for_jobs(
            mock_chronos_job_config, fake_job, 0
        )
    expected_output = "Warning: myservice.myinstance isn't in chronos at all, which means it may not be deployed yet"
    assert output == expected_output
    assert status == pysensu_yelp.Status.WARNING


def test_sensu_message_status_no_run_disabled(mock_chronos_job_config):
    fake_job = None
    chronos_client = None

    with patch(
        "paasta_tools.check_chronos_jobs.load_chronos_job_config",
        autospec=True,
        return_value=Mock(get_disabled=Mock(return_value=True)),
    ):
        output, status = check_chronos_jobs.sensu_message_status_for_jobs(
            mock_chronos_job_config, fake_job, chronos_client
        )
    assert output == "Job myservice.myinstance is disabled - ignoring status."
    assert status == pysensu_yelp.Status.OK


def test_sensu_message_status_disabled(mock_chronos_job_config):
    fake_job = {"name": "fake_job_id", "disabled": True}
    chronos_client = None

    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        mock_chronos_job_config, fake_job, chronos_client
    )
    assert output == "Job myservice.myinstance is disabled - ignoring status."
    assert status == pysensu_yelp.Status.OK


@patch("paasta_tools.check_chronos_jobs.job_is_stuck", autospec=True)
def test_sensu_message_status_stuck(mock_job_is_stuck, mock_chronos_job_config):
    fake_schedule = "* 1 * * *"
    chronos_client = None

    fake_job = {
        "name": "fake_job_id",
        "disabled": False,
        "lastSuccess": (datetime.now(pytz.utc) - timedelta(hours=25)).isoformat(),
        "schedule": fake_schedule,
    }
    mock_chronos_job_config.get_schedule_interval_in_seconds.return_value = 60 * 60 * 24
    mock_chronos_job_config.get_schedule.return_value = fake_schedule
    mock_chronos_job_config.get_schedule_time_zone.return_value = "America/Los_Angeles"

    output, status = check_chronos_jobs.sensu_message_status_for_jobs(
        mock_chronos_job_config, fake_job, chronos_client
    )
    assert status == pysensu_yelp.Status.CRITICAL
    assert (
        "Job myservice.myinstance with schedule {}" " (America/Los_Angeles)"
    ).format(fake_schedule) in output
    assert "paasta logs -s myservice -i myinstance -c mycluster" in output
    assert "and is configured to run every 24h." in output


def test_job_is_stuck_when_no_interval(mock_chronos_client):
    interval = None

    assert not check_chronos_jobs.job_is_stuck(
        "2016-07-26T22:03:00+00:00", interval, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_not_called()


def test_job_is_stuck_when_no_last_run(mock_chronos_client):
    assert not check_chronos_jobs.job_is_stuck(
        None, 2440, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_not_called()


def test_job_is_stuck_when_not_running_yet(mock_chronos_client):
    last_time_run = datetime.now(pytz.utc) - timedelta(hours=4)
    interval = 60 * 60 * 24

    assert not check_chronos_jobs.job_is_stuck(
        last_time_run.isoformat(), interval, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_not_called()


def test_job_is_stuck_when_still_running(mock_chronos_client):
    last_time_run = datetime.now(pytz.utc) - timedelta(hours=25)
    interval = 60 * 60 * 24
    mock_chronos_client.job_stat.return_value = {
        "histogram": {"99thPercentile": 60 * 60 * 8}
    }

    assert not check_chronos_jobs.job_is_stuck(
        last_time_run.isoformat(), interval, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_called_once_with("job_name")


def test_job_is_stuck_when_late(mock_chronos_client):
    last_time_run = datetime.now(pytz.utc) - timedelta(hours=25)
    interval = 60 * 60 * 24
    mock_chronos_client.job_stat.return_value = {
        "histogram": {"99thPercentile": 60 * 30}
    }

    assert check_chronos_jobs.job_is_stuck(
        last_time_run.isoformat(), interval, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_called_once_with("job_name")


def test_job_is_stuck_on_really_long_jobs(mock_chronos_client):
    """A daily job last run 48 hours ago should be considered stuck
    even if chronos says it normally takes 14 days to complete"""
    last_time_run = datetime.now(pytz.utc) - timedelta(hours=48)
    interval = 60 * 60 * 24
    mock_chronos_client.job_stat.return_value = {
        "histogram": {"99thPercentile": 60 * 60 * 24 * 14}
    }

    assert check_chronos_jobs.job_is_stuck(
        last_time_run.isoformat(), interval, mock_chronos_client, "job_name"
    )
    mock_chronos_client.job_stat.assert_called_once_with("job_name")


def test_guess_realert_every():
    assert (
        check_chronos_jobs.guess_realert_every(
            Mock(get_schedule_interval_in_seconds=Mock(return_value=60 * 60 * 3))
        )
        == 60 * 3
    )
    assert (
        check_chronos_jobs.guess_realert_every(
            Mock(get_schedule_interval_in_seconds=Mock(return_value=60 * 60 * 48))
        )
        == 60 * 24
    )


def test_message_for_stuck_job_timezone_is_none():
    fake_schedule = "* 1 * * *"
    output = check_chronos_jobs.message_for_stuck_job(
        "myservice",
        "myinstance",
        "mycluster",
        (datetime.now(pytz.utc) - timedelta(hours=25)).isoformat(),
        60 * 60 * 24,
        fake_schedule,
        None,
    )
    assert ("Job myservice.myinstance with schedule {}" " (UTC)").format(
        fake_schedule
    ) in output
    assert "paasta logs -s myservice -i myinstance -c mycluster" in output
    assert "and is configured to run every 24h." in output
