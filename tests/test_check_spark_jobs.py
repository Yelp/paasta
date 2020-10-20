import datetime

import mock
import pytest

import paasta_tools.check_spark_jobs as check_spark_jobs
from paasta_tools.mesos.framework import Framework


@pytest.fixture
def mock_smtp():
    with mock.patch(
        "paasta_tools.check_spark_jobs.smtplib", autospec=True
    ) as mock_smtp:
        yield mock_smtp


@pytest.fixture
def mock_current_time():
    return datetime.datetime(2019, 4, 3, 0, 0, 0)


@pytest.fixture(autouse=True)
def mock_datetime(mock_current_time):
    with mock.patch(
        "paasta_tools.check_spark_jobs.datetime", autospec=True
    ) as mock_time:
        mock_time.datetime.now.return_value = mock_current_time
        mock_time.datetime.fromtimestamp = datetime.datetime.fromtimestamp
        mock_time.timedelta = datetime.timedelta
        yield mock_time


@pytest.fixture
def mock_get_frameworks(mock_current_time):
    mock_current_timestamp = mock_current_time.timestamp()
    mock_frameworks = [
        {
            "id": "uuid1",
            "name": "not_spark_25_hours",
            "user": "test_user",
            "active": True,
            "registered_time": mock_current_timestamp - 60 * 60 * 25,
        },
        {
            "id": "uuid2",
            "name": "spark_2_hours",
            "principal": "spark",
            "user": "test_user",
            "active": True,
            "webui_url": "url2",
            "registered_time": mock_current_timestamp - 60 * 60 * 2,
        },
        {
            "id": "uuid3",
            "name": "spark_25_hours",
            "principal": "spark",
            "user": "test_user",
            "active": True,
            "webui_url": "url3",
            "registered_time": mock_current_timestamp - 60 * 60 * 25,
        },
        {
            "id": "uuid4",
            "name": "spark_25_hours_inactive",
            "principal": "spark",
            "user": "test_user",
            "active": False,
            "webui_url": "url4",
            "registered_time": mock_current_timestamp - 60 * 60 * 25,
        },
    ]
    with mock.patch(
        "paasta_tools.check_spark_jobs.mesos_tools.get_all_frameworks", autospec=True
    ) as mock_get_frameworks:
        mock_get_frameworks.return_value = [
            Framework(config) for config in mock_frameworks
        ]
        yield mock_get_frameworks


@pytest.mark.parametrize(
    "properties,expected",
    [
        (None, None),
        ([["something_else", "value"]], None),
        (
            [["something_else", "value"], ["spark.executorEnv.PAASTA_SERVICE", "foo"]],
            "foo",
        ),
        ([["spark.executorEnv.PAASTA_SERVICE", "jupyterhub_foo"]], "foo"),
    ],
)
def test_guess_service(properties, expected):
    assert check_spark_jobs.guess_service(properties) == expected


@mock.patch("paasta_tools.check_spark_jobs.get_spark_properties", autospec=True)
@mock.patch("paasta_tools.check_spark_jobs.guess_service", autospec=True)
def test_get_matching_framework_info(
    mock_guess_service, mock_get_spark_properties, mock_get_frameworks
):
    mock_get_spark_properties.return_value = None

    more_than_20_hours = check_spark_jobs.get_matching_framework_info(min_hours=20)
    assert len(more_than_20_hours) == 1
    assert more_than_20_hours == [
        {
            "id": "uuid3",
            "name": "spark_25_hours",
            "webui_url": "url3",
            "service": mock_guess_service.return_value,
            "user": "test_user",
            "time_running": "1 day, 1:00:00",
        }
    ]

    more_than_one_hour = check_spark_jobs.get_matching_framework_info(min_hours=1)
    result_names = [result["name"] for result in more_than_one_hour]
    assert result_names == ["spark_2_hours", "spark_25_hours"]


@pytest.mark.parametrize(
    "framework_user,framework_name,expected",
    [
        ("me", "paasta_spark_run_something", "me"),
        ("root", "Custom Spark App", None),
        ("root", "jupyterhub_bill-search-learning_bill_39904_1234", "bill"),
    ],
)
def test_email_user(mock_smtp, framework_user, framework_name, expected):
    info = {
        "id": "id1",
        "name": framework_name,
        "webui_url": "url1",
        "service": "something",
        "user": framework_user,
        "time_running": "1 day, 03:32:00",
    }
    check_spark_jobs.email_user(info, "test.com")

    mock_send_message = mock_smtp.SMTP.return_value.__enter__.return_value.send_message
    if expected:
        assert mock_send_message.call_count == 1
        msg = mock_send_message.call_args[0][0]
        assert msg["To"] == f"{expected}@test.com"
    else:
        assert mock_send_message.call_count == 0


@mock.patch("paasta_tools.check_spark_jobs.email_user", autospec=True)
@mock.patch("paasta_tools.check_spark_jobs.list_services", autospec=True)
@mock.patch("paasta_tools.check_spark_jobs.get_matching_framework_info", autospec=True)
@mock.patch("paasta_tools.check_spark_jobs.update_check_status", autospec=True)
@pytest.mark.parametrize("no_notify", [True, False])
def test_report_spark_jobs(
    mock_check, mock_get_info, mock_list_services, mock_email_user, no_notify
):
    mock_list_services.return_value = ["service1", "service2", "other_service"]
    mock_get_info.return_value = [
        {
            "id": "uuid1",
            "name": "spark1",
            "webui_url": "url1",
            "service": "service1",
            "user": "test_user",
            "time_running": "7:00:00",
        },
        {
            "id": "uuid2",
            "name": "spark2",
            "webui_url": "url2",
            "service": "service2",
            "user": "test_user",
            "time_running": "7:00:00",
        },
        {
            "id": "uuid3",
            "name": "spark3",
            "webui_url": "url3",
            "service": "service2",
            "user": "test_user",
            "time_running": "7:00:00",
        },
        {
            "id": "uuid3",
            "name": "spark3",
            "webui_url": "url3",
            "service": "service_dne",
            "user": "test_user",
            "time_running": "7:00:00",
        },
    ]
    assert check_spark_jobs.report_spark_jobs(1, no_notify, "test.com") == 1
    assert mock_get_info.call_args_list == [mock.call(min_hours=1)]
    if no_notify:
        assert mock_check.call_count == 0
    else:
        assert sorted(mock_check.call_args_list) == sorted(
            [
                mock.call("service1", mock.ANY, 1),
                mock.call("service2", mock.ANY, 1),
                mock.call("other_service", mock.ANY, 0),
            ]
        )
        assert mock_email_user.call_args_list == [
            mock.call(info, "test.com") for info in mock_get_info.return_value
        ]
