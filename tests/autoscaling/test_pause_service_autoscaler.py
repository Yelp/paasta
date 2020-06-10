import mock

from paasta_tools.autoscaling.pause_service_autoscaler import (
    delete_service_autoscale_pause_time,
)
from paasta_tools.autoscaling.pause_service_autoscaler import (
    get_service_autoscale_pause_time,
)
from paasta_tools.autoscaling.pause_service_autoscaler import (
    update_service_autoscale_pause_time,
)


@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_get_service_autoscale_pause_time_error(mock_client):
    mock_client.get_paasta_api_client.return_value = None
    return_code = get_service_autoscale_pause_time("cluster1")
    assert return_code == 1
    mock_client.get_paasta_api_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_http_result = mock.Mock(status_code=500)
    mock_result = mock.Mock(return_value=(None, mock_http_result))
    mock_api.service_autoscaler.get_service_autoscaler_pause.return_value = mock.Mock(
        result=mock_result
    )
    return_code = get_service_autoscale_pause_time("cluster1")
    assert return_code == 2


@mock.patch("builtins.print", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.time", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_get_service_autoscale_pause_time_not(mock_client, mock_time, mock_print):
    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_http_result = mock.Mock(status_code=200)
    mock_result = mock.Mock(return_value=("3", mock_http_result))
    mock_api.service_autoscaler.get_service_autoscaler_pause.return_value = mock.Mock(
        result=mock_result
    )

    mock_time.time.return_value = 4
    return_code = get_service_autoscale_pause_time("cluster1")
    mock_print.assert_called_with("Service autoscaler is not paused")
    assert return_code == 0


@mock.patch("builtins.print", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.time", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_get_service_autoscale_pause_time_paused(mock_client, mock_time, mock_print):
    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_http_result = mock.Mock(status_code=200)
    mock_result = mock.Mock(return_value=("3", mock_http_result))
    mock_api.service_autoscaler.get_service_autoscaler_pause.return_value = mock.Mock(
        result=mock_result
    )

    mock_time.time.return_value = 2
    return_code = get_service_autoscale_pause_time("cluster1")
    mock_print.assert_called_with(
        "Service autoscaler is paused until 1970-01-01 00:00:03 UTC"
    )
    assert return_code == 0


@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_update_service_autoscale_pause_time(mock_client):
    mock_client.get_paasta_api_client.return_value = None
    return_code = update_service_autoscale_pause_time("cluster1", "2")
    assert return_code == 1
    mock_client.get_paasta_api_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_http_result = mock.Mock(status_code=500)
    mock_result = mock.Mock(return_value=(None, mock_http_result))
    update_mock = mock.Mock(return_value=mock.Mock(result=mock_result))
    mock_api.service_autoscaler.update_service_autoscaler_pause = update_mock
    return_code = update_service_autoscale_pause_time("cluster1", "3")
    update_mock.assert_called_once_with(json_body={"minutes": "3"})
    assert return_code == 2

    mock_http_result = mock.Mock(status_code=200)
    mock_result = mock.Mock(return_value=(None, mock_http_result))
    mock_api.service_autoscaler.update_service_autoscaler_pause.return_value = mock.Mock(
        result=mock_result
    )
    return_code = update_service_autoscale_pause_time("cluster1", "2")
    assert return_code == 0


@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_delete_service_autoscale_pause_time(mock_client):
    mock_client.get_paasta_api_client.return_value = None
    return_code = delete_service_autoscale_pause_time("cluster1")
    assert return_code == 1
    mock_client.get_paasta_api_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_api_client.return_value = mock_api
    mock_http_result = mock.Mock(status_code=500)
    mock_result = mock.Mock(return_value=(None, mock_http_result))
    delete_mock = mock.Mock(return_value=mock.Mock(result=mock_result))
    mock_api.service_autoscaler.delete_service_autoscaler_pause = delete_mock
    return_code = delete_service_autoscale_pause_time("cluster1")
    delete_mock.assert_called_once_with()
    assert return_code == 2

    mock_http_result = mock.Mock(status_code=200)
    mock_result = mock.Mock(return_value=(None, mock_http_result))
    mock_api.service_autoscaler.delete_service_autoscaler_pause.return_value = mock.Mock(
        result=mock_result
    )
    return_code = delete_service_autoscale_pause_time("cluster1")
    assert return_code == 0
