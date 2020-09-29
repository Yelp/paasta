import mock

import paasta_tools.paastaapi.models as paastamodels
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
    mock_client.get_paasta_oapi_client.return_value = None
    return_code = get_service_autoscale_pause_time("cluster1")
    assert return_code == 1
    mock_client.get_paasta_oapi_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_oapi_client.return_value = mock.Mock(default=mock_api)
    mock_api.get_service_autoscaler_pause_with_http_info.return_value = (
        None,
        500,
        None,
    )
    return_code = get_service_autoscale_pause_time("cluster1")
    assert return_code == 2


@mock.patch("builtins.print", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.time", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_get_service_autoscale_pause_time_not(mock_client, mock_time, mock_print):
    mock_api = mock.Mock()
    mock_client.get_paasta_oapi_client.return_value = mock.Mock(default=mock_api)
    mock_api.get_service_autoscaler_pause_with_http_info.return_value = ("3", 200, None)
    mock_time.time.return_value = 4
    return_code = get_service_autoscale_pause_time("cluster1")
    mock_print.assert_called_with("Service autoscaler is not paused")
    assert return_code == 0


@mock.patch(
    "paasta_tools.autoscaling.pause_service_autoscaler.print_paused_message",
    autospec=True,
)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.time", autospec=True)
@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_get_service_autoscale_pause_time_paused(
    mock_client, mock_time, mock_print_paused_message
):
    mock_api = mock.Mock()
    mock_client.get_paasta_oapi_client.return_value = mock.Mock(default=mock_api)
    mock_api.get_service_autoscaler_pause_with_http_info.return_value = ("3", 200, None)
    mock_time.time.return_value = 2
    return_code = get_service_autoscale_pause_time("cluster1")
    mock_print_paused_message.assert_called_with(3.0)
    assert return_code == 0


@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
def test_update_service_autoscale_pause_time(mock_client):
    mock_client.get_paasta_oapi_client.return_value = None
    return_code = update_service_autoscale_pause_time("cluster1", "2")
    assert return_code == 1
    mock_client.get_paasta_oapi_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_oapi_client.return_value = mock.Mock(default=mock_api)
    mock_api.update_service_autoscaler_pause_with_http_info = mock_update = mock.Mock()
    mock_update.return_value = (None, 500, None)
    return_code = update_service_autoscale_pause_time("cluster1", "3")
    mock_update.assert_called_once_with(
        paastamodels.ServiceAutoscalerPauseJsonBody(minutes=3)
    )
    assert return_code == 2

    mock_update.return_value = (None, 200, None)
    return_code = update_service_autoscale_pause_time("cluster1", "2")
    assert return_code == 0


@mock.patch("paasta_tools.autoscaling.pause_service_autoscaler.client", autospec=True)
@mock.patch("paasta_tools.paastaapi.apis.DefaultApi", autospec=True)
def test_delete_service_autoscale_pause_time(mock_default_api, mock_client):
    mock_client.get_paasta_oapi_client.return_value = None
    return_code = delete_service_autoscale_pause_time("cluster1")
    assert return_code == 1
    mock_client.get_paasta_oapi_client.assert_called_with(
        cluster="cluster1", http_res=True
    )

    mock_api = mock.Mock()
    mock_client.get_paasta_oapi_client.return_value = mock.Mock(default=mock_api)
    mock_api.delete_service_autoscaler_pause_with_http_info = mock_delete = mock.Mock()
    mock_delete.return_value = (None, 500, None)
    return_code = delete_service_autoscale_pause_time("cluster1")
    mock_delete.assert_called_once_with()
    assert return_code == 2

    mock_delete.return_value = (None, 200, None)
    return_code = delete_service_autoscale_pause_time("cluster1")
    assert return_code == 0
