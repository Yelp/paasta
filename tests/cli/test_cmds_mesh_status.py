import mock
import pytest

import paasta_tools.paastaapi.models as paastamodels
from paasta_tools.cli.cmds import mesh_status
from paasta_tools.paastaapi import ApiException


@pytest.fixture
def fake_backend_location():
    return paastamodels.EnvoyLocation(
        name="fake_loc",
        running_backends_count=1,
        is_proxied_through_casper=True,
        backends=[
            paastamodels.EnvoyBackend(
                address="1.2.3.4",
                eds_health_status="Healthy",
                has_associated_task=True,
                hostname="5.6.7.8",
                port_value=9012,
                weight=50,
            ),
        ],
    )


@pytest.fixture
def mock_get_oapi_client(fake_backend_location):
    with mock.patch(
        "paasta_tools.cli.cmds.mesh_status.get_paasta_oapi_client", autospec=True
    ) as m:
        client = m.return_value
        client.service.mesh_instance.return_value = paastamodels.InstanceMeshStatus(
            service="fake_service",
            instance="fake_instance",
            envoy=paastamodels.EnvoyStatus(
                expected_backends_per_location=2,
                registration="fake_envoy_reg",
                locations=[fake_backend_location],
            ),
        )
        client.api_error = ApiException
        client.connection_error = ApiException
        client.timeout_error = ApiException
        yield m


@mock.patch("paasta_tools.cli.cmds.mesh_status.get_envoy_status_human", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.mesh_status.get_smartstack_status_human", autospec=True
)
def test_paasta_mesh_status_on_api_endpoint(
    mock_smtstk_status_human,
    mock_envoy_status_human,
    mock_get_oapi_client,
    fake_backend_location,
    system_paasta_config,
):
    envoy_output = mock.Mock()
    mock_envoy_status_human.return_value = [envoy_output]

    code, output = mesh_status.paasta_mesh_status_on_api_endpoint(
        cluster="fake_cluster",
        service="fake_service",
        instance="fake_instance",
        system_paasta_config=system_paasta_config,
    )

    assert code == 0
    assert output == [envoy_output]
    assert mock_smtstk_status_human.call_args_list == []
    assert mock_envoy_status_human.call_args_list == [
        mock.call("fake_envoy_reg", 2, [fake_backend_location]),
    ]


@mock.patch("paasta_tools.cli.cmds.mesh_status.get_envoy_status_human", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.mesh_status.get_smartstack_status_human", autospec=True
)
def test_paasta_mesh_status_on_api_endpoint_error(
    mock_smtstk_status_human,
    mock_envoy_status_human,
    mock_get_oapi_client,
    fake_backend_location,
    system_paasta_config,
):
    client = mock_get_oapi_client.return_value
    api_error = ApiException(status=405, reason="api error",)
    api_error.body = "fake_body"

    test_cases = [
        (api_error, 405, "fake_body"),
        (Exception(), 1, "Exception when talking to the API"),
    ]
    for exc, expected_code, expected_msg in test_cases:
        client.service.mesh_instance.side_effect = [exc]

        code, output = mesh_status.paasta_mesh_status_on_api_endpoint(
            cluster="fake_cluster",
            service="fake_service",
            instance="fake_instance",
            system_paasta_config=system_paasta_config,
        )

        assert expected_code == code
        assert expected_msg in output[0]

    assert mock_smtstk_status_human.call_args_list == []
    assert mock_envoy_status_human.call_args_list == []
