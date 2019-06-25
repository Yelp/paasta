import mock
import pytest
from bravado.exception import HTTPBadRequest

from paasta_tools.api.auth_decorator import AuthFutureDecorator


def test_auth_future_decorator():
    with mock.patch(
        'paasta_tools.api.client.renew_issue_cert',
        autospec=True,
    ) as mock_renew, mock.patch(
        'paasta_tools.api.auth_decorator.load_system_paasta_config',
        autospec=True,
    ):
        mock_future = mock.Mock()
        afd = AuthFutureDecorator(mock_future, "westeros-prod")
        assert afd.result() == mock_future.result.return_value
        assert not mock_renew.called

        mock_result = mock.Mock()
        mock_future.result.side_effect = [HTTPBadRequest(mock.Mock(status_code=400, text='Some problem')), mock_result]
        with pytest.raises(HTTPBadRequest):
            afd.result()
        assert not mock_renew.called

        mock_future.result.side_effect = [HTTPBadRequest(mock.Mock(status_code=400, text='SSL problem')), mock_result]
        assert afd.result() is mock_result
        assert mock_renew.called
