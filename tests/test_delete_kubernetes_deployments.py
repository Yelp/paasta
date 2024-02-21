import mock
from pytest import raises

from paasta_tools.delete_kubernetes_deployments import get_deployment_names_from_list
from paasta_tools.delete_kubernetes_deployments import main
from paasta_tools.utils import InvalidJobNameError


def test_main():
    with mock.patch(
        "paasta_tools.delete_kubernetes_deployments.get_deployment_names_from_list",
        autospec=True,
    ) as mock_get_deployment_names_from_list, mock.patch(
        "paasta_tools.delete_kubernetes_deployments.delete_deployment",
        autospec=True,
    ) as mock_delete_deployment, mock.patch(
        "paasta_tools.delete_kubernetes_deployments.KubeClient", autospec=True
    ) as mock_kube_client, mock.patch(
        "paasta_tools.delete_kubernetes_deployments.ensure_namespace",
        autospec=True,
    ) as mock_ensure_namespace:
        # Test main() success
        mock_get_deployment_names_from_list.return_value = ["fake_pcm_deployment"]
        with raises(SystemExit) as e:
            main(args=["fake_pcm_deployment"])
        assert e.value.code == 0
        assert mock_ensure_namespace.called
        mock_delete_deployment.assert_called_with(
            kube_client=mock_kube_client.return_value,
            deployment_name="fake_pcm_deployment",
            namespace="paasta",
        )

        # Test main() failed
        mock_delete_deployment.side_effect = Exception("Delete Error")
        with raises(SystemExit) as e:
            main(args=["fake_pcm_deployment"])
        assert e.value.code == 1


def test_get_deployment_names_from_list():
    with mock.patch(
        "paasta_tools.delete_kubernetes_deployments.decompose_job_id", autospec=True
    ) as mock_decompose_job_id:
        # Test get_deployment_names_from_list() success
        mock_decompose_job_id.return_value = (
            "fake-service",
            "fake_instance",
            "fake_hash",
            "fake_hash",
        )
        output = get_deployment_names_from_list(["fake-service.fake_instance"])
        assert output[0] == "fake-service-fake--instance"

        # Test get_deployment_names_from_list() failed
        mock_decompose_job_id.side_effect = InvalidJobNameError()
        with raises(SystemExit) as e:
            get_deployment_names_from_list(["fake-service.fake_instance"])
        assert e.value.code == 1
