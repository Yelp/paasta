from typing import Sequence

import mock
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from kubernetes.client.rest import ApiException
from pytest import raises

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.setup_kubernetes_job import create_application_object
from paasta_tools.setup_kubernetes_job import main
from paasta_tools.setup_kubernetes_job import parse_args
from paasta_tools.setup_kubernetes_job import setup_kube_deployments
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable


def test_parse_args():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.argparse", autospec=True
    ) as mock_argparse:
        assert parse_args() == mock_argparse.ArgumentParser.return_value.parse_args()


def test_main():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.setup_kubernetes_job.KubeClient", autospec=True
    ) as mock_kube_client, mock.patch(
        "paasta_tools.setup_kubernetes_job.ensure_namespace", autospec=True
    ) as mock_ensure_namespace, mock.patch(
        "paasta_tools.setup_kubernetes_job.setup_kube_deployments", autospec=True
    ) as mock_setup_kube_deployments:
        mock_setup_kube_deployments.return_value = True
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        assert mock_ensure_namespace.called
        mock_setup_kube_deployments.assert_called_with(
            kube_client=mock_kube_client.return_value,
            service_instances=mock_parse_args.return_value.service_instance_list,
            soa_dir=mock_parse_args.return_value.soa_dir,
        )
        mock_setup_kube_deployments.return_value = False
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 1


def test_setup_kube_deployment_invalid_job_name():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object", autospec=True
    ) as mock_create_application_object, mock.patch(
        "paasta_tools.setup_kubernetes_job.list_all_deployments", autospec=True
    ) as mock_list_all_deployments:
        mock_client = mock.Mock()
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt", instance="f_m", git_sha="", config_sha="", replicas=0
            )
        ]
        mock_service_instances = ["kuruptf_m"]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert mock_create_application_object.call_count == 0


def test_ensure_pod_disruption_budget_create():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.create_pod_disruption_budget",
        autospec=True,
    ) as mock_create_pdr, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.max_unavailable",
        autospec=True,
        return_value=0,
    ):
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.min_available = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.Mock()

        mock_pdr = mock.Mock()
        mock_pdr.spec.min_available = 10

        mock_client.read_namespaced_pod_disruption_budget.side_effect = ApiException(
            status=404
        )

        app = mock.MagicMock()
        app.soa_config.get_desired_instances.return_value = 10
        app.item.service.return_value = "fake_service"
        app.item.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)
        mock_create_pdr.assert_called_once_with(
            kube_client=mock_client, pod_disruption_budget=mock_req_pdr
        )


def test_ensure_pod_disruption_budget_replaces_outdated():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.create_pod_disruption_budget",
        autospec=True,
    ) as mock_create_pdr, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.max_unavailable",
        autospec=True,
        return_value=0,
    ):
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.min_available = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.Mock()

        mock_pdr = mock.Mock()
        mock_pdr.spec.min_available = 10

        mock_client.read_namespaced_pod_disruption_budget.return_value = mock_pdr

        app = mock.MagicMock()
        app.soa_config.get_desired_instances.return_value = 10
        app.item.service.return_value = "fake_service"
        app.item.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)

        mock_client.policy.delete_namespaced_pod_disruption_budget.assert_called_once_with(
            name=mock_req_pdr.metadata.name,
            namespace=mock_req_pdr.metadata.namespace,
            body=V1DeleteOptions(),
        )
        mock_create_pdr.assert_called_once_with(
            kube_client=mock_client, pod_disruption_budget=mock_req_pdr
        )


def test_create_application_object():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.load_kubernetes_service_config_no_cache",
        autospec=True,
    ) as mock_load_kubernetes_service_config_no_cache, mock.patch(
        "paasta_tools.setup_kubernetes_job.load_system_paasta_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.Application.load_local_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.DeploymentWrapper", autospec=True
    ) as mock_deployment_wrapper, mock.patch(
        "paasta_tools.setup_kubernetes_job.StatefulSetWrapper", autospec=True
    ) as mock_stateful_set_wrapper:
        mock_kube_client = mock.Mock()
        mock_deploy = mock.MagicMock(spec=V1Deployment)
        service_config = mock.MagicMock()
        mock_load_kubernetes_service_config_no_cache.return_value = service_config
        service_config.format_kubernetes_app.return_value = mock_deploy

        # Create DeploymentWrapper
        create_application_object(
            kube_client=mock_kube_client,
            service="kurupt",
            instance="fm",
            soa_dir="/nail/blah",
        )

        mock_deployment_wrapper.assert_called_with(mock_deploy)

        mock_deploy = mock.MagicMock(spec=V1StatefulSet)
        service_config.format_kubernetes_app.return_value = mock_deploy
        # Create StatefulSetWrapper
        create_application_object(
            kube_client=mock_kube_client,
            service="kurupt",
            instance="fm",
            soa_dir="/nail/blah",
        )
        mock_stateful_set_wrapper.assert_called_with(mock_deploy)

        # Create object that is not statefulset/deployment
        with raises(Exception):
            service_config.format_kubernetes_app.return_value = mock.MagicMock()
            create_application_object(
                kube_client=mock_kube_client, service="kurupt", instance="fm"
            )

        mock_deployment_wrapper.reset_mock()
        mock_stateful_set_wrapper.reset_mock()
        mock_load_kubernetes_service_config_no_cache.side_effect = (
            NoDeploymentsAvailable
        )
        ret = create_application_object(
            kube_client=mock_kube_client,
            service="kurupt",
            instance="fm",
            soa_dir="/nail/blah",
        )
        assert ret == (True, None)
        assert not mock_deployment_wrapper.called
        assert not mock_stateful_set_wrapper.called

        mock_load_kubernetes_service_config_no_cache.side_effect = (
            NoConfigurationForServiceError
        )
        ret = create_application_object(
            kube_client=mock_kube_client,
            service="kurupt",
            instance="fm",
            soa_dir="/nail/blah",
        )

        assert ret == (False, None)
        assert not mock_deployment_wrapper.called
        assert not mock_stateful_set_wrapper.called

        mock_load_kubernetes_service_config_no_cache.side_effect = None
        mock_load_kubernetes_service_config_no_cache.return_value = mock.Mock(
            format_kubernetes_app=mock.Mock(
                side_effect=InvalidKubernetesConfig(Exception("Oh no!"), "kurupt", "fm")
            )
        )
        ret = create_application_object(
            kube_client=mock_kube_client,
            service="kurupt",
            instance="fm",
            soa_dir="/nail/blah",
        )

        assert ret == (False, None)
        assert not mock_deployment_wrapper.called
        assert not mock_stateful_set_wrapper.called


def test_setup_kube_deployment_create_update():
    fake_create = mock.MagicMock()
    fake_update = mock.MagicMock()

    def simple_create_application_object(kube_client, service, instance, soa_dir):
        fake_app = mock.MagicMock(spec=Application)
        fake_app.kube_deployment = KubeDeployment(
            service=service, instance=instance, git_sha="1", config_sha="1", replicas=1
        )
        fake_app.create = fake_create
        fake_app.update = fake_update
        return True, fake_app

    with mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object",
        autospec=True,
        side_effect=simple_create_application_object,
    ) as mock_create_application_object, mock.patch(
        "paasta_tools.setup_kubernetes_job.list_all_deployments", autospec=True
    ) as mock_list_all_deployments:
        mock_client = mock.Mock()
        # No instances created
        mock_service_instances: Sequence[str] = []
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert mock_create_application_object.call_count == 0

        # Create a new instance
        mock_service_instances = ["kurupt.fm"]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert fake_create.call_count == 1
        assert fake_update.call_count == 0

        # Update when gitsha changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        mock_service_instances = ["kurupt.fm"]
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt", instance="fm", git_sha="2", config_sha="1", replicas=1
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )

        assert fake_update.call_count == 1
        assert fake_create.call_count == 0

        # Update when configsha changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        mock_service_instances = ["kurupt.fm"]
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt", instance="fm", git_sha="1", config_sha="2", replicas=1
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 0

        # Update when replica changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        mock_service_instances = ["kurupt.fm"]
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt", instance="fm", git_sha="1", config_sha="1", replicas=2
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 0

        # Update one and Create One
        fake_create.reset_mock()
        fake_update.reset_mock()
        mock_service_instances = ["kurupt.fm", "kurupt.garage"]
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="garage",
                git_sha="2",
                config_sha="2",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 1

        # not create existing instances
        fake_create.reset_mock()
        fake_update.reset_mock()
        mock_service_instances = ["kurupt.garage"]
        mock_list_all_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="garage",
                git_sha="1",
                config_sha="1",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            soa_dir="/nail/blah",
        )
        assert fake_update.call_count == 0
        assert fake_create.call_count == 0
