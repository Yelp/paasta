from typing import List
from typing import Tuple
from typing import Union

import mock
import pytest
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from pytest import raises

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.setup_kubernetes_job import create_application_object
from paasta_tools.setup_kubernetes_job import get_kubernetes_deployment_config
from paasta_tools.setup_kubernetes_job import get_service_instances_with_valid_names
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


def test_main_logging():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.metrics_lib.get_metrics_interface",
        autospec=True,
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.setup_kubernetes_job.KubeClient", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.get_kubernetes_deployment_config",
        autospec=True,
    ) as mock_service_instance_configs_list, mock.patch(
        "paasta_tools.setup_kubernetes_job.ensure_namespace", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.setup_kube_deployments", autospec=True
    ) as mock_setup_kube_deployments, mock.patch(
        "paasta_tools.setup_kubernetes_job.logging", autospec=True
    ) as mock_logging:
        mock_setup_kube_deployments.return_value = True
        mock_parse_args.return_value.verbose = True
        mock_kube_deploy_config = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="cluster",
            config_dict=KubernetesDeploymentConfigDict(),
            branch_dict=None,
        )
        mock_service_instance_configs_list.return_value = [
            (True, mock_kube_deploy_config)
        ]
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        mock_logging.basicConfig.assert_called_with(level=mock_logging.DEBUG)
        assert not mock_logging.getLogger.called

        mock_parse_args.return_value.verbose = False
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        mock_logging.basicConfig.assert_called_with(level=mock_logging.INFO)
        assert mock_logging.getLogger.called
        mock_logging.getLogger.assert_called_with("kazoo")


@pytest.mark.parametrize(
    "mock_kube_deploy_config, eks_flag",
    [
        (
            KubernetesDeploymentConfig(
                service="my-service",
                instance="my-instance",
                cluster="cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            False,
        ),
        (
            EksDeploymentConfig(
                service="my-service",
                instance="my-instance",
                cluster="cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            True,
        ),
    ],
)
def test_main(mock_kube_deploy_config, eks_flag):
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.metrics_lib.get_metrics_interface",
        autospec=True,
    ) as mock_get_metrics_interface, mock.patch(
        "paasta_tools.setup_kubernetes_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.setup_kubernetes_job.KubeClient", autospec=True
    ) as mock_kube_client, mock.patch(
        "paasta_tools.setup_kubernetes_job.get_kubernetes_deployment_config",
        autospec=True,
    ) as mock_service_instance_configs_list, mock.patch(
        "paasta_tools.setup_kubernetes_job.ensure_namespace", autospec=True
    ) as mock_ensure_namespace, mock.patch(
        "paasta_tools.setup_kubernetes_job.setup_kube_deployments", autospec=True
    ) as mock_setup_kube_deployments, mock.patch(
        "paasta_tools.setup_kubernetes_job.get_hpa_overrides",
        autospec=True,
        return_value={},
    ) as mock_get_hpa_overrides:
        mock_setup_kube_deployments.return_value = True
        mock_metrics_interface = mock_get_metrics_interface.return_value
        mock_parse_args.return_value.eks = eks_flag
        mock_service_instance_configs_list.return_value = [
            (True, mock_kube_deploy_config)
        ]
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        assert mock_ensure_namespace.called
        mock_setup_kube_deployments.assert_called_with(
            kube_client=mock_kube_client.return_value,
            cluster=mock_parse_args.return_value.cluster,
            soa_dir=mock_parse_args.return_value.soa_dir,
            rate_limit=mock_parse_args.return_value.rate_limit,
            service_instance_configs_list=mock_service_instance_configs_list.return_value,
            metrics_interface=mock_metrics_interface,
            eks=mock_parse_args.return_value.eks,
            hpa_overrides=mock_get_hpa_overrides.return_value,
        )
        mock_setup_kube_deployments.return_value = False
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 1

        mock_service_instance_configs_list.return_value = [(False, None)]
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 1


def test_get_service_instances_with_valid_names():
    mock_service_instances = ["kurupt.f_m"]
    ret = get_service_instances_with_valid_names(
        service_instances=mock_service_instances
    )

    assert ret == [("kurupt", "f_m", None, None)]


def test_main_invalid_job_name():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.setup_kubernetes_job.KubeClient", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.metrics_lib.get_metrics_interface",
        autospec=True,
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.load_system_paasta_config", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object", autospec=True
    ) as mock_create_application_object:
        mock_parse_args.return_value.cluster = "fake_cluster"
        mock_parse_args.return_value.soa_dir = "/etc/fake"
        mock_parse_args.return_value.service_instance_list = ["kuruptf_m"]
        with raises(SystemExit) as e:
            main()
        assert mock_create_application_object.call_count == 0
        assert e.value.code == 1


def test_get_kubernetes_deployment_config():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.load_kubernetes_service_config_no_cache",
        autospec=True,
    ) as mock_load_kubernetes_service_config_no_cache:

        mock_get_service_instances_with_valid_names = [
            ("kurupt", "instance", None, None)
        ]

        # Testing NoDeploymentsAvailable exception
        mock_load_kubernetes_service_config_no_cache.side_effect = (
            NoDeploymentsAvailable
        )
        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
        )
        assert ret == [(True, None)]

        # Testing NoConfigurationForServiceError exception
        mock_load_kubernetes_service_config_no_cache.side_effect = (
            NoConfigurationForServiceError
        )

        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
        )
        assert ret == [(False, None)]

        # Testing returning a KubernetesDeploymentConfig
        mock_kube_deploy = KubernetesDeploymentConfig(
            service="kurupt",
            instance="instance",
            cluster="fake_cluster",
            soa_dir="nail/blah",
            config_dict=KubernetesDeploymentConfigDict(),
            branch_dict=None,
        )
        mock_load_kubernetes_service_config_no_cache.side_effect = None
        mock_load_kubernetes_service_config_no_cache.return_value = mock_kube_deploy
        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
        )

        assert ret == [
            (
                True,
                KubernetesDeploymentConfig(
                    service="kurupt",
                    instance="instance",
                    cluster="fake_cluster",
                    soa_dir="nail/blah",
                    config_dict=KubernetesDeploymentConfigDict(),
                    branch_dict=None,
                ),
            )
        ]


def test_get_eks_deployment_config():
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.load_eks_service_config_no_cache",
        autospec=True,
    ) as mock_load_eks_service_config_no_cache:

        mock_get_service_instances_with_valid_names = [
            ("kurupt", "instance", None, None)
        ]

        # Testing NoDeploymentsAvailable exception
        mock_load_eks_service_config_no_cache.side_effect = NoDeploymentsAvailable
        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
            eks=True,
        )
        assert ret == [(True, None)]

        # Testing NoConfigurationForServiceError exception
        mock_load_eks_service_config_no_cache.side_effect = (
            NoConfigurationForServiceError
        )

        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
            eks=True,
        )
        assert ret == [(False, None)]

        # Testing returning a KubernetesDeploymentConfig
        mock_kube_deploy = EksDeploymentConfig(
            service="kurupt",
            instance="instance",
            cluster="fake_cluster",
            soa_dir="nail/blah",
            config_dict=KubernetesDeploymentConfigDict(),
            branch_dict=None,
        )
        mock_load_eks_service_config_no_cache.side_effect = None
        mock_load_eks_service_config_no_cache.return_value = mock_kube_deploy
        ret = get_kubernetes_deployment_config(
            service_instances_with_valid_names=mock_get_service_instances_with_valid_names,
            cluster="fake_cluster",
            soa_dir="nail/blah",
            eks=True,
        )

        assert ret == [
            (
                True,
                EksDeploymentConfig(
                    service="kurupt",
                    instance="instance",
                    cluster="fake_cluster",
                    soa_dir="nail/blah",
                    config_dict=KubernetesDeploymentConfigDict(),
                    branch_dict=None,
                ),
            )
        ]


@pytest.mark.parametrize(
    "eks_flag, mock_service_config",
    [
        (
            "False",
            KubernetesDeploymentConfig(
                service="kurupt",
                instance="instance",
                cluster="fake_cluster",
                soa_dir="nail/blah",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
        ),
        (
            "True",
            EksDeploymentConfig(
                service="kurupt",
                instance="instance",
                cluster="fake_cluster",
                soa_dir="nail/blah",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
        ),
    ],
)
def test_create_application_object(eks_flag, mock_service_config):
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.load_system_paasta_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.Application.load_local_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.DeploymentWrapper",
        autospec=True,
    ) as mock_deployment_wrapper, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.StatefulSetWrapper",
        autospec=True,
    ) as mock_stateful_set_wrapper:
        mock_deploy = mock.MagicMock(spec=V1Deployment)
        service_config = mock.MagicMock(spec=mock_service_config)
        service_config.format_kubernetes_app.return_value = mock_deploy

        # Create DeploymentWrapper
        create_application_object(
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            service_instance_config=service_config,
            eks=eks_flag,
        )

        mock_deployment_wrapper.assert_called_with(mock_deploy, hpa_override=None)

        mock_deploy = mock.MagicMock(spec=V1StatefulSet)
        service_config.format_kubernetes_app.return_value = mock_deploy

        # Create StatefulSetWrapper
        create_application_object(
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            service_instance_config=service_config,
            eks=eks_flag,
        )
        mock_stateful_set_wrapper.assert_called_with(mock_deploy)

        # Create object that is not statefulset/deployment
        with raises(Exception):
            service_config.format_kubernetes_app.return_value = mock.MagicMock()
            create_application_object(
                cluster="fake_cluster",
                soa_dir="/nail/blah",
                service_instance_config=service_config,
                eks=eks_flag,
            )

        mock_deployment_wrapper.reset_mock()
        mock_stateful_set_wrapper.reset_mock()

        service_config.format_kubernetes_app.side_effect = InvalidKubernetesConfig(
            Exception("Oh no!"), "kurupt", "fm"
        )
        ret = create_application_object(
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            service_instance_config=service_config,
            eks=eks_flag,
        )

        assert ret == (False, None)
        assert not mock_deployment_wrapper.called
        assert not mock_stateful_set_wrapper.called


@pytest.mark.parametrize(
    "mock_kube_deploy_config, eks_flag",
    [
        (
            KubernetesDeploymentConfig(
                service="kurupt",
                instance="fm",
                cluster="fake_cluster",
                soa_dir="/nail/blah",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            False,
        ),
        (
            EksDeploymentConfig(
                service="kurupt",
                instance="fm",
                cluster="fake_cluster",
                soa_dir="/nail/blah",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            True,
        ),
    ],
)
def test_setup_kube_deployment_create_update(mock_kube_deploy_config, eks_flag):
    fake_create = mock.MagicMock()
    fake_update = mock.MagicMock()
    fake_update_related_api_objects = mock.MagicMock()

    def simple_create_application_object(
        cluster,
        soa_dir,
        service_instance_config,
        eks,
        hpa_override,
    ):
        fake_app = mock.MagicMock(spec=Application)
        fake_app.kube_deployment = KubeDeployment(
            service=service_instance_config.service,
            instance=service_instance_config.instance,
            namespace="paasta",
            git_sha="1",
            image_version="extrastuff-1",
            config_sha="1",
            replicas=1,
        )
        fake_app.create = fake_create
        fake_app.update = fake_update
        fake_app.update_related_api_objects = fake_update_related_api_objects
        fake_app.item = None
        fake_app.soa_config = KubernetesDeploymentConfig(
            service=service_instance_config.service,
            cluster=cluster,
            instance=service_instance_config.instance,
            config_dict=service_instance_config.config_dict,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        fake_app.__str__ = lambda app: "fake_app"
        return True, fake_app

    with mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object",
        autospec=True,
        side_effect=simple_create_application_object,
    ) as mock_create_application_object, mock.patch(
        "paasta_tools.setup_kubernetes_job.list_all_paasta_deployments", autospec=True
    ) as mock_list_all_paasta_deployments, mock.patch(
        "paasta_tools.setup_kubernetes_job.log", autospec=True
    ) as mock_log_obj, mock.patch(
        "paasta_tools.setup_kubernetes_job.metrics_lib.NoMetrics", autospec=True
    ) as mock_no_metrics, mock.patch(
        "paasta_tools.setup_kubernetes_job.get_kubernetes_deployment_config",
        autospec=True,
    ):
        mock_client = mock.Mock()
        # No instances created
        mock_service_instance_configs_list: List[
            Tuple[bool, Union[KubernetesDeploymentConfig, EksDeploymentConfig]]
        ] = []
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
        )
        assert mock_create_application_object.call_count == 0
        assert fake_update.call_count == 0
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 0
        assert mock_log_obj.info.call_count == 0
        mock_log_obj.info.reset_mock()

        # Create a new instance
        mock_service_instance_configs_list = [(True, mock_kube_deploy_config)]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            metrics_interface=mock_no_metrics,
            eks=eks_flag,
        )
        assert fake_create.call_count == 1
        assert fake_update.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        assert mock_no_metrics.emit_event.call_count == 1
        mock_log_obj.info.reset_mock()
        mock_no_metrics.reset_mock()

        # Skipping downthenup instance cuz of existing_apps
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="fm",
                git_sha="2",
                namespace="paastasvc-kurupt",
                image_version="extrastuff-1",
                config_sha="1",
                replicas=1,
            )
        ]
        mock_downthenup_kube_deploy_config = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            config_dict=KubernetesDeploymentConfigDict(bounce_method="downthenup"),
            branch_dict=None,
        )
        mock_service_instance_configs_list = [
            (True, mock_downthenup_kube_deploy_config)
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            metrics_interface=mock_no_metrics,
            eks=eks_flag,
        )
        assert fake_create.call_count == 0
        assert fake_update.call_count == 0
        assert fake_update_related_api_objects.call_count == 0
        assert mock_no_metrics.emit_event.call_count == 0
        mock_log_obj.info.reset_mock()
        mock_no_metrics.reset_mock()

        # Update when gitsha changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="fm",
                git_sha="2",
                namespace="paasta",
                image_version="extrastuff-1",
                config_sha="1",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            metrics_interface=mock_no_metrics,
            eks=eks_flag,
        )

        assert fake_update.call_count == 1
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        mock_no_metrics.emit_event.assert_called_with(
            name="deploy",
            dimensions={
                "paasta_cluster": "fake_cluster",
                "paasta_service": "kurupt",
                "paasta_instance": "fm",
                "paasta_namespace": "paasta",
                "deploy_event": "update",
            },
        )
        mock_log_obj.info.reset_mock()
        mock_no_metrics.reset_mock()

        # Update when image_version changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="fm",
                git_sha="1",
                namespace="paasta",
                image_version="extrastuff-2",
                config_sha="1",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            eks=eks_flag,
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        mock_log_obj.info.reset_mock()

        # Update when configsha changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="fm",
                git_sha="1",
                namespace="paasta",
                image_version="extrastuff-1",
                config_sha="2",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            eks=eks_flag,
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        mock_log_obj.info.reset_mock()

        # Update when replica changed
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="fm",
                git_sha="1",
                namespace="paasta",
                image_version="extrastuff-1",
                config_sha="1",
                replicas=2,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            eks=eks_flag,
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        mock_log_obj.info.reset_mock()

        # Update one and Create One
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_kube_deploy_config_new = KubernetesDeploymentConfig(
            service="kurupt",
            instance="garage",
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            config_dict=KubernetesDeploymentConfigDict(),
            branch_dict=None,
        )
        mock_service_instance_configs_list = [
            (True, mock_kube_deploy_config),
            (True, mock_kube_deploy_config_new),
        ]
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="garage",
                git_sha="2",
                namespace="paasta",
                image_version="extrastuff-1",
                config_sha="2",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            eks=eks_flag,
        )
        assert fake_update.call_count == 1
        assert fake_create.call_count == 1
        assert fake_update_related_api_objects.call_count == 2
        mock_log_obj.info.reset_mock()

        # Always attempt to update related API objects
        fake_create.reset_mock()
        fake_update.reset_mock()
        fake_update_related_api_objects.reset_mock()
        mock_service_instance_configs_list = [(True, mock_kube_deploy_config_new)]
        mock_list_all_paasta_deployments.return_value = [
            KubeDeployment(
                service="kurupt",
                instance="garage",
                git_sha="1",
                namespace="paasta",
                image_version="extrastuff-1",
                config_sha="1",
                replicas=1,
            )
        ]
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            eks=eks_flag,
        )
        assert fake_update.call_count == 0
        assert fake_create.call_count == 0
        assert fake_update_related_api_objects.call_count == 1
        assert mock_log_obj.info.call_args_list[0] == mock.call(
            "fake_app is up-to-date!"
        )


@pytest.mark.parametrize(
    "mock_kube_deploy_config_fm, mock_kube_deploy_config_garage, mock_kube_deploy_config_radio, eks_flag",
    [
        (
            KubernetesDeploymentConfig(
                service="kurupt",
                instance="fm",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            KubernetesDeploymentConfig(
                service="kurupt",
                instance="garage",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            KubernetesDeploymentConfig(
                service="kurupt",
                instance="radio",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            False,
        ),
        (
            EksDeploymentConfig(
                service="kurupt",
                instance="fm",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            EksDeploymentConfig(
                service="kurupt",
                instance="garage",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            EksDeploymentConfig(
                service="kurupt",
                instance="radio",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            True,
        ),
    ],
)
def test_setup_kube_deployments_rate_limit(
    mock_kube_deploy_config_fm,
    mock_kube_deploy_config_garage,
    mock_kube_deploy_config_radio,
    eks_flag,
):
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object",
        autospec=True,
    ) as mock_create_application_object, mock.patch(
        "paasta_tools.setup_kubernetes_job.list_all_paasta_deployments", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.log", autospec=True
    ) as mock_log_obj:
        mock_client = mock.Mock()
        mock_service_instance_configs_list: List[
            Tuple[bool, Union[KubernetesDeploymentConfig, EksDeploymentConfig]]
        ] = [
            (True, mock_kube_deploy_config_fm),
            (True, mock_kube_deploy_config_garage),
            (True, mock_kube_deploy_config_radio),
        ]
        fake_app = mock.Mock(create=mock.Mock())
        mock_create_application_object.return_value = (True, fake_app)

        # Rate limit: 2 calls allowed
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            rate_limit=2,
            eks=eks_flag,
        )
        assert fake_app.create.call_count == 2
        mock_log_obj.info.assert_any_call(
            "Not doing any further updates as we reached the limit (2)"
        )

        # No rate limit
        fake_app.reset_mock()
        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            rate_limit=0,
            eks=eks_flag,
        )
        assert fake_app.create.call_count == 3


@pytest.mark.parametrize(
    "mock_kube_deploy_config_fake, mock_kube_deploy_config_mock, eks_flag",
    [
        (
            KubernetesDeploymentConfig(
                service="fake",
                instance="instance",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            KubernetesDeploymentConfig(
                service="mock",
                instance="instance",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            False,
        ),
        (
            EksDeploymentConfig(
                service="fake",
                instance="instance",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            EksDeploymentConfig(
                service="mock",
                instance="instance",
                cluster="fake_cluster",
                config_dict=KubernetesDeploymentConfigDict(),
                branch_dict=None,
            ),
            True,
        ),
    ],
)
def test_setup_kube_deployments_skip_malformed_apps(
    mock_kube_deploy_config_fake, mock_kube_deploy_config_mock, eks_flag
):
    with mock.patch(
        "paasta_tools.setup_kubernetes_job.create_application_object",
        autospec=True,
    ) as mock_create_application_object, mock.patch(
        "paasta_tools.setup_kubernetes_job.list_all_paasta_deployments", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_job.log", autospec=True
    ) as mock_log_obj:
        mock_client = mock.Mock()
        mock_service_instance_configs_list: List[
            Tuple[bool, Union[KubernetesDeploymentConfig, EksDeploymentConfig]]
        ] = [
            (True, mock_kube_deploy_config_fake),
            (True, mock_kube_deploy_config_mock),
        ]
        fake_app = mock.Mock(create=mock.Mock())
        fake_app.create = mock.Mock(
            side_effect=[Exception("Kaboom!"), mock.Mock(create=mock.Mock())]
        )
        fake_app.__str__ = mock.Mock(return_value="fake_app")
        mock_create_application_object.return_value = (True, fake_app)

        setup_kube_deployments(
            kube_client=mock_client,
            service_instance_configs_list=mock_service_instance_configs_list,
            cluster="fake_cluster",
            soa_dir="/nail/blah",
            rate_limit=0,
            eks=eks_flag,
        )
        assert fake_app.create.call_count == 2
        assert len(mock_log_obj.exception.call_args_list) == 1
        assert mock_log_obj.exception.call_args_list[0] == mock.call(
            "Error while processing: fake_app"
        )
