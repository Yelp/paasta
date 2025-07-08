import mock

from paasta_tools import check_services_replication_tools


def test_main_kubernetes():
    with mock.patch(
        "paasta_tools.check_services_replication_tools.check_services_replication",
        autospec=True,
    ) as mock_check_services_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.check_services_replication_tools.get_kubernetes_pods_and_nodes",
        autospec=True,
        return_value=([mock.Mock()], [mock.Mock()]),
    ), mock.patch(
        "paasta_tools.check_services_replication_tools.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.check_services_replication_tools.yelp_meteorite",
        autospec=True,
    ) as mock_yelp_meteorite, mock.patch(
        "paasta_tools.check_services_replication_tools.metrics_lib.system_timer",
        autospec=True,
    ) as mock_system_timer, mock.patch(
        "paasta_tools.check_services_replication_tools.sys.exit",
        autospec=True,
    ) as mock_sys_exit:
        mock_parse_args.return_value.under_replicated_crit_pct = 5
        mock_parse_args.return_value.min_count_critical = 1
        mock_parse_args.return_value.dry_run = False

        mock_check_services_replication.return_value = (6, 100)

        check_services_replication_tools.main(
            instance_type_class=None, check_service_replication=None, namespace="test"
        )
        assert mock_check_services_replication.called

        mock_yelp_meteorite.create_gauge.assert_called_once_with(
            "paasta.pct_services_under_replicated",
            {
                "paasta_cluster": mock_load_system_paasta_config.return_value.get_cluster.return_value,
                "scheduler": "kubernetes",
            },
        )
        mock_gauge = mock_yelp_meteorite.create_gauge.return_value
        mock_gauge.set.assert_called_once_with(6)

        mock_timer = mock_system_timer.return_value
        assert mock_timer.start.called
        mock_timer.stop.assert_called_once_with(tmp_dimensions={"result": 2})
        mock_sys_exit.assert_called_once_with(2)


def test_check_services_replication():
    soa_dir = "anw"
    instance_config = mock.Mock()
    instance_config.get_docker_image.return_value = True
    with mock.patch(
        "paasta_tools.check_services_replication_tools.list_services",
        autospec=True,
        return_value=["a"],
    ), mock.patch(
        "paasta_tools.check_kubernetes_services_replication.check_kubernetes_pod_replication",
        autospec=True,
    ) as mock_check_service_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_paasta_service_config_loader, mock.patch(
        "paasta_tools.check_services_replication_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_kube_client.return_value = mock.Mock()
        mock_paasta_service_config_loader.return_value.instance_configs.return_value = [
            instance_config
        ]
        mock_client = mock.Mock()
        mock_client.list_tasks.return_value = []
        mock_replication_checker = mock.Mock()
        mock_pods = [mock.Mock(), mock.Mock()]
        mock_check_service_replication.return_value = True
        pods_by_service_instance = {
            instance_config.service: {
                instance_config.instance: mock_pods,
            }
        }

        (
            count_under_replicated,
            total,
        ) = check_services_replication_tools.check_services_replication(
            soa_dir=soa_dir,
            cluster="westeros-prod",
            service_instances=[],
            instance_type_class=None,
            check_service_replication=mock_check_service_replication,
            replication_checker=mock_replication_checker,
            pods_by_service_instance=pods_by_service_instance,
            dry_run=True,
        )
        mock_paasta_service_config_loader.assert_called_once_with(
            service="a", soa_dir=soa_dir
        )
        instance_config.get_docker_image.assert_called_once_with()
        mock_check_service_replication.assert_called_once_with(
            instance_config=instance_config,
            pods_by_service_instance=pods_by_service_instance,
            replication_checker=mock_replication_checker,
            dry_run=True,
        )
        assert count_under_replicated == 0
        assert total == 1
