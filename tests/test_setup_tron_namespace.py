from typing import List
from unittest import mock

from paasta_tools import setup_tron_namespace
from paasta_tools import spark_tools
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.tron_tools import KUBERNETES_NAMESPACE
from paasta_tools.tron_tools import TronActionConfig
from paasta_tools.tron_tools import TronJobConfig


def _make_action(
    iam_role: str = "",
    executor: str = "paasta",
    spark_executor_iam_role: str = "",
) -> mock.Mock:
    action = mock.Mock(spec_set=TronActionConfig)
    action.get_iam_role.return_value = iam_role
    action.get_executor.return_value = executor
    action.get_spark_executor_iam_role.return_value = spark_executor_iam_role
    return action


def _make_job(actions: List[TronActionConfig]) -> mock.Mock:
    job = mock.Mock(spec_set=TronJobConfig)
    job.get_actions.return_value = actions
    return job


def test_ensure_service_accounts():
    regular_kube_client = mock.Mock(spec=KubeClient)
    spark_kube_client = mock.Mock(spec=KubeClient)

    with mock.patch.object(
        setup_tron_namespace, "ensure_service_account", autospec=True
    ) as mock_ensure_service_account, mock.patch.object(
        setup_tron_namespace, "KubeClient", autospec=True
    ) as mock_kube_client_class, mock.patch.object(
        setup_tron_namespace, "load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_kube_client_class.side_effect = [regular_kube_client, spark_kube_client]

        job_configs = [
            _make_job(
                [
                    _make_action(iam_role="role-a"),
                    _make_action(
                        iam_role="role-b",
                        executor="spark",
                        spark_executor_iam_role="spark-role-b",
                    ),
                    # actions without an iam role should be skipped
                    _make_action(),
                ]
            ),
            # duplicate iam roles across jobs should only be ensured once
            _make_job([_make_action(iam_role="role-a")]),
        ]

        setup_tron_namespace.ensure_service_accounts(job_configs)

        # the spark executor SA is ensured with a client pointing at the spark cluster
        assert mock_kube_client_class.call_args_list == [
            mock.call(),
            mock.call(
                config_file=mock_load_system_paasta_config.return_value.get_spark_kubeconfig.return_value
            ),
        ]

        # sets are iterated in an arbitrary order, so we can't assert on call order
        assert mock_ensure_service_account.call_count == 3
        mock_ensure_service_account.assert_has_calls(
            [
                mock.call(
                    "role-a",
                    namespace=KUBERNETES_NAMESPACE,
                    kube_client=regular_kube_client,
                ),
                mock.call(
                    "role-b",
                    namespace=KUBERNETES_NAMESPACE,
                    kube_client=regular_kube_client,
                ),
                mock.call(
                    "spark-role-b",
                    namespace=spark_tools.SPARK_EXECUTOR_NAMESPACE,
                    kube_client=spark_kube_client,
                ),
            ],
            any_order=True,
        )
