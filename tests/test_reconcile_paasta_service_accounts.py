#!/usr/bin/env python
# Copyright 2015-2024 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from unittest import mock

import pytest
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1ServiceAccount
from kubernetes.client.exceptions import ApiException

from paasta_tools.reconcile_paasta_service_accounts import MANAGED_LABEL
from paasta_tools.reconcile_paasta_service_accounts import ROLE_ARN_ANNOTATION
from paasta_tools.reconcile_paasta_service_accounts import DriftItem
from paasta_tools.reconcile_paasta_service_accounts import DriftKind
from paasta_tools.reconcile_paasta_service_accounts import apply_drift
from paasta_tools.reconcile_paasta_service_accounts import collect_desired_sas
from paasta_tools.reconcile_paasta_service_accounts import compute_drift
from paasta_tools.reconcile_paasta_service_accounts import get_oidc_provider_arn
from paasta_tools.reconcile_paasta_service_accounts import (
    parse_namespaces_from_trust_policy,
)


class TestGetOidcProviderArn:
    def test_eks_cluster(self):
        result = get_oidc_provider_arn(
            account_id="123456789012",
            cluster_server_url="https://ABCDEF1234.gr7.us-west-2.eks.amazonaws.com",
            aws_region="us-west-2",
            cluster_name="my-cluster",
            ecosystem="prod",
        )
        assert result == (
            "arn:aws:iam::123456789012:oidc-provider/"
            "oidc.eks.us-west-2.amazonaws.com/id/ABCDEF1234"
        )

    def test_non_eks_cluster(self):
        result = get_oidc_provider_arn(
            account_id="123456789012",
            cluster_server_url="https://k8s.example.com",
            aws_region="us-west-1",
            cluster_name="norcal-devc",
            ecosystem="devc",
        )
        assert result == (
            "arn:aws:iam::123456789012:oidc-provider/"
            "s3.us-west-1.amazonaws.com/aws-k8s-pod-identity-oidc-devc-us-west-1/norcal-devc"
        )


class TestParseNamespacesFromTrustPolicy:
    def test_extracts_namespaces_from_deny_statement(self):
        trust_policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Condition": {
                        "StringNotLike": {
                            "oidc.eks.us-west-2.amazonaws.com/id/ABC:sub": [
                                "system:serviceaccount:mwaa:*",
                                "system:serviceaccount:temporal:*",
                            ]
                        }
                    },
                }
            ]
        }
        result = parse_namespaces_from_trust_policy(
            trust_policy, {"mwaa", "temporal", "paasta"}
        )
        assert result == {"mwaa", "temporal"}

    def test_ignores_allow_statements(self):
        trust_policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Condition": {
                        "StringNotLike": {"oidc:sub": ["system:serviceaccount:mwaa:*"]}
                    },
                }
            ]
        }
        result = parse_namespaces_from_trust_policy(trust_policy, {"mwaa"})
        assert result == set()

    def test_ignores_namespaces_not_in_allowed(self):
        trust_policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Condition": {
                        "StringNotLike": {
                            "oidc:sub": ["system:serviceaccount:other-ns:*"]
                        }
                    },
                }
            ]
        }
        result = parse_namespaces_from_trust_policy(trust_policy, {"mwaa"})
        assert result == set()

    def test_handles_string_pattern_not_list(self):
        trust_policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Condition": {
                        "StringNotLike": {"oidc:sub": "system:serviceaccount:mwaa:*"}
                    },
                }
            ]
        }
        result = parse_namespaces_from_trust_policy(trust_policy, {"mwaa"})
        assert result == {"mwaa"}

    def test_wildcard_pattern_matches_all_allowed_namespaces(self):
        trust_policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Condition": {
                        "StringNotLike": {
                            "oidc.eks.us-west-2.amazonaws.com/id/ABC:sub": "*:*:*:*",
                        }
                    },
                }
            ]
        }
        result = parse_namespaces_from_trust_policy(trust_policy, {"mwaa", "temporal"})
        assert result == {"mwaa", "temporal"}

    def test_empty_trust_policy(self):
        result = parse_namespaces_from_trust_policy({}, {"mwaa"})
        assert result == set()


ROLE_ARN = "arn:aws:iam::123456789012:role/my-role"
OIDC_ARN = (
    "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/ABC"
)


def _make_trust_policy(oidc_arn, namespaces):
    return {
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Principal": {"Federated": oidc_arn},
            },
            {
                "Effect": "Deny",
                "Condition": {
                    "StringNotLike": {
                        f"{oidc_arn}:sub": [
                            f"system:serviceaccount:{ns}:*" for ns in namespaces
                        ]
                    }
                },
            },
        ],
    }


class TestCollectDesiredSas:
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.boto3.Session", autospec=True
    )
    def test_finds_matching_roles(self, mock_session_cls):
        mock_iam = mock.MagicMock()
        mock_session_cls.return_value.client.return_value = mock_iam

        mock_paginator = mock.MagicMock()
        mock_iam.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Roles": [
                    {
                        "Arn": ROLE_ARN,
                        "AssumeRolePolicyDocument": _make_trust_policy(
                            OIDC_ARN, ["mwaa"]
                        ),
                    },
                ]
            }
        ]

        result = collect_desired_sas(OIDC_ARN, {"mwaa", "temporal"})
        assert len(result) == 1
        assert result[0].namespace == "mwaa"
        assert result[0].role_arn == ROLE_ARN

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.boto3.Session", autospec=True
    )
    def test_skips_roles_not_referencing_our_cluster(self, mock_session_cls):
        mock_iam = mock.MagicMock()
        mock_session_cls.return_value.client.return_value = mock_iam

        mock_paginator = mock.MagicMock()
        mock_iam.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Roles": [
                    {
                        "Arn": ROLE_ARN,
                        "AssumeRolePolicyDocument": _make_trust_policy(
                            "arn:aws:iam::999999999999:oidc-provider/other",
                            ["mwaa"],
                        ),
                    },
                ]
            }
        ]

        result = collect_desired_sas(OIDC_ARN, {"mwaa"})
        assert len(result) == 0

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.boto3.Session", autospec=True
    )
    def test_skips_roles_with_no_matching_namespaces(self, mock_session_cls):
        mock_iam = mock.MagicMock()
        mock_session_cls.return_value.client.return_value = mock_iam

        mock_paginator = mock.MagicMock()
        mock_iam.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Roles": [
                    {
                        "Arn": ROLE_ARN,
                        "AssumeRolePolicyDocument": _make_trust_policy(
                            OIDC_ARN, ["paasta"]
                        ),
                    },
                ]
            }
        ]

        result = collect_desired_sas(OIDC_ARN, {"mwaa"})
        assert len(result) == 0


def _make_sa(name, namespace, role_arn=None, managed=False):
    labels = {MANAGED_LABEL: "true"} if managed else {}
    annotations = {ROLE_ARN_ANNOTATION: role_arn} if role_arn else {}
    return V1ServiceAccount(
        metadata=V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels,
            annotations=annotations,
        )
    )


def _make_namespace(name):
    ns = mock.MagicMock()
    ns.metadata.name = name
    return ns


class TestComputeDrift:
    def test_missing_sa(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = []

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        assert len(errors) == 0
        assert len(drift) == 1
        assert drift[0].kind == DriftKind.MISSING
        assert drift[0].sa_name == "paasta--my-role"

    def test_ok_sa(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = [
            _make_sa("paasta--my-role", "mwaa", role_arn=ROLE_ARN, managed=True),
        ]

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        assert len(errors) == 0
        ok_items = [d for d in drift if d.kind == DriftKind.OK]
        assert len(ok_items) == 1

    def test_wrong_arn(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = [
            _make_sa(
                "paasta--my-role",
                "mwaa",
                role_arn="arn:aws:iam::000:role/wrong",
                managed=True,
            ),
        ]

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        wrong = [d for d in drift if d.kind == DriftKind.WRONG_ARN]
        assert len(wrong) == 1
        assert wrong[0].actual_arn == "arn:aws:iam::000:role/wrong"
        assert wrong[0].desired_arn == ROLE_ARN

    def test_unmanaged_sa(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = [
            _make_sa("paasta--my-role", "mwaa", role_arn=ROLE_ARN, managed=False),
        ]

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        unmanaged = [d for d in drift if d.kind == DriftKind.UNMANAGED]
        assert len(unmanaged) == 1

    def test_extra_managed_sa(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = [
            _make_sa("paasta--old-role", "mwaa", role_arn="arn:old", managed=True),
        ]

        drift, errors = compute_drift(mock_kube, [], {"mwaa"})

        extra = [d for d in drift if d.kind == DriftKind.EXTRA]
        assert len(extra) == 1
        assert extra[0].sa_name == "paasta--old-role"

    def test_namespace_does_not_exist(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = []

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        assert len(drift) == 1
        assert drift[0].kind == DriftKind.MISSING

    def test_list_namespace_failure(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.side_effect = Exception("connection refused")

        drift, errors = compute_drift(mock_kube, [], {"mwaa"})

        assert len(errors) == 1
        assert "Failed to list namespaces" in errors[0]

    def test_list_sa_failure(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.side_effect = ApiException(
            status=403, reason="Forbidden"
        )

        from paasta_tools.reconcile_paasta_service_accounts import DesiredSA

        desired = [
            DesiredSA(namespace="mwaa", sa_name="paasta--my-role", role_arn=ROLE_ARN)
        ]
        drift, errors = compute_drift(mock_kube, desired, {"mwaa"})

        assert len(errors) == 1
        assert "Failed to list SAs in mwaa" in errors[0]


class TestApplyDrift:
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.ensure_service_account",
        autospec=True,
    )
    def test_create_missing(self, mock_ensure):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--my-role",
                kind=DriftKind.MISSING,
                desired_arn=ROLE_ARN,
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=False)

        assert stats.created == 1
        assert stats.errors == 0
        mock_ensure.assert_called_once_with(
            iam_role=ROLE_ARN,
            namespace="mwaa",
            kube_client=mock_kube,
            managed=True,
        )

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.ensure_service_account",
        autospec=True,
    )
    def test_create_dry_run(self, mock_ensure):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--my-role",
                kind=DriftKind.MISSING,
                desired_arn=ROLE_ARN,
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=True)

        assert stats.created == 1
        mock_ensure.assert_not_called()

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.ensure_service_account",
        autospec=True,
    )
    def test_create_failure(self, mock_ensure):
        mock_ensure.side_effect = ApiException(
            status=500, reason="Internal Server Error"
        )
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--my-role",
                kind=DriftKind.MISSING,
                desired_arn=ROLE_ARN,
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=False)

        assert stats.created == 0
        assert stats.errors == 1

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.ensure_service_account",
        autospec=True,
    )
    def test_update_wrong_arn(self, mock_ensure):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--my-role",
                kind=DriftKind.WRONG_ARN,
                desired_arn=ROLE_ARN,
                actual_arn="arn:old",
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=False)

        assert stats.updated == 1
        assert stats.errors == 0
        mock_ensure.assert_called_once_with(
            iam_role=ROLE_ARN,
            namespace="mwaa",
            kube_client=mock_kube,
            managed=True,
        )

    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.ensure_service_account",
        autospec=True,
    )
    def test_adopt_unmanaged(self, mock_ensure):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--my-role",
                kind=DriftKind.UNMANAGED,
                desired_arn=ROLE_ARN,
                actual_arn=ROLE_ARN,
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=False)

        assert stats.updated == 1
        mock_ensure.assert_called_once_with(
            iam_role=ROLE_ARN,
            namespace="mwaa",
            kube_client=mock_kube,
            managed=True,
        )

    def test_delete_extra(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--old-role",
                kind=DriftKind.EXTRA,
                actual_arn="arn:old",
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=False)

        assert stats.deleted == 1
        assert stats.errors == 0
        mock_kube.core.delete_namespaced_service_account.assert_called_once()

    def test_delete_dry_run(self):
        mock_kube = mock.MagicMock(spec_set=["core"])
        drift = [
            DriftItem(
                namespace="mwaa",
                sa_name="paasta--old-role",
                kind=DriftKind.EXTRA,
                actual_arn="arn:old",
            )
        ]

        stats = apply_drift(mock_kube, drift, dry_run=True)

        assert stats.deleted == 1
        mock_kube.core.delete_namespaced_service_account.assert_not_called()


class TestMain:
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.KubeClient", autospec=True
    )
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.collect_desired_sas",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.boto3.Session", autospec=True
    )
    @mock.patch(
        "paasta_tools.reconcile_paasta_service_accounts.load_system_paasta_config",
        autospec=True,
    )
    def test_dry_run_skips_apply(
        self,
        mock_config,
        mock_session,
        mock_collect,
        mock_kube_cls,
    ):
        mock_config.return_value.get_cluster.return_value = "test-cluster"
        mock_config.return_value.get_reconciled_iam_service_account_namespaces.return_value = [
            "mwaa"
        ]
        mock_config.return_value.get_kube_clusters.return_value = {
            "test-cluster": {
                "aws_region": "us-west-2",
                "ecosystem": "prod",
                "server": "https://ABC.gr7.us-west-2.eks.amazonaws.com",
            }
        }
        mock_sts = mock.MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.return_value.client.return_value = mock_sts

        mock_collect.return_value = []

        mock_kube = mock.MagicMock(spec_set=["core"])
        mock_kube.core.list_namespace.return_value.items = [_make_namespace("mwaa")]
        mock_kube.core.list_namespaced_service_account.return_value.items = []
        mock_kube_cls.return_value = mock_kube

        from paasta_tools.reconcile_paasta_service_accounts import main

        with mock.patch(
            "sys.argv", ["reconcile_paasta_service_accounts.py", "--dry-run"]
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 0
        mock_kube.core.create_namespaced_service_account.assert_not_called()
        mock_kube.core.patch_namespaced_service_account.assert_not_called()
        mock_kube.core.delete_namespaced_service_account.assert_not_called()
