# Copyright 2015-2018 Yelp Inc.
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
import sys
from unittest import mock

import pytest
from kubernetes.client.exceptions import ApiException

from paasta_tools.setup_smartstack_namespace_k8s_svcs import cleanup_namespace_services
from paasta_tools.setup_smartstack_namespace_k8s_svcs import delete_namespace_service
from paasta_tools.setup_smartstack_namespace_k8s_svcs import filter_grouped_namespaces
from paasta_tools.setup_smartstack_namespace_k8s_svcs import (
    get_existing_kubernetes_service_names,
)
from paasta_tools.setup_smartstack_namespace_k8s_svcs import (
    get_services_from_namespaces,
)
from paasta_tools.setup_smartstack_namespace_k8s_svcs import group_namespaces_by_service
from paasta_tools.setup_smartstack_namespace_k8s_svcs import is_external_routing_enabled
from paasta_tools.setup_smartstack_namespace_k8s_svcs import load_smartstack_namespaces
from paasta_tools.setup_smartstack_namespace_k8s_svcs import main
from paasta_tools.setup_smartstack_namespace_k8s_svcs import parse_args
from paasta_tools.setup_smartstack_namespace_k8s_svcs import (
    sanitise_kubernetes_service_name,
)
from paasta_tools.setup_smartstack_namespace_k8s_svcs import server_side_apply_service
from paasta_tools.setup_smartstack_namespace_k8s_svcs import setup_namespace_services
from paasta_tools.setup_smartstack_namespace_k8s_svcs import (
    setup_namespace_services_for_kube_namespace,
)


@pytest.fixture
def mock_kube_client():
    """Standard mock KubeClient for tests."""
    client = mock.Mock()
    client.core.api_client.sanitize_for_serialization.return_value = {
        "metadata": {"name": "svc"}
    }
    return client


@pytest.fixture
def mock_service():
    """Standard mock V1Service object."""
    service = mock.Mock()
    service.metadata.name = "test-service"
    service.metadata.namespace = "paastasvc-foo"
    service.spec.selector = {"paasta.yelp.com/registration": "true"}
    return service


@pytest.fixture
def sample_smartstack_namespaces():
    """Sample smartstack namespace configurations."""
    return {
        "foo.main": {"routing": {"external": True}, "proxy_port": 20001},
        "foo.canary": {"routing": {"external": True}, "proxy_port": 20002},
        "bar.main": {"routing": {"external": True}},
        "baz.internal": {"routing": {"external": False}},
        "qux.unrouted": {},
    }


class TestIsExternalRoutingEnabled:
    def test_returns_true_when_external_routing_enabled(self):
        config = {"routing": {"external": True}}
        assert is_external_routing_enabled(config) is True

    def test_returns_false_when_external_routing_disabled(self):
        config = {"routing": {"external": False}}
        assert is_external_routing_enabled(config) is False

    def test_returns_false_when_routing_missing(self):
        config = {}
        assert is_external_routing_enabled(config) is False

    def test_returns_false_when_external_missing(self):
        config = {"routing": {}}
        assert is_external_routing_enabled(config) is False

    def test_returns_false_when_routing_not_a_mapping(self):
        config = {"routing": "invalid"}
        assert is_external_routing_enabled(config) is False


class TestGroupNamespacesByService:
    def test_groups_namespaces_by_service_name(self, sample_smartstack_namespaces):
        result = group_namespaces_by_service(sample_smartstack_namespaces)
        assert result == {
            "foo": {"foo.main", "foo.canary"},
            "bar": {"bar.main"},
        }

    def test_filters_out_non_external_routing(self):
        namespaces = {
            "service.public": {"routing": {"external": True}},
            "service.private": {"routing": {"external": False}},
        }
        result = group_namespaces_by_service(namespaces)
        assert result == {"service": {"service.public"}}

    def test_skips_invalid_namespace_format(self):
        namespaces = {
            "valid.main": {"routing": {"external": True}},
            "invalid_no_dot": {"routing": {"external": True}},
        }
        result = group_namespaces_by_service(namespaces)
        assert result == {"valid": {"valid.main"}}

    def test_returns_empty_dict_for_empty_input(self):
        result = group_namespaces_by_service({})
        assert result == {}


class TestFilterGroupedNamespaces:
    def test_returns_all_when_no_filter_provided(self):
        grouped = {
            "foo": {"foo.main", "foo.canary"},
            "bar": {"bar.main"},
        }
        result = filter_grouped_namespaces(grouped, None)
        assert result == grouped

    def test_filters_to_specific_namespaces(self):
        grouped = {
            "foo": {"foo.main", "foo.canary"},
            "bar": {"bar.main"},
        }
        result = filter_grouped_namespaces(grouped, ["foo.main"])
        assert result == {"foo": {"foo.main"}}

    def test_filters_multiple_namespaces(self):
        grouped = {
            "foo": {"foo.main", "foo.canary"},
            "bar": {"bar.main"},
        }
        result = filter_grouped_namespaces(grouped, ["foo.main", "bar.main"])
        assert result == {
            "foo": {"foo.main"},
            "bar": {"bar.main"},
        }

    def test_skips_invalid_filter_format(self):
        grouped = {"foo": {"foo.main"}}
        result = filter_grouped_namespaces(grouped, ["invalid_no_dot"])
        assert result == {}

    def test_warns_when_service_not_found(self):
        grouped = {"foo": {"foo.main"}}
        result = filter_grouped_namespaces(grouped, ["nonexistent.main"])
        assert result == {}

    def test_warns_when_namespace_not_routable(self):
        grouped = {"foo": {"foo.main"}}
        result = filter_grouped_namespaces(grouped, ["foo.canary"])
        assert result == {}


class TestGetServicesFromNamespaces:
    def test_extracts_unique_services(self):
        namespaces = ["foo.main", "foo.canary", "bar.main"]
        result = get_services_from_namespaces(namespaces)
        assert result == {"foo", "bar"}

    def test_returns_none_when_no_filter(self):
        result = get_services_from_namespaces(None)
        assert result is None

    def test_skips_invalid_formats(self):
        namespaces = ["foo.main", "invalid", "bar.main"]
        result = get_services_from_namespaces(namespaces)
        assert result == {"foo", "bar"}


class TestLoadSmartstackNamespaces:
    def test_loads_valid_smartstack_yaml(self, tmp_path):
        service_dir = tmp_path / "testservice"
        service_dir.mkdir()
        smartstack_yaml = service_dir / "smartstack.yaml"
        smartstack_yaml.write_text(
            """
main:
  routing:
    external: true
canary:
  routing:
    external: false
"""
        )

        result = load_smartstack_namespaces(str(tmp_path))
        assert "testservice.main" in result
        assert "testservice.canary" in result
        assert result["testservice.main"]["routing"]["external"] is True

    def test_loads_only_specified_services(self, tmp_path):
        # Create multiple service directories
        for service in ["foo", "bar", "baz"]:
            service_dir = tmp_path / service
            service_dir.mkdir()
            smartstack_yaml = service_dir / "smartstack.yaml"
            smartstack_yaml.write_text("main:\n  routing:\n    external: true\n")

        # Only load foo and bar
        result = load_smartstack_namespaces(str(tmp_path), services={"foo", "bar"})
        assert "foo.main" in result
        assert "bar.main" in result
        assert "baz.main" not in result

    def test_skips_service_without_smartstack_yaml(self, tmp_path):
        service_dir = tmp_path / "no_smartstack"
        service_dir.mkdir()

        result = load_smartstack_namespaces(str(tmp_path))
        assert "no_smartstack.main" not in result

    def test_handles_empty_namespace_details(self, tmp_path):
        service_dir = tmp_path / "testservice"
        service_dir.mkdir()
        smartstack_yaml = service_dir / "smartstack.yaml"
        smartstack_yaml.write_text("main:\n")

        result = load_smartstack_namespaces(str(tmp_path))
        assert "testservice.main" not in result

    def test_handles_non_mapping_namespace(self, tmp_path):
        service_dir = tmp_path / "testservice"
        service_dir.mkdir()
        smartstack_yaml = service_dir / "smartstack.yaml"
        smartstack_yaml.write_text("main: invalid_string\n")

        result = load_smartstack_namespaces(str(tmp_path))
        assert "testservice.main" not in result

    def test_handles_non_mapping_root(self, tmp_path):
        service_dir = tmp_path / "testservice"
        service_dir.mkdir()
        smartstack_yaml = service_dir / "smartstack.yaml"
        smartstack_yaml.write_text("- not\n- a\n- mapping\n")

        result = load_smartstack_namespaces(str(tmp_path))
        assert len(result) == 0

    def test_handles_malformed_yaml(self, tmp_path):
        service_dir = tmp_path / "testservice"
        service_dir.mkdir()
        smartstack_yaml = service_dir / "smartstack.yaml"
        smartstack_yaml.write_text("invalid: yaml: content:\n")

        result = load_smartstack_namespaces(str(tmp_path))
        # Should not crash, just log warning
        assert isinstance(result, dict)


class TestSanitiseKubernetesServiceName:
    def test_replaces_dots_with_triple_dash(self):
        assert sanitise_kubernetes_service_name("service.main") == "service---main"

    def test_handles_long_names(self):
        long_name = "very.long.service.name.that.exceeds.kubernetes.limits" * 5
        result = sanitise_kubernetes_service_name(long_name)
        # Should not crash and should be valid k8s name
        assert len(result) <= 63
        assert result.replace("-", "").isalnum()


class TestParseArgs:
    def test_parses_flags(self, monkeypatch):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "setup_smartstack_namespace_k8s_svcs.py",
                "--dry-run",
                "-v",
                "-d",
                "/nail/etc/services-custom",
                "-n",
                "foo.main",
                "-n",
                "bar.canary",
            ],
        )

        args = parse_args()

        assert args.dry_run is True
        assert args.verbose is True
        assert args.soa_dir == "/nail/etc/services-custom"
        assert args.namespaces == ["foo.main", "bar.canary"]


class TestGetExistingKubernetesServiceNames:
    def test_returns_empty_when_namespace_missing(self, mock_kube_client):
        mock_kube_client.core.list_namespaced_service.side_effect = ApiException(
            status=404
        )

        result = get_existing_kubernetes_service_names(
            mock_kube_client, "paastasvc-missing"
        )

        assert result == set()
        mock_kube_client.core.list_namespaced_service.assert_called_once()

    def test_reraises_other_api_errors(self, mock_kube_client):
        mock_kube_client.core.list_namespaced_service.side_effect = ApiException(
            status=500
        )

        with pytest.raises(ApiException):
            get_existing_kubernetes_service_names(mock_kube_client, "paastasvc-busted")

    def test_returns_discovered_services(self, mock_kube_client):
        mock_service = mock.Mock()
        mock_service.metadata = mock.Mock(name="svc-one")
        mock_service.metadata.name = "svc-one"
        mock_other = mock.Mock()
        mock_other.metadata = mock.Mock(name="svc-two")
        mock_other.metadata.name = "svc-two"
        mock_kube_client.core.list_namespaced_service.return_value = mock.Mock(
            items=[mock_service, mock_other]
        )

        result = get_existing_kubernetes_service_names(
            mock_kube_client, "paastasvc-foo"
        )

        assert result == {"svc-one", "svc-two"}


class TestSetupNamespaceServicesForKubeNamespace:
    def test_reconciles_each_namespace(self, mock_kube_client):
        smartstack_namespaces = ["service.main", "service.canary"]
        results = list(
            setup_namespace_services_for_kube_namespace(
                mock_kube_client,
                "paastasvc-service",
                smartstack_namespaces,
            )
        )

        assert len(results) == 2
        assert all(success for success, _ in results)

    def test_reconciles_successfully(self, mock_kube_client):
        service_name = "compute-infra-test-service.main"
        results = list(
            setup_namespace_services_for_kube_namespace(
                kube_client=mock_kube_client,
                kube_namespace="paastasvc-compute-infra-test-service",
                smartstack_namespaces=[service_name],
            )
        )

        assert len(results) == 1
        success, message = results[0]
        assert success is True
        assert "Reconciled Service" in message

    def test_handles_errors(self, mock_kube_client):
        mock_kube_client.core.patch_namespaced_service.side_effect = Exception(
            "API error"
        )

        results = list(
            setup_namespace_services_for_kube_namespace(
                mock_kube_client,
                "paastasvc-service",
                ["service.main"],
            )
        )

        assert len(results) == 1
        success, message = results[0]
        assert success is False
        assert "Failed to reconcile" in message
        assert "API error" in message


class TestCleanupNamespaceServices:
    def test_removes_only_undeclared_services(self, mock_kube_client):
        smartstack_namespaces = ["svc1", "svc2"]
        existing_services = {
            sanitise_kubernetes_service_name("svc1"),
            "stale-service",
        }

        results = list(
            cleanup_namespace_services(
                mock_kube_client,
                "paastasvc-test",
                smartstack_namespaces,
                existing_services,
            )
        )

        assert len(results) == 1
        success, message = results[0]
        assert success is True
        assert "Deleted Service" in message
        assert "stale-service" in message

    def test_removes_nothing_when_all_services_declared(self, mock_kube_client):
        smartstack_namespaces = ["svc1", "svc2"]
        existing_services = {
            sanitise_kubernetes_service_name("svc1"),
            sanitise_kubernetes_service_name("svc2"),
        }

        results = list(
            cleanup_namespace_services(
                mock_kube_client,
                "paastasvc-test",
                smartstack_namespaces,
                existing_services,
            )
        )

        assert len(results) == 0

    def test_handles_deletion_errors(self, mock_kube_client):
        mock_kube_client.core.delete_namespaced_service.side_effect = Exception(
            "Delete failed"
        )
        smartstack_namespaces = []
        existing_services = {"stale-service"}

        results = list(
            cleanup_namespace_services(
                mock_kube_client,
                "paastasvc-test",
                smartstack_namespaces,
                existing_services,
            )
        )

        assert len(results) == 1
        success, message = results[0]
        assert success is False
        assert "Failed to delete" in message
        assert "Delete failed" in message


class TestServerSideApplyService:
    def test_patches_existing_service(self, mock_kube_client, mock_service):
        server_side_apply_service(
            mock_kube_client,
            "paastasvc-foo",
            mock_service,
        )

        mock_kube_client.core.patch_namespaced_service.assert_called_once()
        call_kwargs = mock_kube_client.core.patch_namespaced_service.call_args[1]
        assert call_kwargs["name"] == "test-service"
        assert call_kwargs["namespace"] == "paastasvc-foo"
        assert call_kwargs["field_manager"] == "paasta-namespace-services"
        assert call_kwargs["content_type"] == "application/apply-patch+yaml"
        assert "dry_run" not in call_kwargs

    def test_creates_then_patches_on_404(self, mock_kube_client, mock_service):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
            None,
        ]

        server_side_apply_service(mock_kube_client, "paastasvc-foo", mock_service)

        mock_kube_client.core.create_namespaced_service.assert_called_once_with(
            namespace="paastasvc-foo",
            body=mock_service,
        )
        assert mock_kube_client.core.patch_namespaced_service.call_count == 2

    def test_rollback_on_patch_failure_after_create(
        self, mock_kube_client, mock_service
    ):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
            Exception("Patch failed"),
        ]

        with pytest.raises(Exception, match="Patch failed"):
            server_side_apply_service(mock_kube_client, "paastasvc-foo", mock_service)

        mock_kube_client.core.delete_namespaced_service.assert_called_once_with(
            name="test-service",
            namespace="paastasvc-foo",
        )

    def test_no_rollback_on_create_failure(self, mock_kube_client, mock_service):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
        ]
        mock_kube_client.core.create_namespaced_service.side_effect = Exception(
            "Create failed"
        )

        with pytest.raises(Exception, match="Create failed"):
            server_side_apply_service(mock_kube_client, "paastasvc-foo", mock_service)

        mock_kube_client.core.delete_namespaced_service.assert_not_called()

    def test_no_rollback_in_dry_run_mode(self, mock_kube_client, mock_service):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
            Exception("Patch failed"),
        ]

        server_side_apply_service(
            mock_kube_client, "paastasvc-foo", mock_service, dry_run=True
        )

        mock_kube_client.core.delete_namespaced_service.assert_not_called()

    def test_dry_run_sets_flag_on_all_operations(self, mock_kube_client, mock_service):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
            None,
        ]

        server_side_apply_service(
            mock_kube_client, "paastasvc-foo", mock_service, dry_run=True
        )

        create_kwargs = mock_kube_client.core.create_namespaced_service.call_args[1]
        assert create_kwargs["dry_run"] == "All"

        patch_calls = mock_kube_client.core.patch_namespaced_service.call_args_list
        assert all(call[1]["dry_run"] == "All" for call in patch_calls)

    def test_dry_run_creation_skips_followup_patch(
        self, mock_kube_client, mock_service
    ):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404)
        ]

        server_side_apply_service(
            mock_kube_client, "paastasvc-foo", mock_service, dry_run=True
        )

        assert mock_kube_client.core.patch_namespaced_service.call_count == 1
        mock_kube_client.core.create_namespaced_service.assert_called_once()

    @mock.patch("paasta_tools.setup_smartstack_namespace_k8s_svcs.log", autospec=True)
    def test_logs_when_rollback_delete_fails(
        self, mock_log, mock_kube_client, mock_service
    ):
        mock_kube_client.core.patch_namespaced_service.side_effect = [
            ApiException(status=404),
            Exception("Patch failed"),
        ]
        mock_kube_client.core.delete_namespaced_service.side_effect = Exception(
            "Delete failed"
        )

        with pytest.raises(Exception, match="Patch failed"):
            server_side_apply_service(mock_kube_client, "paastasvc-foo", mock_service)

        mock_kube_client.core.delete_namespaced_service.assert_called_once()
        mock_log.exception.assert_called_once()

    def test_raises_on_non_404_api_exception(self, mock_kube_client, mock_service):
        mock_kube_client.core.patch_namespaced_service.side_effect = ApiException(
            status=403
        )

        with pytest.raises(ApiException) as exc_info:
            server_side_apply_service(mock_kube_client, "paastasvc-foo", mock_service)

        assert exc_info.value.status == 403
        mock_kube_client.core.create_namespaced_service.assert_not_called()


class TestDeleteNamespaceService:
    def test_deletes_service(self, mock_kube_client):
        delete_namespace_service(
            mock_kube_client,
            "paastasvc-foo",
            "service-to-delete",
        )

        mock_kube_client.core.delete_namespaced_service.assert_called_once_with(
            name="service-to-delete",
            namespace="paastasvc-foo",
        )

    def test_dry_run_sets_flag(self, mock_kube_client):
        delete_namespace_service(
            mock_kube_client,
            "paastasvc-foo",
            "service-to-delete",
            dry_run=True,
        )

        call_kwargs = mock_kube_client.core.delete_namespaced_service.call_args[1]
        assert call_kwargs["dry_run"] == "All"


class TestSetupNamespaceServices:
    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_processes_all_services(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
        sample_smartstack_namespaces,
    ):
        mock_load_smartstack.return_value = sample_smartstack_namespaces
        mock_get_existing.side_effect = [set(), set()]
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(mock_kube_client)

        assert result is True
        mock_ensure_namespace.assert_has_calls(
            [
                mock.call(mock_kube_client, namespace="paastasvc-foo"),
                mock.call(mock_kube_client, namespace="paastasvc-bar"),
            ]
        )

        # Verify correct namespaces were processed
        namespaces_seen = {
            _call_value(call, 1, "kube_namespace")
            for call in mock_server_side_apply.call_args_list
        }
        assert namespaces_seen == {"paastasvc-foo", "paastasvc-bar"}

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_filters_to_requested_namespaces(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
        sample_smartstack_namespaces,
    ):
        mock_load_smartstack.return_value = sample_smartstack_namespaces
        mock_get_existing.return_value = set()
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(
            mock_kube_client,
            target_namespaces=["foo.main"],
        )

        assert result is True
        mock_ensure_namespace.assert_called_once_with(
            mock_kube_client, namespace="paastasvc-foo"
        )

        # Should only process foo service
        namespaces_seen = {
            _call_value(call, 1, "kube_namespace")
            for call in mock_server_side_apply.call_args_list
        }
        assert namespaces_seen == {"paastasvc-foo"}

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_dry_run_skips_namespace_creation(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
        sample_smartstack_namespaces,
    ):
        mock_load_smartstack.return_value = sample_smartstack_namespaces
        mock_get_existing.return_value = set()
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(mock_kube_client, dry_run=True)

        assert result is True
        mock_ensure_namespace.assert_not_called()

        # Verify dry_run propagated to operations
        assert all(
            _call_value(call, 3, "dry_run") is True
            for call in mock_server_side_apply.call_args_list
        )
        assert all(
            _call_value(call, 3, "dry_run") is True
            for call in mock_delete_service.call_args_list
        )

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_cleans_up_stale_services(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
    ):
        mock_load_smartstack.return_value = {
            "foo.main": {"routing": {"external": True}},
        }
        mock_get_existing.return_value = {"foo---main", "stale-service"}
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(mock_kube_client)

        assert result is True
        assert mock_delete_service.called
        assert (
            _call_value(mock_delete_service.call_args, 2, "service_name")
            == "stale-service"
        )

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_continues_on_error(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
    ):
        mock_load_smartstack.return_value = {
            "foo.main": {"routing": {"external": True}},
            "bar.main": {"routing": {"external": True}},
        }
        mock_get_existing.side_effect = [set(), set()]
        mock_kube_client = mock.Mock()

        # First service fails, second succeeds
        mock_server_side_apply.side_effect = [
            Exception("Failed"),
            None,
        ]

        result = setup_namespace_services(mock_kube_client)

        # Should return False because of error
        assert result is False
        # But should still call for both services
        assert mock_server_side_apply.call_count == 2

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.server_side_apply_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.delete_namespace_service",
        autospec=True,
    )
    def test_handles_no_matching_namespaces(
        self,
        mock_delete_service,
        mock_server_side_apply,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_sleep,
    ):
        mock_load_smartstack.return_value = {
            "foo.main": {"routing": {"external": False}},
        }
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(mock_kube_client)

        assert result is True
        mock_ensure_namespace.assert_not_called()
        assert not mock_server_side_apply.called

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.cleanup_namespace_services",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.setup_namespace_services_for_kube_namespace",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.load_smartstack_namespaces",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.get_existing_kubernetes_service_names",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.ensure_namespace",
        autospec=True,
    )
    def test_cleanup_errors_mark_failure(
        self,
        mock_ensure_namespace,
        mock_get_existing,
        mock_load_smartstack,
        mock_setup_for_ns,
        mock_cleanup,
        mock_sleep,
    ):
        mock_load_smartstack.return_value = {
            "foo.main": {"routing": {"external": True}},
        }
        mock_get_existing.return_value = set()
        mock_setup_for_ns.return_value = iter([(True, "ok")])
        mock_cleanup.mock.return_value = iter([(False, "boom")])
        mock_kube_client = mock.Mock()

        result = setup_namespace_services(mock_kube_client)

        assert result is False
        mock_cleanup.mock.assert_called_once()


def _call_value(call, position, name):
    """Helper to extract argument from mock call by position or name."""
    args, kwargs = call
    if name in kwargs:
        return kwargs[name]
    if len(args) > position:
        return args[position]
    return None


class TestMain:
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.setup_namespace_services",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.KubeClient", autospec=True
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.logging", autospec=True
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.parse_args", autospec=True
    )
    def test_main_success_exit_zero(
        self,
        mock_parse_args,
        mock_logging,
        mock_kube_client_cls,
        mock_setup_namespace_services,
    ):
        args = mock.Mock(
            verbose=False,
            namespaces=["foo.main"],
            soa_dir="/nail/etc/services",
            dry_run=True,
        )
        mock_parse_args.return_value = args
        mock_setup_namespace_services.return_value = True

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 0
        mock_logging.basicConfig.assert_called_once_with(level=mock_logging.INFO)
        mock_kube_client_cls.assert_called_once()
        mock_setup_namespace_services.assert_called_once_with(
            mock_kube_client_cls.return_value,
            args.namespaces,
            args.soa_dir,
            args.dry_run,
        )

    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.setup_namespace_services",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.KubeClient", autospec=True
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.logging", autospec=True
    )
    @mock.patch(
        "paasta_tools.setup_smartstack_namespace_k8s_svcs.parse_args", autospec=True
    )
    def test_main_failure_exit_one(
        self,
        mock_parse_args,
        mock_logging,
        mock_kube_client_cls,
        mock_setup_namespace_services,
    ):
        args = mock.Mock(
            verbose=True, namespaces=None, soa_dir="/nail/etc/services", dry_run=False
        )
        mock_parse_args.return_value = args
        mock_setup_namespace_services.return_value = False

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1
        mock_logging.basicConfig.assert_called_once_with(level=mock_logging.DEBUG)
