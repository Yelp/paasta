#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import mock
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from pytest import raises

from paasta_tools.cleanup_kubernetes_jobs import cleanup_unused_apps
from paasta_tools.cleanup_kubernetes_jobs import DontKillEverythingError
from paasta_tools.cleanup_kubernetes_jobs import main
from paasta_tools.kubernetes_test_lib.utils import MockKubeClient


fake_deployment = V1Deployment(
    metadata=mock.Mock(
        namespace='paasta',
        labels={
            'yelp.com/paasta_service': 'service',
            'yelp.com/paasta_instance': 'instance-1',
            'yelp.com/paasta_git_sha': '1234',
            'yelp.com/paasta_config_sha': '1234',
        },
    ),
    spec=mock.Mock(replicas=0),
)
type(fake_deployment.metadata).name = mock.PropertyMock(return_value='service-instance-1')

fake_stateful_set = V1StatefulSet(
    metadata=mock.Mock(
        namespace='paasta',
        labels={
            'yelp.com/paasta_service': 'service',
            'yelp.com/paasta_instance': 'instance-2',
            'yelp.com/paasta_git_sha': '1234',
            'yelp.com/paasta_config_sha': '1234',
        },
    ),
    spec=mock.Mock(replicas=0),
)
type(fake_stateful_set.metadata).name = mock.PropertyMock(return_value='service-instance-2'),

invalid_app = V1Deployment(
    metadata=mock.Mock(
        namespace='paasta',
        labels={},
    ),
    spec=mock.Mock(replicas=0),
)
type(invalid_app.metadata).name = mock.PropertyMock(return_value='invalid_app'),


def test_main():
    soa_dir = 'paasta_maaaachine'
    with mock.patch('paasta_tools.cleanup_kubernetes_jobs.cleanup_unused_apps', autospec=True) as cleanup_patch:
        main(('--soa-dir', soa_dir))
        cleanup_patch.assert_called_once_with(
            soa_dir, kill_threshold=0.5, force=False,
        )


def test_cleanup_unused_apps():
    mock_kube_client = MockKubeClient()
    namespace = 'paasta'
    mock_kube_client.deployments.create_namespaced_deployment(namespace, fake_deployment)
    mock_kube_client.deployments.create_namespaced_stateful_set(namespace, fake_stateful_set)
    with mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.KubeClient',
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster',
        return_value={},
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.notify',
        autospec=True,
    ) as mock_notify:
        mock_notify.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_notify.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps('soa_dir', kill_threshold=1, force=False)
        assert len(mock_kube_client.deployments.list_namespaced_deployment(namespace).items) == 0
        assert len(mock_kube_client.deployments.list_namespaced_stateful_set(namespace).items) == 0


def test_cleanup_unused_apps_does_not_delete():
    mock_kube_client = MockKubeClient()
    namespace = 'paasta'
    mock_kube_client.deployments.create_namespaced_deployment(namespace, fake_deployment)
    mock_kube_client.deployments.create_namespaced_stateful_set(namespace, fake_stateful_set)
    with mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.KubeClient',
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster',
        return_value={('service', 'instance-1'), ('service', 'instance-2')},
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.notify',
        autospec=True,
    ) as mock_notify:
        mock_notify.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_notify.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps('soa_dir', kill_threshold=1, force=False)
        assert fake_deployment in mock_kube_client.deployments.list_namespaced_deployment(namespace).items
        assert fake_stateful_set in mock_kube_client.deployments.list_namespaced_stateful_set(namespace).items


def test_cleanup_unused_apps_dont_kill_everything():
    mock_kube_client = MockKubeClient()
    namespace = 'paasta'
    mock_kube_client.deployments.create_namespaced_deployment(namespace, fake_deployment)
    with mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.KubeClient',
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster',
        return_value={},
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.notify',
        autospec=True,
    ) as mock_notify:
        mock_notify.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_notify.__exit__ = mock.Mock(return_value=None)
        with raises(DontKillEverythingError):
            cleanup_unused_apps('soa_dir', kill_threshold=0, force=False)


def test_cleanup_unused_apps_force():
    mock_kube_client = MockKubeClient()
    namespace = 'paasta'
    mock_kube_client.deployments.create_namespaced_deployment(namespace, fake_deployment)
    with mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.KubeClient',
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster',
        return_value={},
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.notify',
        autospec=True,
    ) as mock_notify:
        mock_notify.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_notify.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps('soa_dir', kill_threshold=0, force=True)
        assert len(mock_kube_client.deployments.list_namespaced_deployment(namespace).items) == 0


def test_cleanup_unused_apps_ignore_invalid_apps():
    mock_kube_client = MockKubeClient()
    namespace = 'paasta'
    mock_kube_client.deployments.create_namespaced_deployment(namespace, invalid_app)
    with mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.KubeClient',
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster',
        return_value={},
        autospec=True,
    ), mock.patch(
        'paasta_tools.cleanup_kubernetes_jobs.notify',
        autospec=True,
    ) as mock_notify:
        mock_notify.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_notify.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps('soa_dir', kill_threshold=0, force=True)
        assert invalid_app in mock_kube_client.deployments.list_namespaced_deployment(namespace).items
