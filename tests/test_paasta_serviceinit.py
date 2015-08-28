#!/usr/bin/env python

import contextlib

import mock

from paasta_tools import marathon_serviceinit


def test_validate_service_instance_valid():
    mock_services = [('service1', 'main'), ('service2', 'main')]
    my_service = 'service1'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_services_for_cluster',
                   autospec=True,
                   return_value=mock_services),
    ) as (
        get_services_for_cluster_patch,
    ):
        assert marathon_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
        get_services_for_cluster_patch.assert_called_once_with(cluster=fake_cluster, instance_type='marathon')


def test_validate_service_instance_invalid():
    mock_services = [('service1', 'main'), ('service2', 'main')]
    my_service = 'bad_service'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_services_for_cluster',
                   autospec=True,
                   return_value=mock_services),
        mock.patch('sys.exit'),
    ) as (
        get_services_for_cluster_patch,
        sys_exit_patch,
    ):
        assert marathon_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
        sys_exit_patch.assert_called_once_with(3)
        get_services_for_cluster_patch.assert_called_once_with(cluster=fake_cluster, instance_type='marathon')
