#!/usr/bin/env python

import contextlib

import mock

from paasta_tools import paasta_serviceinit


def test_validate_service_instance_valid_marathon():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    my_service = 'service1'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_serviceinit.get_services_for_cluster',
                   autospec=True,
                   side_effect=[mock_marathon_services, mock_chronos_services]),
    ) as (
        get_services_for_cluster_patch,
    ):
        assert paasta_serviceinit.validate_service_instance(
            my_service,
            my_instance,
            fake_cluster,
            fake_soa_dir,
        ) == 'marathon'
        assert mock.call(
            cluster=fake_cluster,
            instance_type='marathon',
            soa_dir=fake_soa_dir,
        ) in get_services_for_cluster_patch.call_args_list


def test_validate_service_instance_valid_chronos():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    my_service = 'service1'
    my_instance = 'worker'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_serviceinit.get_services_for_cluster',
                   autospec=True,
                   side_effect=[mock_marathon_services, mock_chronos_services]),
    ) as (
        get_services_for_cluster_patch,
    ):
        assert paasta_serviceinit.validate_service_instance(
            my_service,
            my_instance,
            fake_cluster,
            fake_soa_dir,
        ) == 'chronos'
        assert mock.call(
            cluster=fake_cluster,
            instance_type='chronos',
            soa_dir=fake_soa_dir,
        ) in get_services_for_cluster_patch.call_args_list


def test_validate_service_instance_invalid():
    mock_marathon_services = [('service1', 'main'), ('service2', 'main')]
    mock_chronos_services = [('service1', 'worker'), ('service2', 'tailer')]
    my_service = 'bad_service'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    fake_soa_dir = 'fake_soa_dir'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_serviceinit.get_services_for_cluster',
                   autospec=True,
                   side_effect=[mock_marathon_services, mock_chronos_services]),
        mock.patch('sys.exit'),
    ) as (
        get_services_for_cluster_patch,
        sys_exit_patch,
    ):
        paasta_serviceinit.validate_service_instance(
            my_service,
            my_instance,
            fake_cluster,
            fake_soa_dir,
        )
        sys_exit_patch.assert_called_once_with(3)
