import check_marathon_services_replication
import mock
import contextlib

import pysensu_yelp

from paasta_tools.smartstack_tools import DEFAULT_SYNAPSE_PORT

check_marathon_services_replication.log = mock.Mock()


def test_send_event():
    fake_service_name = 'superfast'
    fake_namespace = 'jellyfish'
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    fake_monitoring_overrides = {'fake_key': 'fake_value'}
    fake_soa_dir = '/hi/hello/hey'
    fake_cluster = 'fake_cluster'
    expected_check_name = 'check_marathon_services_replication.%s.%s' % (fake_service_name, fake_namespace)
    with contextlib.nested(
        mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
        mock.patch('check_marathon_services_replication.load_system_paasta_config', autospec=True),
        mock.patch("paasta_tools.check_marathon_services_replication._log", autospec=True),
        mock.patch("paasta_tools.marathon_tools.load_marathon_service_config", autospec=True),
    ) as (
        send_event_patch,
        load_system_paasta_config_patch,
        log_patch,
        load_marathon_service_config_patch,
    ):
        load_system_paasta_config_patch.return_value.get_cluster.return_value = fake_cluster
        load_marathon_service_config_patch.return_value.get_monitoring.return_value = fake_monitoring_overrides
        check_marathon_services_replication.send_event(fake_service_name,
                                                       fake_namespace,
                                                       fake_soa_dir,
                                                       fake_status,
                                                       fake_output)
        send_event_patch.assert_called_once_with(
            fake_service_name,
            expected_check_name,
            fake_monitoring_overrides,
            fake_status,
            fake_output,
            fake_soa_dir
        )


def test_split_id():
    fake_name = 'zero'
    fake_ns = 'hints'
    fake_id = '%s.%s' % (fake_name, fake_ns)
    expected = (fake_name, fake_ns)
    assert check_marathon_services_replication.split_id(fake_id) == expected


def test_add_context_to_event():
    service = 'fake_service'
    instance = 'fake_instance'
    output = 'fake_output'
    fake_context = 'fake_context'

    with mock.patch('check_marathon_services_replication.get_context', autospec=True) as get_context_patch:
        get_context_patch.return_value = "fake_context"
        actual = check_marathon_services_replication.add_context_to_event(service, instance, output)
        get_context_patch.assert_called_once_with(service, instance)
        assert fake_context in actual


def test_is_under_replicated_ok():
    num_available = 1
    expected_count = 1
    crit_threshold = 50
    actual = check_marathon_services_replication.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_zero():
    num_available = 1
    expected_count = 0
    crit_threshold = 50
    actual = check_marathon_services_replication.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_critical():
    num_available = 0
    expected_count = 1
    crit_threshold = 50
    actual = check_marathon_services_replication.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (True, float(0))


def test_check_smartstack_replication_for_instance_ok_when_expecting_zero():
    service = 'test'
    instance = 'main'
    available = {'fake_region': {'test.main': 1, 'test.three': 4, 'test.four': 8}}
    expected_replication_count = 0
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.OK, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_absent():
    service = 'test'
    instance = 'some_absent_instance'
    available = {'fake_region': {'test.two': 1, 'test.three': 4, 'test.four': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_zero_replication():
    service = 'test'
    instance = 'zero_running'
    available = {'fake_region': {'test.zero_running': 0, 'test.main': 8, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_low_replication():
    service = 'test'
    instance = 'not_enough'
    available = {'fake_region': {'test.canary': 1, 'test.not_enough': 4, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_ok_with_enough_replication():
    service = 'test'
    instance = 'everything_up'
    available = {'fake_region': {'test.canary': 1, 'test.low_replication': 4, 'test.everything_up': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.OK, mock.ANY)


def test_check_smartstack_replication_for_instance_ignores_things_under_a_different_namespace():
    service = 'test'
    instance = 'main'
    namespace = 'canary'
    available = {'fake_region': {'test.canary': 1, 'test.main': 4, 'test.fully_replicated': 8}}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=namespace),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.call_count == 0


def test_check_smartstack_replication_for_instance_ok_with_enough_replication_multilocation():
    service = 'test'
    instance = 'everything_up'
    available = {'fake_region': {'test.everything_up': 1}, 'fake_other_region': {'test.everything_up': 1}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.OK, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_low_replication_multilocation():
    service = 'test'
    instance = 'low_replication'
    available = {'fake_region': {'test.low_replication': 1}, 'fake_other_region': {'test.low_replication': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_zero_replication_multilocation():
    service = 'test'
    instance = 'zero_running'
    available = {'fake_region': {'test.zero_running': 0}, 'fake_other_region': {'test.zero_running': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_missing_replication_multilocation():
    service = 'test'
    instance = 'missing_instance'
    available = {'fake_region': {'test.main': 0}, 'fake_other_region': {'test.main': 0}}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_smartstack_replication_for_instance_crit_when_no_smartstack_info():
    service = 'test'
    instance = 'some_instance'
    available = {}
    expected_replication_count = 2
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_read_namespace_for_service_instance,
        mock_get_context,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event.assert_called_once_with(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)


def test_check_service_replication_for_normal_smartstack():
    service = 'test_service'
    instance = 'test_instance'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance',
                   autospec=True, return_value=666),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True, return_value=100),
        mock.patch('check_marathon_services_replication.check_smartstack_replication_for_instance',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
        mock_check_smartstack_replication_for_service
    ):
        check_marathon_services_replication.check_service_replication(
            service, instance, None, {'fake_location': {}}, None)
        mock_check_smartstack_replication_for_service.assert_called_once_with(
            service, instance, mock.ANY, mock.ANY, mock.ANY, mock.ANY)


def test_check_service_replication_for_non_smartstack():
    service = 'test_service'
    instance = 'worker'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance', autospec=True, return_value=None),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True, return_value=100),
        mock.patch('check_marathon_services_replication.check_mesos_replication_for_service', autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
        mock_get_mesos_replication_for_service,
    ):
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        mock_get_mesos_replication_for_service.assert_called_once_with(service, instance, None, None, 100)


def test_check_service_replication_for_namespace_with_no_deployments():
    service = 'test_service'
    instance = 'worker'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance', autospec=True, return_value=None),
        mock.patch('paasta_tools.marathon_tools.get_expected_instance_count_for_namespace',
                   autospec=True),
        mock.patch('check_marathon_services_replication.check_mesos_replication_for_service', autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_expected_count,
        mock_get_mesos_replication_for_service,
    ):
        mock_get_expected_count.side_effect = check_marathon_services_replication.NoDeploymentsAvailable
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        assert mock_get_proxy_port_for_instance.call_count == 0


def test_check_mesos_replication_for_service_good():
    service = 'test_service'
    instance = 'worker'
    running_tasks = ['a', 'b']
    crit = 90
    expected_tasks = 66
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('check_marathon_services_replication.get_running_tasks_from_active_frameworks', autospec=True),
    ) as (
        mock_send_event_if_under_replication,
        mock_get_running_tasks_from_active_frameworks,
    ):
        mock_get_running_tasks_from_active_frameworks.return_value = running_tasks
        check_marathon_services_replication.check_mesos_replication_for_service(
            service, instance, None, crit, expected_tasks)
        mock_get_running_tasks_from_active_frameworks.assert_called_once_with(service, instance)
        mock_send_event_if_under_replication.assert_called_once_with(
            service=service,
            instance=instance,
            crit_threshold=crit,
            expected_count=expected_tasks,
            num_available=len(running_tasks),
            soa_dir=None
        )


def test_send_event_if_under_replication_handles_0_expected():
    service = 'test_service'
    instance = 'worker'
    crit = 90
    expected_count = 0
    available = 0
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_get_context,
    ):
        check_marathon_services_replication.send_event_if_under_replication(
            service, instance, crit, expected_count, available, soa_dir)
        mock_send_event.assert_called_once_with(service, instance, soa_dir, 0, mock.ANY)


def test_send_event_if_under_replication_good():
    service = 'test_service'
    instance = 'worker'
    crit = 90
    expected_count = 100
    available = 100
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_get_context,
    ):
        check_marathon_services_replication.send_event_if_under_replication(
            service, instance, crit, expected_count, available, soa_dir)
        mock_send_event.assert_called_once_with(service, instance, soa_dir, 0, mock.ANY)


def test_send_event_if_under_replication_critical():
    service = 'test_service'
    instance = 'worker'
    crit = 90
    expected_count = 100
    available = 89
    soa_dir = '/dne'
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        mock_send_event,
        mock_get_context,
    ):
        check_marathon_services_replication.send_event_if_under_replication(
            service, instance, crit, expected_count, available, soa_dir)
        mock_send_event.assert_called_once_with(service, instance, soa_dir, 2, mock.ANY)


def test_load_smartstack_info_for_services():
    fake_namespaces = ['fake_instance1', 'fake_instance2']
    fake_services = [('fake_service1', 'fake_instance1'), ('fake_service2', 'fake_instance2')]

    fake_values_and_replication_info = {
        'fake_value_1': {},
        'fake_other_value': {}
    }
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('check_marathon_services_replication.get_smartstack_replication_for_attribute',
                   autospec=True, return_value=fake_values_and_replication_info)
    ) as (
        mock_load_service_namespace_config,
        mock_get_smartstack_replication_for_attribute
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_attribute'
        expected = {
            'fake_attribute': fake_values_and_replication_info
        }
        actual = check_marathon_services_replication.load_smartstack_info_for_services(fake_services, fake_namespaces,
                                                                                       'fake_soa_dir')
        assert actual == expected
        mock_get_smartstack_replication_for_attribute.assert_called_once_with('fake_attribute', fake_namespaces)


def test_get_smartstack_replication_for_attribute():
    fake_namespaces = ['fake_instance1', 'fake_instance2']

    fake_values_and_hosts = {
        'fake_value_1': ['fake_host_1', 'fake_host_3'],
        'fake_other_value': ['fake_host_4'],
    }
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.get_mesos_slaves_grouped_by_attribute',
                   return_value=fake_values_and_hosts),
        mock.patch('paasta_tools.monitoring.replication_utils.get_replication_for_services',
                   return_value={}, autospec=True),
    ) as (
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_replication_for_services,
    ):
        expected = {
            'fake_value_1': {},
            'fake_other_value': {}
        }
        actual = check_marathon_services_replication.get_smartstack_replication_for_attribute(
            'fake_attribute', fake_namespaces)
        assert actual == expected
        assert mock_get_replication_for_services.call_count == 2
        mock_get_mesos_slaves_grouped_by_attribute.assert_called_once_with('fake_attribute')
        mock_get_replication_for_services.assert_any_call(
            synapse_host='fake_host_1',
            synapse_port=DEFAULT_SYNAPSE_PORT,
            service_names=fake_namespaces,
        )


def test_main():
    soa_dir = 'anw'
    crit = 1
    services = [('a', 'main'), ('b', 'main'), ('c', 'main')]
    namespaces = [('a.main', 1), ('b.main', 2), ('c.main', 3)]
    args = mock.Mock(soa_dir=soa_dir, crit=crit, verbose=False)
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.parse_args',
                   return_value=args, autospec=True),
        mock.patch('paasta_tools.marathon_tools.get_all_namespaces',
                   return_value=namespaces, autospec=True),
        mock.patch('check_marathon_services_replication.get_services_for_cluster',
                   return_value=services, autospec=True),
        mock.patch('check_marathon_services_replication.check_service_replication',
                   autospec=True),
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('check_marathon_services_replication.load_smartstack_info_for_services', autospec=True,
                   return_value={'fake_attribute': {}})
    ) as (
        mock_parse_args,
        mock_get_all_namespaces,
        mock_get_services_for_cluster,
        mock_check_service_replication,
        mock_load_service_namespace_config,
        mock_load_smartstack_info,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_attribute'
        check_marathon_services_replication.main()
        mock_parse_args.assert_called_once_with()
        mock_get_services_for_cluster.assert_called_once_with(instance_type='marathon', soa_dir=soa_dir)
        mock_check_service_replication.call_count = len(services)
