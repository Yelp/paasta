import check_marathon_services_replication
import mock
import contextlib
import pysensu_yelp


check_marathon_services_replication.log = mock.Mock()


def test_send_event():
    fake_service_name = 'superfast'
    fake_namespace = 'jellyfish'
    fake_status = '999999'
    fake_output = 'YOU DID IT'
    fake_runbook = 'y/notreally'
    fake_team = 'fake_mean'
    fake_tip = 'fake_pit'
    fake_notification_email = 'notify@fake'
    fake_page = True
    expected_kwargs = {
        'tip': fake_tip,
        'notification_email': fake_notification_email,
        'page': fake_page,
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
    }
    expected_check_name = 'check_marathon_services_replication.%s.%s' % (fake_service_name, fake_namespace)
    with contextlib.nested(
        mock.patch("service_deployment_tools.monitoring_tools.get_team",
                   return_value=fake_team),
        mock.patch("service_deployment_tools.monitoring_tools.get_runbook",
                   return_value=fake_runbook),
        mock.patch("service_deployment_tools.monitoring_tools.get_tip",
                   return_value=fake_tip),
        mock.patch("service_deployment_tools.monitoring_tools.get_notification_email",
                   return_value=fake_notification_email),
        mock.patch("service_deployment_tools.monitoring_tools.get_page",
                   return_value=fake_page),
        mock.patch("pysensu_yelp.send_event"),
    ) as (
        monitoring_tools_get_team_patch,
        monitoring_tools_get_runbook_patch,
        monitoring_tools_get_tip_patch,
        monitoring_tools_get_notification_email_patch,
        monitoring_tools_get_page_patch,
        pysensu_yelp_send_event_patch,
    ):
        check_marathon_services_replication.send_event(fake_service_name,
                                                       fake_namespace,
                                                       fake_status,
                                                       fake_output)
        monitoring_tools_get_team_patch.assert_called_once_with('marathon', fake_service_name)
        monitoring_tools_get_runbook_patch.assert_called_once_with('marathon', fake_service_name)
        monitoring_tools_get_tip_patch.assert_called_once_with('marathon', fake_service_name)
        monitoring_tools_get_notification_email_patch.assert_called_once_with('marathon', fake_service_name)
        monitoring_tools_get_page_patch.assert_called_once_with('marathon', fake_service_name)
        pysensu_yelp_send_event_patch.assert_called_once_with(expected_check_name, fake_runbook, fake_status,
                                                              fake_output, fake_team, **expected_kwargs)


def test_split_id():
    fake_name = 'zero'
    fake_ns = 'hints'
    fake_id = '%s.%s' % (fake_name, fake_ns)
    expected = (fake_name, fake_ns)
    assert check_marathon_services_replication.split_id(fake_id) == expected


def test_get_expected_instances():
    service_name = 'red'
    namespace = 'rojo'
    soa_dir = 'que_esta'
    fake_instances = [(service_name, 'blue'), (service_name, 'green')]
    fake_srv_config = {'nerve_ns': 'rojo'}

    def config_helper(name, inst, soa_dir=None):
        if inst == 'blue':
            return fake_srv_config
        else:
            return {'nerve_ns': 'amarillo'}

    with contextlib.nested(
        mock.patch('service_deployment_tools.marathon_tools.get_service_instance_list',
                   return_value=fake_instances),
        mock.patch('service_deployment_tools.marathon_tools.read_service_config',
                   side_effect=config_helper),
        mock.patch('service_deployment_tools.marathon_tools.get_instances',
                   return_value=11)
    ) as (
        inst_list_patch,
        read_config_patch,
        get_inst_patch
    ):
        actual = check_marathon_services_replication.get_expected_instances(service_name, namespace, soa_dir)
        assert actual == 11
        inst_list_patch.assert_called_once_with(service_name, soa_dir=soa_dir)
        read_config_patch.assert_any_call(service_name, 'blue', soa_dir=soa_dir)
        read_config_patch.assert_any_call(service_name, 'green', soa_dir=soa_dir)
        get_inst_patch.assert_called_once_with(fake_srv_config)


def test_check_namespaces():
    namespaces = ['test.one', 'test.two', 'test.three', 'test.four', 'test.five']
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    warn = 50
    crit = 12.5
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.get_expected_instances',
                   side_effect=lambda a, b, c: expected.pop()),
        mock.patch('check_marathon_services_replication.send_event'),
    ) as (
        expected_patch,
        event_patch
    ):
        check_marathon_services_replication.check_namespaces(namespaces, available, soa_dir, crit, warn)
        expected_patch.assert_any_call('test', 'one', soa_dir)
        expected_patch.assert_any_call('test', 'two', soa_dir)
        expected_patch.assert_any_call('test', 'three', soa_dir)
        expected_patch.assert_any_call('test', 'four', soa_dir)
        expected_patch.assert_any_call('test', 'five', soa_dir)
        assert expected_patch.call_count == 5
        event_patch.assert_any_call('test', 'one', pysensu_yelp.Status.CRITICAL, mock.ANY)
        event_patch.assert_any_call('test', 'two', pysensu_yelp.Status.CRITICAL, mock.ANY)
        event_patch.assert_any_call('test', 'three', pysensu_yelp.Status.WARNING, mock.ANY)
        event_patch.assert_any_call('test', 'four', pysensu_yelp.Status.OK, mock.ANY)
        assert event_patch.call_count == 4


def test_main():
    soa_dir = 'anw'
    warn = 0
    crit = 1
    synapse_host = 'hammer:time'
    namespaces = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
    actual_namespaces = ['a', 'b', 'c', 'd']
    replication = 'reeeeeeeeeeeplicated'
    args = mock.Mock(soa_dir=soa_dir, warn=warn, crit=crit, synapse_host_port=synapse_host, verbose=False)
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.parse_args', return_value=args),
        mock.patch('service_deployment_tools.marathon_tools.get_all_namespaces',
                   return_value=namespaces),
        mock.patch('service_deployment_tools.monitoring.replication_utils.get_replication_for_services',
                   return_value=replication),
        mock.patch('check_marathon_services_replication.check_namespaces')
    ) as (
        args_patch,
        namespaces_patch,
        replication_patch,
        check_patch
    ):
        check_marathon_services_replication.main()
        args_patch.assert_called_once_with()
        namespaces_patch.assert_called_once_with()
        replication_patch.assert_called_once_with(synapse_host, actual_namespaces)
        check_patch.assert_called_once_with(actual_namespaces, replication, soa_dir, crit, warn)
