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
    fake_irc = '#fake'
    fake_page = True
    fake_soa_dir = '/hi/hello/hey'
    fake_cluster = 'fake_cluster'
    expected_kwargs = {
        'tip': fake_tip,
        'notification_email': fake_notification_email,
        'page': fake_page,
        'irc_channels': fake_irc,
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
        'source': 'paasta-fake_cluster',
    }
    expected_check_name = 'check_marathon_services_replication.%s.%s' % (fake_service_name, fake_namespace)
    with contextlib.nested(
        mock.patch("paasta_tools.monitoring_tools.get_team",
                   return_value=fake_team),
        mock.patch("paasta_tools.monitoring_tools.get_runbook",
                   return_value=fake_runbook),
        mock.patch("paasta_tools.monitoring_tools.get_tip",
                   return_value=fake_tip),
        mock.patch("paasta_tools.monitoring_tools.get_notification_email",
                   return_value=fake_notification_email),
        mock.patch("paasta_tools.monitoring_tools.get_page",
                   return_value=fake_page),
        mock.patch("paasta_tools.monitoring_tools.get_irc_channels",
                   return_value=fake_irc),
        mock.patch("pysensu_yelp.send_event"),
        mock.patch('paasta_tools.marathon_tools.get_cluster',
                   return_value=fake_cluster),
        mock.patch("paasta_tools.check_marathon_services_replication._log")
    ) as (
        monitoring_tools_get_team_patch,
        monitoring_tools_get_runbook_patch,
        monitoring_tools_get_tip_patch,
        monitoring_tools_get_notification_email_patch,
        monitoring_tools_get_page_patch,
        monitoring_tools_get_irc_patch,
        pysensu_yelp_send_event_patch,
        cluster_patch,
        log_patch,
    ):
        check_marathon_services_replication.send_event(fake_service_name,
                                                       fake_namespace,
                                                       fake_soa_dir,
                                                       fake_status,
                                                       fake_output)
        monitoring_tools_get_team_patch.assert_called_once_with('marathon', fake_service_name,
                                                                soa_dir=fake_soa_dir)
        monitoring_tools_get_runbook_patch.assert_called_once_with('marathon', fake_service_name,
                                                                   soa_dir=fake_soa_dir)
        monitoring_tools_get_tip_patch.assert_called_once_with('marathon', fake_service_name,
                                                               soa_dir=fake_soa_dir)
        monitoring_tools_get_notification_email_patch.assert_called_once_with('marathon', fake_service_name,
                                                                              soa_dir=fake_soa_dir)
        monitoring_tools_get_page_patch.assert_called_once_with('marathon', fake_service_name,
                                                                soa_dir=fake_soa_dir)
        monitoring_tools_get_irc_patch.assert_called_once_with('marathon', fake_service_name,
                                                               soa_dir=fake_soa_dir)
        pysensu_yelp_send_event_patch.assert_called_once_with(expected_check_name, fake_runbook, fake_status,
                                                              fake_output, fake_team, **expected_kwargs)
        cluster_patch.assert_called_once_with()


def test_split_id():
    fake_name = 'zero'
    fake_ns = 'hints'
    fake_id = '%s.%s' % (fake_name, fake_ns)
    expected = (fake_name, fake_ns)
    assert check_marathon_services_replication.split_id(fake_id) == expected


def test_check_smarstack_replication_for_namespace_crit_when_absent():
    namespace = 'test.one'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_counts = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected_replication_counts.pop(), autospec=True),
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_namespace(
            namespace, available, soa_dir, crit
        )
        expected_patch.assert_any_call('test', 'one', soa_dir=soa_dir)
        event_patch.assert_any_call('test', 'one', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        context_patch.assert_any_call('test', 'one')


def test_check_smarstack_replication_for_namespace_crit_when_zero_replication():
    namespace = 'test.two'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_counts = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected_replication_counts.pop(), autospec=True),
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_namespace(
            namespace, available, soa_dir, crit
        )
        expected_patch.assert_any_call('test', 'two', soa_dir=soa_dir)
        event_patch.assert_any_call('test', 'two', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        context_patch.assert_any_call('test', 'two')


def test_check_smarstack_replication_for_namespace_crit_when_low_replication():
    namespace = 'test.three'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_counts = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected_replication_counts.pop(), autospec=True),
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_namespace(
            namespace, available, soa_dir, crit
        )
        expected_patch.assert_any_call('test', 'three', soa_dir=soa_dir)
        event_patch.assert_any_call('test', 'three', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        context_patch.assert_any_call('test', 'three')


def test_check_smarstack_replication_for_namespace_ok_with_enough_replication():
    namespace = 'test.four'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_counts = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected_replication_counts.pop(), autospec=True),
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_namespace(
            namespace, available, soa_dir, crit
        )
        expected_patch.assert_any_call('test', 'four', soa_dir=soa_dir)
        event_patch.assert_any_call('test', 'four', soa_dir, pysensu_yelp.Status.OK, mock.ANY)
        context_patch.call_count == 0


def test_check_smarstack_replication_for_namespace_ignores_bogus_namespaces():
    namespace = 'test.five'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_counts = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected_replication_counts.pop(), autospec=True),
        mock.patch('check_marathon_services_replication.send_event', autospec=True),
        mock.patch('check_marathon_services_replication.get_context', autospec=True),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_namespace(
            namespace, available, soa_dir, crit
        )
        expected_patch.assert_any_call('test', 'five', soa_dir=soa_dir)
        event_patch.call_count == 0
        context_patch.call_count == 0


def test_check_service_replication_for_normal_smartstack():
    service = 'test_service'
    instance = 'test_instance'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance',
                   autospec=True, return_value=666),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
        mock.patch('check_marathon_services_replication.check_smartstack_replication_for_namespace',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_read_namespace_for_service_instance,
        mock_get_smartstack_replication_for_service
    ):
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        full_name = "%s.%s" % (service, instance)
        mock_get_smartstack_replication_for_service.assert_called_once_with(full_name, None, None, None)
        assert mock_get_smartstack_replication_for_service.call_count == 1


def test_check_service_replication_for_smartstack_under_other_namespace():
    service = 'test_service'
    instance = 'canary'
    namespace = 'main'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance',
                   autospec=True, return_value=666),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=namespace),
        mock.patch('check_marathon_services_replication.check_smartstack_replication_for_namespace',
                   autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_read_namespace_for_service_instance,
        mock_get_smartstack_replication_for_service
    ):
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        assert mock_get_smartstack_replication_for_service.call_count == 0


def test_check_service_replication_for_non_smartstack():
    service = 'test_service'
    instance = 'worker'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.get_proxy_port_for_instance', autospec=True, return_value=None),
        mock.patch('check_marathon_services_replication.check_mesos_replication_for_service', autospec=True),
    ) as (
        mock_get_proxy_port_for_instance,
        mock_get_mesos_replication_for_service,
    ):
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        mock_get_mesos_replication_for_service.assert_called_once_with(service, instance, None, None)


def test_check_mesos_replication_for_service_good():
    check_marathon_services_replication.mesos_replication_for_service(
    pass


def test_check_mesos_replication_for_service_bad():
    pass


def test_main():
    soa_dir = 'anw'
    crit = 1
    services = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
    replication = 'reeeeeeeeeeeplicated'
    args = mock.Mock(soa_dir=soa_dir, crit=crit, verbose=False)
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.parse_args',
                   return_value=args, autospec=True),
        mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster',
                   return_value=services, autospec=True),
        mock.patch('paasta_tools.monitoring.replication_utils.get_replication_for_services',
                   return_value=replication, autospec=True),
        mock.patch('check_marathon_services_replication.check_service_replication',
                   autospec=True)
    ) as (
        mock_parse_args,
        mock_get_marathon_services_for_cluster,
        mock_get_replication_for_services,
        mock_check_service_replication,
    ):
        check_marathon_services_replication.main()
        mock_parse_args.assert_called_once_with()
        mock_get_marathon_services_for_cluster.assert_called_once_with(cluster=mock.ANY, soa_dir=soa_dir)
        mock_get_replication_for_services.assert_called_once_with(mock.ANY, services)
        mock_check_service_replication.call_count = len(services)
