import check_marathon_services_replication
import mock
import contextlib


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


def test_check_smarstack_replication_for_instance_crit_when_absent():
    service = 'test'
    instance = 'some_absent_instance'
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.assert_called_once_with(
            service, instance, crit, expected_replication_count, 0, soa_dir)


def test_check_smarstack_replication_for_instance_crit_when_zero_replication():
    service = 'test'
    instance = 'zero_running'
    available = {'test.zero_running': 0, 'test.main': 8, 'test.fully_replicated': 8}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.assert_called_once_with(
            service, instance, crit, expected_replication_count, 0, soa_dir)


def test_check_smarstack_replication_for_instance_crit_when_low_replication():
    service = 'test'
    instance = 'not_enough'
    available = {'test.canary': 1, 'test.not_enough': 4, 'test.fully_replicated': 8}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.assert_called_once_with(
            service, instance, crit, expected_replication_count, 4, soa_dir)


def test_check_smarstack_replication_for_instance_ok_with_enough_replication():
    service = 'test'
    instance = 'everything_up'
    available = {'test.canary': 1, 'test.low_replication': 4, 'test.everything_up': 8}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.assert_called_once_with(
            service, instance, crit, expected_replication_count, 8, soa_dir)


def test_check_smarstack_replication_for_instance_ignores_bogus_instance():
    service = 'test'
    instance = 'something_random'
    available = {'test.canary': 1, 'test.main': 4, 'test.fully_replicated': 8}
    expected_replication_count = 8
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.send_event_if_under_replication', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                   autospec=True, return_value=instance),
    ) as (
        mock_send_event_if_under_replication,
        mock_read_namespace_for_service_instance,
    ):
        check_marathon_services_replication.check_smartstack_replication_for_instance(
            service, instance, available, soa_dir, crit, expected_replication_count,
        )
        mock_send_event_if_under_replication.call_count == 0


def test_check_smarstack_replication_for_instance_ignores_things_under_a_different_namespace():
    service = 'test'
    instance = 'main'
    namespace = 'canary'
    available = {'test.canary': 1, 'test.main': 4, 'test.fully_replicated': 8}
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
        mock_get_smartstack_replication_for_service
    ):
        check_marathon_services_replication.check_service_replication(service, instance, None, None, None)
        mock_get_smartstack_replication_for_service.assert_called_once_with(
            service, instance, mock.ANY, mock.ANY, mock.ANY, mock.ANY)
        assert mock_get_smartstack_replication_for_service.call_count == 1


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
            service, instance, crit, expected_tasks, len(running_tasks), None)


def test_main():
    soa_dir = 'anw'
    crit = 1
    services = [('a', 'main'), ('b', 'main'), ('c', 'main')]
    namespaces = [('a.main', 1), ('b.main', 2), ('c.main', 3)]
    replication = 'reeeeeeeeeeeplicated'
    args = mock.Mock(soa_dir=soa_dir, crit=crit, verbose=False)
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.parse_args',
                   return_value=args, autospec=True),
        mock.patch('paasta_tools.marathon_tools.get_all_namespaces',
                   return_value=namespaces, autospec=True),
        mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster',
                   return_value=services, autospec=True),
        mock.patch('paasta_tools.monitoring.replication_utils.get_replication_for_services',
                   return_value=replication, autospec=True),
        mock.patch('check_marathon_services_replication.check_service_replication',
                   autospec=True)
    ) as (
        mock_parse_args,
        mock_get_all_namespaces,
        mock_get_marathon_services_for_cluster,
        mock_get_replication_for_services,
        mock_check_service_replication,
    ):
        check_marathon_services_replication.main()
        mock_parse_args.assert_called_once_with()
        mock_get_marathon_services_for_cluster.assert_called_once_with(soa_dir=soa_dir)
        mock_get_replication_for_services.assert_called_once_with(mock.ANY, mock.ANY)
        mock_check_service_replication.call_count = len(services)
