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
        'source': 'mesos-fake_cluster',
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


def test_check_namespaces():
    namespaces = ['test.one', 'test.two', 'test.three', 'test.four', 'test.five']
    available = {'test.two': 1, 'test.three': 4, 'test.four': 8}
    expected = [0, 8, 8, 8, 8]
    soa_dir = 'test_dir'
    crit = 90
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.marathon_tools.get_expected_instance_count_for_namespace',
                   side_effect=lambda a, b, soa_dir=soa_dir: expected.pop()),
        mock.patch('check_marathon_services_replication.send_event'),
        mock.patch('check_marathon_services_replication.get_context'),
    ) as (
        expected_patch,
        event_patch,
        context_patch,
    ):
        check_marathon_services_replication.check_namespaces(namespaces, available, soa_dir, crit)
        expected_patch.assert_any_call('test', 'one', soa_dir=soa_dir)
        expected_patch.assert_any_call('test', 'two', soa_dir=soa_dir)
        expected_patch.assert_any_call('test', 'three', soa_dir=soa_dir)
        expected_patch.assert_any_call('test', 'four', soa_dir=soa_dir)
        expected_patch.assert_any_call('test', 'five', soa_dir=soa_dir)
        assert expected_patch.call_count == 5
        event_patch.assert_any_call('test', 'one', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        event_patch.assert_any_call('test', 'two', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        event_patch.assert_any_call('test', 'three', soa_dir, pysensu_yelp.Status.CRITICAL, mock.ANY)
        event_patch.assert_any_call('test', 'four', soa_dir, pysensu_yelp.Status.OK, mock.ANY)
        assert event_patch.call_count == 4
        # Context should have been requested for anything not ok
        context_patch.assert_any_call('test', 'one')
        context_patch.assert_any_call('test', 'two')
        context_patch.assert_any_call('test', 'three')
        assert context_patch.call_count == 3


def test_main():
    soa_dir = 'anw'
    crit = 1
    synapse_host = 'hammer:time'
    namespaces = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
    actual_namespaces = ['a', 'b', 'c', 'd']
    replication = 'reeeeeeeeeeeplicated'
    args = mock.Mock(soa_dir=soa_dir, crit=crit, synapse_host_port=synapse_host, verbose=False)
    with contextlib.nested(
        mock.patch('check_marathon_services_replication.parse_args', return_value=args),
        mock.patch('paasta_tools.marathon_tools.get_all_namespaces',
                   return_value=namespaces),
        mock.patch('paasta_tools.monitoring.replication_utils.get_replication_for_services',
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
        check_patch.assert_called_once_with(actual_namespaces, replication, soa_dir, crit)
