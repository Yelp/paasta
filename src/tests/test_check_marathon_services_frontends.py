import check_marathon_services_frontends
import mock
import contextlib
import subprocess
import pysensu_yelp


def test_build_check_command():
    mode = 'paasta'
    port = 666
    expected = '/usr/lib/nagios/plugins/check_%s -H localhost -p %d' % (mode, port)
    actual = check_marathon_services_frontends.build_check_command(port, mode)
    assert expected == actual


def test_check_service():
    fake_port = 19343
    fake_command = '/usr/bin/check_sandwich working_girls'
    fake_output = 'vader_nooooooo.jpg'
    fake_mode = 'water'
    fake_process = mock.Mock(returncode=11, communicate=mock.Mock(return_value=(fake_output, '42')))
    expected = (11, fake_output)
    with contextlib.nested(
        mock.patch('check_marathon_services_frontends.build_check_command',
                   return_value=fake_command),
        mock.patch('subprocess.Popen',
                   return_value=fake_process)
    ) as (
        build_cmd_patch,
        popen_patch,
    ):
        actual = check_marathon_services_frontends.check_service(fake_port, fake_mode)
        assert expected == actual
        build_cmd_patch.assert_called_once_with(fake_port, fake_mode)
        popen_patch.assert_called_once_with(fake_command.split(),
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT)


def test_send_event():
    fake_service_name = 'fake_service'
    fake_instance_name = 'fake_instance'
    fake_check_name = 'soa_bla'
    fake_status = '42'
    fake_output = 'The http port is not open'
    fake_runbook = 'y/fakerunbook'
    fake_team = 'fake_team'
    fake_tip = 'fake_tip'
    fake_notification_email = 'fake@notify'
    fake_page = False
    expected_kwargs = {
        'tip': fake_tip,
        'notification_email': fake_notification_email,
        'page': fake_page,
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
    }
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
        check_marathon_services_frontends.send_event(fake_service_name,
                                                     fake_instance_name,
                                                     fake_check_name,
                                                     fake_status,
                                                     fake_output)
        monitoring_tools_get_team_patch.assert_called_once_with('marathon', fake_service_name, fake_instance_name)
        monitoring_tools_get_runbook_patch.assert_called_once_with('marathon', fake_service_name, fake_instance_name)
        monitoring_tools_get_tip_patch.assert_called_once_with('marathon', fake_service_name, fake_instance_name)
        monitoring_tools_get_notification_email_patch.assert_called_once_with('marathon', fake_service_name, fake_instance_name)
        monitoring_tools_get_page_patch.assert_called_once_with('marathon', fake_service_name, fake_instance_name)
        pysensu_yelp_send_event_patch.assert_called_once_with(fake_check_name, fake_runbook, fake_status,
                                                              fake_output, fake_team, **expected_kwargs)


def test_check_service_instance():
    fake_service_name = "fake_service"
    fake_instance_name = "fake_instance"
    fake_status = 42
    fake_output = "Check passed"
    fake_port = 666
    fake_mode = 'http'
    fake_soa_dir = '/zelda/link'
    expected_check_name = 'check_marathon_services_frontends.%s.%s' % (fake_service_name, fake_instance_name)
    with contextlib.nested(
        mock.patch("service_deployment_tools.marathon_tools.get_mode_for_instance",
                   return_value=fake_mode),
        mock.patch("service_deployment_tools.marathon_tools.get_proxy_port_for_instance",
                   return_value=fake_port),
        mock.patch("check_marathon_services_frontends.send_event"),
        mock.patch("check_marathon_services_frontends.check_service", return_value=(fake_status, fake_output))
    ) as (
        get_mode_patch,
        get_proxy_port_patch,
        send_event_patch,
        check_service_patch
    ):
        check_marathon_services_frontends.check_service_instance(fake_service_name, fake_instance_name,
                                                                 fake_soa_dir)
        get_mode_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                               soa_dir=fake_soa_dir)
        get_proxy_port_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                                     soa_dir=fake_soa_dir)
        check_service_patch.assert_called_once_with(fake_port, fake_mode)
        send_event_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                                 expected_check_name, fake_status, fake_output)


def test_check_service_instance_invalid_mode():
    fake_service_name = "apt_naming"
    fake_instance_name = "why_thank_you"
    fake_status = 42
    fake_output = "Check passed"
    fake_port = 666
    fake_mode = 'ycp'
    fake_soa_dir = '/zelda/link'
    expected_check_name = 'check_marathon_services_frontends.%s.%s' % (fake_service_name, fake_instance_name)
    with contextlib.nested(
        mock.patch("service_deployment_tools.marathon_tools.get_mode_for_instance",
                   return_value=fake_mode),
        mock.patch("service_deployment_tools.marathon_tools.get_proxy_port_for_instance",
                   return_value=fake_port),
        mock.patch("check_marathon_services_frontends.send_event"),
        mock.patch("check_marathon_services_frontends.check_service", return_value=(fake_status, fake_output))
    ) as (
        get_mode_patch,
        get_proxy_port_patch,
        send_event_patch,
        check_service_patch
    ):
        check_marathon_services_frontends.check_service_instance(fake_service_name, fake_instance_name,
                                                                 fake_soa_dir)
        get_mode_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                               soa_dir=fake_soa_dir)
        get_proxy_port_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                                     soa_dir=fake_soa_dir)
        assert check_service_patch.call_count == 0
        send_event_patch.assert_called_once_with(fake_service_name, fake_instance_name,
                                                 expected_check_name, pysensu_yelp.Status.CRITICAL,
                                                 mock.ANY)


def test_main():
    fake_dir = '/etc/nail/etc/nail'
    fake_args = mock.Mock(soa_dir=fake_dir, verbose=False)
    fake_service_list = [('fake_service1', 'fake_instance1'), ('fake_service2', 'fake_instance2')]
    with contextlib.nested(
            mock.patch("check_marathon_services_frontends.parse_args", return_value=fake_args),
            mock.patch("service_deployment_tools.marathon_tools.get_marathon_services_for_cluster",
                       return_value=fake_service_list),
            mock.patch("check_marathon_services_frontends.check_service_instance"),
    ) as (
        args_patch,
        get_marathon_services_for_cluster_patch,
        check_service_instance_patch
    ):
        check_marathon_services_frontends.main()
        args_patch.assert_called_once_with()
        get_marathon_services_for_cluster_patch.assert_called_once_with()
        assert check_service_instance_patch.call_count == len(fake_service_list)
        check_service_instance_patch.assert_any_call(fake_service_list[0][0], fake_service_list[0][1], fake_dir)
        check_service_instance_patch.assert_any_call(fake_service_list[1][0], fake_service_list[1][1], fake_dir)
