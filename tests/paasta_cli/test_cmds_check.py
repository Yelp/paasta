import contextlib
from StringIO import StringIO

from mock import patch, MagicMock, call

from paasta_tools.paasta_cli.cmds.check import deploy_check
from paasta_tools.paasta_cli.cmds.check import deploy_has_performance_check
from paasta_tools.paasta_cli.cmds.check import deploy_has_security_check
from paasta_tools.paasta_cli.cmds.check import docker_check
from paasta_tools.paasta_cli.cmds.check import docker_file_reads_from_yelpcorp
from paasta_tools.paasta_cli.cmds.check import get_marathon_steps
from paasta_tools.paasta_cli.cmds.check import makefile_check
from paasta_tools.paasta_cli.cmds.check import makefile_has_a_tab
from paasta_tools.paasta_cli.cmds.check import makefile_has_docker_tag
from paasta_tools.paasta_cli.cmds.check import makefile_responds_to
from paasta_tools.paasta_cli.cmds.check import marathon_check
from paasta_tools.paasta_cli.cmds.check import marathon_deployments_check
from paasta_tools.paasta_cli.cmds.check import NoSuchService
from paasta_tools.paasta_cli.cmds.check import paasta_check
from paasta_tools.paasta_cli.cmds.check import pipeline_check
from paasta_tools.paasta_cli.cmds.check import sensu_check
from paasta_tools.paasta_cli.cmds.check import service_dir_check
from paasta_tools.paasta_cli.cmds.check import smartstack_check
from paasta_tools.paasta_cli.utils import PaastaCheckMessages


@patch('paasta_tools.paasta_cli.cmds.check.git_repo_check')
@patch('paasta_tools.paasta_cli.cmds.check.pipeline_check')
@patch('paasta_tools.paasta_cli.cmds.check.service_dir_check')
@patch('paasta_tools.paasta_cli.cmds.check.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.check.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.check.deploy_check')
@patch('paasta_tools.paasta_cli.cmds.check.deploy_has_performance_check')
@patch('paasta_tools.paasta_cli.cmds.check.deploy_has_security_check')
@patch('paasta_tools.paasta_cli.cmds.check.docker_check')
@patch('paasta_tools.paasta_cli.cmds.check.makefile_check')
@patch('paasta_tools.paasta_cli.cmds.check.marathon_check')
@patch('paasta_tools.paasta_cli.cmds.check.marathon_deployments_check')
@patch('paasta_tools.paasta_cli.cmds.check.sensu_check')
@patch('paasta_tools.paasta_cli.cmds.check.smartstack_check')
def test_check_paasta_check_calls_everything(
        mock_smartstart_check,
        mock_sensu_check,
        mock_marathon_deployments_check,
        mock_marathon_check,
        mock_makefile_check,
        mock_docker_check,
        mock_deploy_check,
        mock_deploy_security_check,
        mock_deploy_performance_check,
        mock_guess_service_name,
        mock_validate_service_name,
        mock_service_dir_check,
        mock_pipeline_check,
        mock_git_repo_check
):
    # Ensure each check in 'paasta_check' is called

    mock_guess_service_name.return_value = 'servicedocs'
    mock_validate_service_name.return_value = None
    paasta_check(None)

    assert mock_git_repo_check.called
    assert mock_pipeline_check.called
    assert mock_service_dir_check.called
    assert mock_deploy_check.called
    assert mock_deploy_security_check.called
    assert mock_deploy_performance_check.called
    assert mock_docker_check.called
    assert mock_makefile_check.called
    assert mock_marathon_check.called
    assert mock_sensu_check.called
    assert mock_smartstart_check.called


@patch('paasta_tools.paasta_cli.cmds.check.validate_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_dir_check_pass(mock_stdout, mock_validate_service_name):
    mock_validate_service_name.return_value = None
    service_name = 'fake_service'
    expected_output = \
        "%s\n" % PaastaCheckMessages.service_dir_found(service_name)
    service_dir_check(service_name)
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.validate_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_dir_check_fail(mock_stdout, mock_validate_service_name):
    service_name = 'fake_service'
    mock_validate_service_name.side_effect = NoSuchService(service_name)
    expected_output = "%s\n" \
                      % PaastaCheckMessages.service_dir_missing(service_name)
    service_dir_check(service_name)
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_pass(mock_stdout, mock_is_file_in_dir):
    # Deploy check passes when file found in service path

    mock_is_file_in_dir.return_value = True

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_FOUND
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_fail(mock_stdout, mock_is_file_in_dir):
    # Deploy check fails when file not in service path

    mock_is_file_in_dir.return_value = False

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_MISSING
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('paasta_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_pass(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir):
    # Dockerfile exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_reads_from_yelpcorp.return_value = True

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_FOUND in output
    assert PaastaCheckMessages.DOCKERFILE_YELPCORP in output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('paasta_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_doesnt_read_yelpcorp(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir):

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_reads_from_yelpcorp.return_value = False

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_FOUND in output
    assert PaastaCheckMessages.DOCKERFILE_NOT_YELPCORP in output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('paasta_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_file_not_found(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir):

    mock_is_file_in_dir.return_value = False
    mock_docker_file_reads_from_yelpcorp.return_value = False

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_MISSING in output
    assert PaastaCheckMessages.DOCKERFILE_NOT_YELPCORP not in output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_marathon_check_pass(mock_stdout, mock_is_file_in_dir):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    expected_output = "%s\n" % PaastaCheckMessages.MARATHON_YAML_FOUND

    marathon_check('path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_marathon_check_fail(mock_stdout, mock_is_file_in_dir):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.MARATHON_YAML_MISSING

    marathon_check('path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('paasta_tools.paasta_cli.cmds.check.get_team')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_check_pass(mock_stdout, mock_get_team,
                                mock_is_file_in_dir):
    # monitoring.yaml exists and team is found

    mock_is_file_in_dir.return_value = "/fake/path"
    team = 'team-service-infra'
    mock_get_team.return_value = team
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SENSU_MONITORING_FOUND,
                                    PaastaCheckMessages.sensu_team_found(team))

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output
    mock_get_team.assert_called_once_with(service_name='fake_service', overrides={})


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('paasta_tools.paasta_cli.cmds.check.get_team')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_team_missing(mock_stdout, mock_get_team,
                                  mock_is_file_in_dir):
    # monitoring.yaml exists but team is not found

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_get_team.return_value = None
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SENSU_MONITORING_FOUND,
                                    PaastaCheckMessages.SENSU_TEAM_MISSING)

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_check_fail(mock_stdout, mock_is_file_in_dir):
    # monitoring.yaml doest exist

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.SENSU_MONITORING_MISSING

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_configuration_lib.'
       'read_service_configuration')
@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_pass(mock_stdout, mock_is_file_in_dir,
                                     mock_read_service_info):
    # smartstack.yaml exists and port is found

    mock_is_file_in_dir.return_value = True
    port = 80
    instance = 'main'
    smartstack_dict = {
        'smartstack': {
            instance: {
                'proxy_port': port
            }
        }
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                         PaastaCheckMessages.smartstack_port_found(
                             instance, port))

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_configuration_lib.'
       'read_service_configuration')
@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_missing_port(
        mock_stdout, mock_is_file_in_dir, mock_read_service_info):
    # smartstack.yaml, instance exists, but no ports found

    mock_is_file_in_dir.return_value = True
    instance = 'main'
    smartstack_dict = {
        instance: {
            'foo': 0
        }
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                         PaastaCheckMessages.SMARTSTACK_PORT_MISSING)

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.'
       'read_service_configuration')
@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_missing_instance(
        mock_stdout, mock_is_file_in_dir, mock_read_service_info):
    # smartstack.yaml exists, but no instances found

    mock_is_file_in_dir.return_value = True
    smartstack_dict = {}
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                         PaastaCheckMessages.SMARTSTACK_PORT_MISSING)

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_is_ok_when_no_smartstack(mock_stdout, mock_is_file_in_dir):

    mock_is_file_in_dir.return_value = False
    expected_output = ""
    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.urllib2.urlopen')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_pass(mock_stdout, mock_urlopen):
    mock_result = MagicMock()
    mock_result.getcode.return_value = 200
    mock_urlopen.return_value = mock_result
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_FOUND
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.urllib2.urlopen')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_fail_404(mock_stdout, mock_urlopen):
    mock_result = MagicMock()
    mock_result.getcode.return_value = 404
    mock_urlopen.return_value = mock_result
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_MISSING
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check.urllib2.HTTPERROR')
@patch('paasta_tools.paasta_cli.cmds.check.urllib2')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_fail_httperr(mock_stdout, mock_urllib2, mock_error):

    mock_urllib2.urlopen.side_effect = mock_error
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_MISSING
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.check._run')
def test_makefile_responds_to_good(mock_run):
    mock_run.return_value = (1, 'Output')
    actual = makefile_responds_to('present-target')
    assert actual is True


@patch('paasta_tools.paasta_cli.cmds.check._run')
def test_makefile_responds_to_run(mock_run):
    mock_run.return_value = (2, 'Output')
    actual = makefile_responds_to('non-present-target')
    assert actual is False


def test_makefile_has_a_tab_true():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n\tcommand'
    with contextlib.nested(
        patch(
            'paasta_tools.paasta_cli.cmds.check.get_file_contents',
            autospec=True,
            return_value=fake_contents
        ),
    ) as (
        mock_get_file_contents,
    ):
        assert makefile_has_a_tab(fake_makefile_path) is True


def test_makefile_has_a_tab_false():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n    command'
    with contextlib.nested(
        patch(
            'paasta_tools.paasta_cli.cmds.check.get_file_contents',
            autospec=True,
            return_value=fake_contents
        ),
    ) as (
        mock_get_file_contents,
    ):
        assert makefile_has_a_tab(fake_makefile_path) is False


def test_makefile_has_docker_tag_true():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'Blah\nDOCKER_TAG ?= something:\ntarget:\n    command'
    with contextlib.nested(
        patch(
            'paasta_tools.paasta_cli.cmds.check.get_file_contents',
            autospec=True,
            return_value=fake_contents
        ),
    ) as (
        mock_get_file_contents,
    ):
        assert makefile_has_docker_tag(fake_makefile_path) is True


def test_makefile_has_docker_tag_false():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n    command'
    with contextlib.nested(
        patch(
            'paasta_tools.paasta_cli.cmds.check.get_file_contents',
            autospec=True,
            return_value=fake_contents
        ),
    ) as (
        mock_get_file_contents,
    ):
        assert makefile_has_docker_tag(fake_makefile_path) is False


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
def test_deploy_has_security_check_false(mock_pipeline_config, mock_stdout):
    mock_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    actual = deploy_has_security_check('fake_service')
    assert actual is False


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
def test_deploy_has_security_check_true(mock_pipeline_config, mock_stdout):
    mock_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'security-check', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    actual = deploy_has_security_check('fake_service')
    assert actual is True


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
def test_deploy_has_performance_check_false(mock_pipeline_config, mock_stdout):
    mock_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    actual = deploy_has_performance_check('fake_service')
    assert actual is False


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
def test_deploy_has_performance_check_true(mock_pipeline_config, mock_stdout):
    mock_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'performance-check', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    actual = deploy_has_performance_check('fake_service')
    assert actual is True


@patch('paasta_tools.paasta_cli.cmds.check.list_clusters')
@patch('paasta_tools.paasta_cli.cmds.check.get_service_instance_list')
def test_get_marathon_steps(
    mock_get_service_instance_list,
    mock_list_clusters,
):
    mock_list_clusters.return_value = ['cluster1']
    mock_get_service_instance_list.return_value = [('unused', 'instance1'), ('unused', 'instance2')]
    expected = ['cluster1.instance1', 'cluster1.instance2']
    actual = get_marathon_steps('unused')
    assert actual == expected


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
@patch('paasta_tools.paasta_cli.cmds.check.get_marathon_steps')
def test_marathon_deployments_check_good(
    mock_get_marathon_steps,
    mock_get_pipeline_config,
    mock_stdout,
):
    mock_get_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'performance-check', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    mock_get_marathon_steps.return_value = [
        'hab.canary',
        'hab.main',
    ]
    actual = marathon_deployments_check('fake_service')
    assert actual is True


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
@patch('paasta_tools.paasta_cli.cmds.check.get_marathon_steps')
def test_marathon_deployments_deploy_but_not_marathon(
    mock_get_marathon_steps,
    mock_get_pipeline_config,
    mock_stdout,
):
    mock_get_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'performance-check', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
        {'instancename': 'hab.EXTRA', },
    ]
    mock_get_marathon_steps.return_value = [
        'hab.canary',
        'hab.main',
    ]
    actual = marathon_deployments_check('fake_service')
    assert actual is False
    assert 'EXTRA' in mock_stdout.getvalue()


@patch('sys.stdout', new_callable=StringIO)
@patch('paasta_tools.paasta_cli.cmds.check.get_pipeline_config')
@patch('paasta_tools.paasta_cli.cmds.check.get_marathon_steps')
def test_marathon_deployments_marathon_but_not_deploy(
    mock_get_marathon_steps,
    mock_get_pipeline_config,
    mock_stdout,
):
    mock_get_pipeline_config.return_value = [
        {'instancename': 'itest', },
        {'instancename': 'performance-check', },
        {'instancename': 'push-to-registry', },
        {'instancename': 'hab.canary', 'trigger_next_step_manually': True, },
        {'instancename': 'hab.main', },
    ]
    mock_get_marathon_steps.return_value = [
        'hab.canary',
        'hab.main',
        'hab.BOGUS',
    ]
    actual = marathon_deployments_check('fake_service')
    assert actual is False
    assert 'BOGUS' in mock_stdout.getvalue()


@patch('paasta_tools.paasta_cli.cmds.check.read_dockerfile_lines', autospec=True)
def test_docker_file_reads_from_yelpcorp_sad(
    mock_read_dockerfile_lines,
):
    mock_read_dockerfile_lines.return_value = [
        '# some comment',
        'FROM BAD',
    ]
    assert docker_file_reads_from_yelpcorp("unused") is False


@patch('paasta_tools.paasta_cli.cmds.check.read_dockerfile_lines', autospec=True)
def test_docker_file_reads_from_yelpcorp_happy(
    mock_read_dockerfile_lines,
):
    mock_read_dockerfile_lines.return_value = [
        '# some comment',
        'FROM docker-dev.yelpcorp.com/trusty_yelp',
    ]
    assert docker_file_reads_from_yelpcorp("unused") is True


def test_makefile_check():
    fake_makefile_path = 'UNUSED'
    fake_contents = "DOCKER_TAG ?= something\ntest:\n\tsomething\nitest:\n\tsomething"
    with contextlib.nested(
        patch(
            'paasta_tools.paasta_cli.cmds.check.get_file_contents',
            autospec=True,
            return_value=fake_contents
        ),
        patch(
            'paasta_tools.paasta_cli.cmds.check.makefile_has_a_tab',
        ),
        patch(
            'paasta_tools.paasta_cli.cmds.check.makefile_responds_to',
        ),
        patch(
            'paasta_tools.paasta_cli.cmds.check.makefile_has_docker_tag',
        ),
        patch(
            'paasta_tools.paasta_cli.cmds.check.is_file_in_dir',
            autospec=True,
            return_value=fake_makefile_path
        ),
    ) as (
        mock_get_file_contents,
        mock_makefile_has_a_tab,
        mock_makefile_responds_to,
        mock_makefile_has_docker_tag,
        mock_is_file_in_dir,
    ):
        makefile_check()
        assert mock_makefile_has_a_tab.call_count == 1
        calls = [call('test'), call('itest'), call('cook-image')]
        mock_makefile_responds_to.assert_has_calls(calls, any_order=True)
        assert mock_makefile_has_docker_tag.call_count == 1
