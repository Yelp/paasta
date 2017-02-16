# -*- coding: utf-8 -*-
import mock
import sys

import pytest

from paasta_tools import docker_hostname_wrapper

class TestParseEnvArgs(object):
    def test_empty(self):
        env = docker_hostname_wrapper.parse_env_args(['docker'])
        assert env == {}

    @pytest.mark.parametrize('args', [
        ['docker', '-e', 'key=value'],
        ['docker', '-e=key=value'],
        ['docker', '-te', 'key=value'],  # -e can be combined with other short args
        ['docker', '-et', 'key=value'],  # -t takes no additional parameters so docker allows this order
    ])
    def test_short(self, args):
        env = docker_hostname_wrapper.parse_env_args(args)
        assert env == {'key': 'value'}

    @pytest.mark.parametrize('args', [
        ['docker', '--env', 'key=value'],
        ['docker', '--env=key=value'],
    ])
    def test_long(self, args):
        env = docker_hostname_wrapper.parse_env_args(args)
        assert env == {'key': 'value'}

    @pytest.mark.parametrize('args', [
        ['docker', '--eep', 'key=value'],  # ignore -e in double dashes unless its --env
        ['docker', '-a', 'key=value'],  # only -e should matter
        ['docker', '-'], # just don't crash
    ])
    def test_short_invalid(self, args):
        env = docker_hostname_wrapper.parse_env_args(args)
        assert env == {}

    def test_mixed_short_long(self):
        env = docker_hostname_wrapper.parse_env_args(['docker', '-e', 'foo=bar', '--env=apple=banana', '--env', 'c=d'])
        assert env == {'foo': 'bar', 'apple': 'banana', 'c': 'd'}

    def test_multiple_equals(self):
        env = docker_hostname_wrapper.parse_env_args(['docker', '-e', 'foo=bar=cat'])
        assert env == {'foo': 'bar=cat'}

    def test_dupe(self):
        env = docker_hostname_wrapper.parse_env_args(['docker', '-e', 'foo=bar', '-e', 'foo=cat'])
        assert env == {'foo': 'cat'}

    def test_empty_value(self):
        env = docker_hostname_wrapper.parse_env_args(['docker', '-e', 'foo=', '--env=bar='])
        assert env == {'foo': '', 'bar': ''}


class TestAlreadyHasHostname(object):
    def test_empty(self):
        assert docker_hostname_wrapper.already_has_hostname(['docker']) is False

    @pytest.mark.parametrize('args', [
        ['docker', '-h'],
        ['docker', '-th'],
        ['docker', '-ht'],
    ])
    def test_short(self, args):
        assert docker_hostname_wrapper.already_has_hostname(args) is True

    @pytest.mark.parametrize('args', [
        ['docker', '-e=foo=hhh'],
        ['docker', '--hhh'],
        ['docker', '-'],
    ])
    def test_short_invalid(self, args):
        assert docker_hostname_wrapper.already_has_hostname(args) is False

    @pytest.mark.parametrize('args', [
        ['docker', '--hostname', 'foo'],
        ['docker', '--hostname=foo'],
    ])
    def test_long(self, args):
        assert docker_hostname_wrapper.already_has_hostname(args) is True


class TestGenerateHostname(object):
    def test_simple(self):
        hostname = docker_hostname_wrapper.generate_hostname('first.part.matters', 'what.only.matters.is.lastpart')
        assert hostname == 'first-lastpart'

    def test_truncate(self):
        hostname = docker_hostname_wrapper.generate_hostname('reallllllllllllllylooooooooooooooong', 'reallyreallylongidsssssssssssssssssssssssss')
        assert hostname == 'reallllllllllllllylooooooooooooooong-reallyreallylongidsssssssss'
        assert len(hostname) == 64


@pytest.mark.parametrize('input_args,expected_args', [
    (['docker', 'ps'], ['docker', 'ps']),  # do not add it for non-run commands
    (['docker', 'run', 'run'], ['docker', 'run', '--hostname=myhostname', 'run']),  # add it after the first run arg
    (['docker', '--host', '/fake.sock', 'run', '-t'], ['docker', '--host', '/fake.sock', 'run', '--hostname=myhostname', '-t'])  # ignore args before run
])
def test_add_hostname(input_args, expected_args):
    args = list(input_args) # add_hostname modifies the parameter directly
    docker_hostname_wrapper.add_hostname(args, 'myhostname')
    assert args == expected_args


class TestMain(object):
    @pytest.fixture(autouse=True)
    def mock_execlp(self):
        # always patch execlp so we don't actually exec
        with mock.patch.object(docker_hostname_wrapper.os, 'execlp') as mock_execlp:
            yield mock_execlp

    def test_simple(self, mock_execlp):
        with mock.patch.object(sys, 'argv', [
            'docker',
            'run',
            '--env=MARATHON_HOST=mymarathon',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
        ]):
            docker_hostname_wrapper.main()
            assert mock_execlp.mock_calls == [mock.call(
                'docker',
                'docker',
                'run',
                '--hostname=mymarathon-my-mesos-task-id',
                '--env=MARATHON_HOST=mymarathon',
                '--env=MESOS_TASK_ID=my-mesos-task-id')]

    def test_env_not_present(self, mock_execlp):
        with mock.patch.object(sys, 'argv', [
            'docker',
            'run',
            '--env=MARATHON_HOST=mymarathon',
        ]):
            docker_hostname_wrapper.main()
            assert mock_execlp.mock_calls == [mock.call(
                'docker',
                'docker',
                'run',
                '--env=MARATHON_HOST=mymarathon')]

    def test_already_has_hostname(self, mock_execlp):
        with mock.patch.object(sys, 'argv', [
            'docker',
            'run',
            '--env=MARATHON_HOST=mymarathon',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
            '--hostname=somehostname',
        ]):
            docker_hostname_wrapper.main()
            assert mock_execlp.mock_calls == [mock.call(
                'docker',
                'docker',
                'run',
                '--env=MARATHON_HOST=mymarathon',
                '--env=MESOS_TASK_ID=my-mesos-task-id',
                '--hostname=somehostname')]

    def test_not_run(self, mock_execlp):
        with mock.patch.object(sys, 'argv', [
            'docker',
            'ps',
            '--env=MARATHON_HOST=mymarathon',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
        ]):
            docker_hostname_wrapper.main()
            assert mock_execlp.mock_calls == [mock.call(
                'docker',
                'docker',
                'ps',
                '--env=MARATHON_HOST=mymarathon',
                '--env=MESOS_TASK_ID=my-mesos-task-id')]
