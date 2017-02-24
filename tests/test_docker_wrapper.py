# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket

import mock
import pytest

from paasta_tools import docker_wrapper


class TestParseEnvArgs(object):
    def test_empty(self):
        env = docker_wrapper.parse_env_args(['docker'])
        assert env == {}

    @pytest.mark.parametrize('args', [
        ['docker', '-e', 'key=value'],
        ['docker', '-e=key=value'],
        ['docker', '-te', 'key=value'],  # -e can be combined with other short args
        ['docker', '-et', 'key=value'],  # -t takes no additional parameters so docker allows this order
    ])
    def test_short(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {'key': 'value'}

    @pytest.mark.parametrize('args', [
        ['docker', '--env', 'key=value'],
        ['docker', '--env=key=value'],
    ])
    def test_long(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {'key': 'value'}

    @pytest.mark.parametrize('args', [
        ['docker', '--eep', 'key=value'],  # ignore -e in double dashes unless its --env
        ['docker', '-a', 'key=value'],  # only -e should matter
        ['docker', '-'],  # just don't crash
    ])
    def test_short_invalid(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {}

    def test_mixed_short_long(self):
        env = docker_wrapper.parse_env_args(['docker', '-e', 'foo=bar', '--env=apple=banana', '--env', 'c=d'])
        assert env == {'foo': 'bar', 'apple': 'banana', 'c': 'd'}

    def test_multiple_equals(self):
        env = docker_wrapper.parse_env_args(['docker', '-e', 'foo=bar=cat'])
        assert env == {'foo': 'bar=cat'}

    def test_dupe(self):
        env = docker_wrapper.parse_env_args(['docker', '-e', 'foo=bar', '-e', 'foo=cat'])
        assert env == {'foo': 'cat'}

    def test_empty_value(self):
        env = docker_wrapper.parse_env_args(['docker', '-e', 'foo=', '--env=bar='])
        assert env == {'foo': '', 'bar': ''}


class TestAlreadyHasHostname(object):
    def test_empty(self):
        assert docker_wrapper.already_has_hostname(['docker']) is False

    @pytest.mark.parametrize('args', [
        ['docker', '-h'],
        ['docker', '-th'],
        ['docker', '-ht'],
    ])
    def test_short(self, args):
        assert docker_wrapper.already_has_hostname(args) is True

    @pytest.mark.parametrize('args', [
        ['docker', '-e=foo=hhh'],
        ['docker', '--hhh'],
        ['docker', '-'],
    ])
    def test_short_invalid(self, args):
        assert docker_wrapper.already_has_hostname(args) is False

    @pytest.mark.parametrize('args', [
        ['docker', '--hostname', 'foo'],
        ['docker', '--hostname=foo'],
    ])
    def test_long(self, args):
        assert docker_wrapper.already_has_hostname(args) is True


class TestGenerateHostname(object):
    def test_simple(self):
        hostname = docker_wrapper.generate_hostname(
            'first.part.matters',
            'what.only.matters.is.lastpart')
        assert hostname == 'first-lastpart'

    def test_truncate(self):
        hostname = docker_wrapper.generate_hostname(
            'reallllllllllllllylooooooooooooooong',
            'reallyreallylongidsssssssssssssssssssssssss')
        assert hostname == 'reallllllllllllllylooooooooooooooong-reallyreallylongidssssssss'
        assert len(hostname) == 63

    def test_symbols(self):
        hostname = docker_wrapper.generate_hostname(
            'first.part.matters',
            'chronos:can_do!s0me weird-stuff')
        assert hostname == 'first-chronos-can-do-s0me-weird-stuff'


@pytest.mark.parametrize('input_args,expected_args', [
    (  # do not add it for non-run commands
        ['docker', 'ps'],
        ['docker', 'ps']),
    (  # add it after the first run arg
        ['docker', 'run', 'run'],
        ['docker', 'run', '--hostname=myhostname', 'run']),
    (  # ignore args before run
        ['docker', '--host', '/fake.sock', 'run', '-t'],
        ['docker', '--host', '/fake.sock', 'run', '--hostname=myhostname', '-t']
    )
])
def test_add_hostname(input_args, expected_args):
    args = docker_wrapper.add_hostname(input_args, 'myhostname')
    assert args == expected_args


class TestMain(object):
    @pytest.yield_fixture(autouse=True)
    def mock_execlp(self):
        # always patch execlp so we don't actually exec
        with mock.patch.object(docker_wrapper.os, 'execlp') as mock_execlp:
            yield mock_execlp

    def test_marathon(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=MESOS_TASK_ID=paasta--canary.main.git332d4a22.config458863b1.0126a188-f944-11e6-bdfb-12abac3adf8c',
        ]
        with mock.patch.object(socket, 'getfqdn', return_value='myhostname'):
            docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--hostname=myhostname-0126a188-f944-11e6-bdfb-12abac3adf8c',
            '--env=MESOS_TASK_ID=paasta--canary.main.git332d4a22.config458863b1.0126a188-f944-11e6-bdfb-12abac3adf8c')]

    def test_chronos(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=mesos_task_id=ct:1487804100000:0:thirdparty_feeds thirdparty_feeds-cloudflare-all:',
        ]
        with mock.patch.object(socket, 'getfqdn', return_value='myhostname'):
            docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--hostname=myhostname-ct-1487804100000-0-thirdparty-feeds-thirdparty-feeds',
            '--env=mesos_task_id=ct:1487804100000:0:thirdparty_feeds thirdparty_feeds-cloudflare-all:')]

    def test_env_not_present(self, mock_execlp):
        argv = [
            'docker',
            'run',
            'foobar',
        ]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            'foobar')]

    def test_already_has_hostname(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
            '--hostname=somehostname',
        ]
        with mock.patch.object(socket, 'getfqdn', return_value='myhostname'):
            docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
            '--hostname=somehostname')]

    def test_not_run(self, mock_execlp):
        argv = [
            'docker',
            'ps',
            '--env=MESOS_TASK_ID=my-mesos-task-id',
        ]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'ps',
            '--env=MESOS_TASK_ID=my-mesos-task-id')]
