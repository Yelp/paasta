# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import socket
from contextlib import contextmanager

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


class TestCanAddHostname(object):
    def test_empty(self):
        assert docker_wrapper.can_add_hostname(['docker']) is True

    @pytest.mark.parametrize('args', [
        ['docker', '-h'],
        ['docker', '-th'],
        ['docker', '-ht'],
    ])
    def test_short(self, args):
        assert docker_wrapper.can_add_hostname(args) is False

    @pytest.mark.parametrize('args', [
        ['docker', '-e=foo=hhh'],
        ['docker', '--hhh'],
        ['docker', '-'],
        ['docker', '--net', 'bridged'],
        ['docker', '--net'],
        ['docker', '--network', 'bridged'],
        ['docker', '--network'],
    ])
    def test_short_invalid(self, args):
        assert docker_wrapper.can_add_hostname(args) is True

    @pytest.mark.parametrize('args', [
        ['docker', '--hostname', 'foo'],
        ['docker', '--hostname=foo'],
        ['docker', '--net=host'],
        ['docker', '--net', 'host'],
        ['docker', '--network=host'],
        ['docker', '--network', 'host'],
        ['docker', '--hostname=foo', '--net=host'],
    ])
    def test_long(self, args):
        assert docker_wrapper.can_add_hostname(args) is False


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
def test_add_argument(input_args, expected_args):
    args = docker_wrapper.add_argument(input_args, '--hostname=myhostname')
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

    def test_numa_string_value(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE="true"',
        ]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE="true"')]

    def test_numa_bogus_node(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=True',
        ]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=True')]

    def test_numa_unsupported(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=1.5',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=False,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
        ]

    def test_marathon_bogus_value(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=blah',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=blah',
        )]

    def test_numa_OK(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=1.5',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=1.5',
        )]

    def test_cpuset_already_set(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--cpuset-cpus=0,2',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=1.5',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-cpus=0,2',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=1.5',
        )]

    def test_numa_req_bogus_mem_value(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=overflowwwww',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=overflowwwww',
        )]

    def test_numa_req_more_mem_than_available(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=40000.0',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=40000.0',
        )]

    def test_numa_req_less_mem_than_available(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=20000.0',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
            '--env=MARATHON_APP_RESOURCE_MEM=20000.0',
        )]

    def test_numa_req_exact_amount_of_cores(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=2',
        )]

    def test_numa_req_too_many_cores(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=3.0',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
            '--env=MARATHON_APP_RESOURCE_CPUS=3.0',
        )]

    def test_numa_enabled_no_marathon(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--cpuset-mems=1',
            '--cpuset-cpus=1,3',
            '--env=PIN_TO_NUMA_NODE=1',
        )]

    def test_numa_wrong_cpu(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=2',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 1',
                'physical id    : 0',
                'physical id    : 1',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=2',
        )]

    def test_numa_single_cpu(self, mock_execlp):
        argv = [
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
        ]
        with self._patch_docker_wrapper_dependencies(
            is_numa_enabled=True,
            node_mem=32000,
            cpu_info=[
                'physical id    : 0',
                'physical id    : 0',
            ],
        ):
            docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [mock.call(
            'docker',
            'docker',
            'run',
            '--env=PIN_TO_NUMA_NODE=1',
        )]

    @contextmanager
    def _patch_docker_wrapper_dependencies(self, is_numa_enabled, cpu_info, node_mem):
        m = mock.mock_open()
        m.return_value.__iter__.return_value = cpu_info
        with mock.patch.object(
            docker_wrapper,
            'is_numa_enabled',
            return_value=is_numa_enabled
        ), mock.patch.object(docker_wrapper,
                             'get_numa_memsize',
                             return_value=node_mem
                             ), mock.patch.object(docker_wrapper, 'open', new=m):
            yield
