#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import errno
import json
import os.path
import random
import shlex
import sys
import threading
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT

import yaml

from paasta_tools.utils import paasta_print


def _timeout(process):
    """Helper function for _run. It terminates the process.
    Doesn't raise OSError, if we try to terminate a non-existing
    process as there can be a very small window between poll() and kill()
    """
    if process.poll() is None:
        try:
            # sending SIGKILL to the process
            process.kill()
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise


def cmd(command):
    stream = False
    timeout = 60
    output = []
    try:
        process = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT, stdin=None)
        process.name = command
        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, (process,))
            proctimer.start()
        for line in iter(process.stdout.readline, ''):
            if stream:
                paasta_print(line.rstrip('\n'))
            output.append(line.rstrip('\n'))
        # when finished, get the exit code
        returncode = process.wait()
    except OSError as e:
        output.append(e.strerror.rstrip('\n'))
        returncode = e.errno
    except (KeyboardInterrupt, SystemExit):
        # need to clean up the timing thread here
        if timeout:
            proctimer.cancel()
        raise
    else:
        # Stop the timer
        if timeout:
            proctimer.cancel()
    if returncode == -9:
        output.append("Command '%s' timed out (longer than %ss)" % (command, timeout))
    return returncode, '\n'.join(output)


def abort(message):
    paasta_print(message)
    sys.exit(1)


def condquit(rc, message):
    if rc != 0:
        paasta_print(message)
        sys.exit(rc)


def docker_env_to_dict(environment_array):
    environment = {}
    for kv in environment_array:
        k, v = kv.split('=', 1)
        environment[k] = v
    return environment


def get_proxy_port(service_name, instance_name):
    smartstack_yaml = "/nail/etc/services/%s/smartstack.yaml" % service_name
    proxy_port = None
    if os.path.exists(smartstack_yaml):
        with open(smartstack_yaml, 'r') as stream:
            data = yaml.safe_load(stream)
            if instance_name in data:
                proxy_port = data[instance_name].get('proxy_port', None)
    return proxy_port


def get_last_killed(drained_apps, service, instance):
    """look "back" in drained_apps, find at what time
    the given (service, instance) was last killed"""
    last_killed_t = -1000
    for drained_app in reversed(drained_apps):
        dt, dservice, dinstance = drained_app
        if dservice == service and dinstance == instance:
            last_killed_t = dt
            break
    return last_killed_t


def has_all_paasta_env(environment):
    for k in ('PAASTA_SERVICE', 'PAASTA_INSTANCE', 'MARATHON_PORT'):
        if k not in environment:
            return False

    return True


def main():
    rc, output = cmd('sudo docker ps -q')
    condquit(rc, 'docker ps')
    lines = output.split("\n")

    if len(lines) == 0:
        abort("no containers running")

    running_container_ids = []

    for line in lines:
        if len(line) != 12:
            abort("%s doesn't look like a container ID" % line)
        running_container_ids.append(line.rstrip())

    random.shuffle(running_container_ids)

    drained_apps = []  # ( t_killed, service, instance )
    smartstack_grace_sleep = 10
    between_containers_grace_sleep = 10
    min_kill_interval = 60  # minimum time to wait between same service.instance kills
    hadown_expire_in_seconds = 120
    t = 0

    for container_id in running_container_ids:
        rc, output = cmd("sudo docker inspect %s" % container_id)
        condquit(rc, "docker inspect %s" % container_id)
        docker_inspect_data = json.loads(output)
        environment = docker_env_to_dict(docker_inspect_data[0]['Config']['Env'])
        if 'CHRONOS_JOB_NAME' in environment:
            paasta_print(
                "# WARNING! %s is a chronos job (%s), skipping" % (container_id, environment['CHRONOS_JOB_NAME']))
            paasta_print()
            continue
        if not has_all_paasta_env(environment):
            paasta_print("# WARNING: %s is not a paasta container, skipping)" % (container_id))
            continue
        service = environment['PAASTA_SERVICE']
        instance = environment['PAASTA_INSTANCE']
        paasta_print("# %s.%s" % (service, instance))
        marathon_port = int(environment['MARATHON_PORT'])
        proxy_port = get_proxy_port(service, instance)
        paasta_print("# %s,%s,%s,%s,%s" % (container_id, service, instance, proxy_port, marathon_port))
        paasta_print("sudo hadown -P %s -e $((`date +'%%s'`+%s)) %s.%s" % (
            marathon_port, hadown_expire_in_seconds, service, instance
        ))
        paasta_print("sleep %s" % smartstack_grace_sleep)
        t += smartstack_grace_sleep
        paasta_print("sudo docker kill %s" % container_id)
        paasta_print("sudo haup -P %s %s.%s" % (marathon_port, service, instance))
        last_killed_t = get_last_killed(drained_apps, service, instance)
        drained_apps.append((t, service, instance))
        # print "t:%s last_killed_t:%s" % (t, last_killed_t)
        sleep_amount = between_containers_grace_sleep
        if (t - last_killed_t) < min_kill_interval:
            sleep_amount = min_kill_interval - (t - last_killed_t) + between_containers_grace_sleep
        paasta_print("sleep %s" % sleep_amount)
        t += sleep_amount
        paasta_print()


if __name__ == "__main__":
    main()
