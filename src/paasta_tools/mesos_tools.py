import os

from paasta_tools.marathon_tools import compose_job_id
# mesos.cli.master reads its config file at *import* time, so we must have
# this environment variable set and ready to go at that time so we can
# read in the config for zookeeper, etc
os.environ['MESOS_CLI_CONFIG'] = '/nail/etc/mesos-cli.json'
from mesos.cli import master


def get_mesos_tasks_from_master(job_id):
    return master.CURRENT.tasks(fltr=job_id)


def get_mesos_tasks_for_service(service, instance):
    job_id = compose_job_id(service, instance)
    return get_mesos_tasks_from_master(job_id)


def get_running_mesos_tasks_for_service(service, instance):
    all_tasks = get_mesos_tasks_for_service(service, instance)
    return [task for task in all_tasks if task['state'] == 'TASK_RUNNING']
