#!/usr/bin/env python

from paasta_tools.mesos_tools import fetch_mesos_stats


def get_mesos_status():
    """Prints information about the mesos cluster.
    """
    metrics = fetch_mesos_stats()
    print "Mesos:"
    print "    cpus: %d total => %d used, %d available" % \
        (
            metrics['master/cpus_total'],
            metrics['master/cpus_used'],
            metrics['master/cpus_total']-metrics['master/cpus_used']
        )
    print "    memory: %0.2f GB total => %0.2f GB used, %0.2f GB available" % \
        (
            metrics['master/mem_total']/1024,
            metrics['master/mem_used']/1024,
            (metrics['master/mem_total']-metrics['master/mem_used'])/1024
        )
    print "    tasks: %d running, %d staging, %d starting" % \
        (
            metrics['master/tasks_running'],
            metrics['master/tasks_staging'],
            metrics['master/tasks_starting'],
        )
    print "    slaves: %d active, %d inactive" % \
        (
            metrics['master/slaves_active'],
            metrics['master/slaves_inactive'],
        )


def main():
    get_mesos_status()


if __name__ == '__main__':
    main()
