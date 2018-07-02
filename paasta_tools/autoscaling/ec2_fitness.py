"""
Provides functions for evaluating the 'fitness'
of ec2 instances. This module really just provides
functions for deciding which instance is best to be
killed by the autoscaler.
"""


def sort_by_system_instance_health(instances):
    return sorted(
        instances,
        key=lambda i: (
            i.instance_status['SystemStatus']['Status'] != 'ok' or
            i.instance_status['InstanceStatus']['Status'] != 'ok'
        ),
    )


def sort_by_upcoming_events(instances):
    return sorted(
        instances,
        key=lambda i: len(i.instance_status.get('Events', [])),
    )


def sort_by_total_tasks(instances):
    return sorted(instances, key=lambda i: i.task_counts.count, reverse=True)


def sort_by_running_batch_count(instances):
    return sorted(instances, key=lambda i: i.task_counts.batch_count, reverse=True)


def sort_by_ec2_fitness(instances):
    """
    Sort a list according to their fitness. This will return the list of instances
    in order of 'fitness': that is, that which is least desirable to kill is first in
    the list.

    Fitness is judged according to the following rules:
        - any instance considered to have a non 'ok' system or instance status is always
          considered to be least healthy
        - next, instances are ranked according to whether they have events planned. an event
          planned marks against your fitness.
        - next, instances are sorted according to the number of chronos tasks running on them.
          we can't drain chronos tasks, so make an effort to avoid disrupting them.
        - finally, instances are sorted according to the number of total tasks they have. those with
          the hightest total task are considered fittest, because it's painful to drain them.
    """
    return sort_by_system_instance_health(
        sort_by_upcoming_events(
            sort_by_running_batch_count(
                sort_by_total_tasks(instances),
            ),
        ),
    )
