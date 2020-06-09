from paasta_tools.frameworks.native_scheduler import MESOS_TASK_SPACER
from paasta_tools.mesos_tools import status_mesos_tasks_verbose
from paasta_tools.utils import calculate_tail_lines
from paasta_tools.utils import compose_job_id


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    tail_lines = calculate_tail_lines(verbose_level=verbose)

    # We have to add a spacer at the end to make sure we only return
    # things for service.main and not service.main_foo
    task_id_prefix = "{}{}".format(compose_job_id(service, instance), MESOS_TASK_SPACER)

    if command == "status":
        print(
            status_mesos_tasks_verbose(
                job_id=task_id_prefix,
                get_short_task_id=lambda x: x,
                tail_lines=tail_lines,
            )
        )
