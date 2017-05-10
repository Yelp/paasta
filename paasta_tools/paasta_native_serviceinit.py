from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.frameworks.native_scheduler import MESOS_TASK_SPACER
from paasta_tools.mesos_tools import status_mesos_tasks_verbose
from paasta_tools.utils import calculate_tail_lines
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import paasta_print


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    if verbose > 0:
        tail_lines = calculate_tail_lines(verbose_level=verbose)
    else:
        tail_lines = 0

    # We have to add a spacer at the end to make sure we only return
    # things for service.main and not service.main_foo
    task_id_prefix = "%s%s" % (compose_job_id(service, instance), MESOS_TASK_SPACER)

    if command == 'status':
        paasta_print(status_mesos_tasks_verbose(
            job_id=task_id_prefix,
            get_short_task_id=lambda x: x,
            tail_lines=tail_lines,
        ))
