from __future__ import absolute_import
from __future__ import unicode_literals

from task_processing.interfaces.task_executor import TaskExecutor


def test_task_executor():
    class TaskExecutorImpl(TaskExecutor):
        def run(self, task_config):
            return task_config

        def kill(self, task_id):
            return True

    t = TaskExecutorImpl()
    assert t.run('foo') == 'foo'
    assert t.kill('mock')
