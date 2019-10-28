import argparse
import os
import subprocess
import time
import xmlrpc.client

import colorlog

from clusterman.args import add_branch_or_tag_arg
from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.autoscaler.signals import setup_signals_environment
from clusterman.config import setup_config
from clusterman.util import setup_logging


class AutoscalerBootstrapException(Exception):
    pass


logger = colorlog.getLogger(__name__)
SUPERVISORD_ADDR = 'http://localhost:9001/RPC2'
SUPERVISORD_RUNNING_STATES = ('STARTING', 'RUNNING')


def wait_for_process(
    rpc: xmlrpc.client.ServerProxy,
    process_name: str,
    num_procs: int = 1,
    terminal_state: str = 'RUNNING',
) -> None:
    logger.info(f'waiting for {process_name} to start')
    while True:
        states = [
            rpc.supervisor.getProcessInfo(f'{process_name}:{process_name}_{i}')['statename']
            for i in range(num_procs)
        ]

        if any(state == 'FATAL' for state in states):
            raise AutoscalerBootstrapException(f'Process {process_name} could not start; aborting')
        elif all(state == terminal_state for state in states):
            break
        time.sleep(1)


class AutoscalerBootstrapBatch:

    def parse_args(self) -> None:
        parser = argparse.ArgumentParser()
        arg_group = parser.add_argument_group('AutoscalerMonitor options')
        add_cluster_arg(arg_group)
        add_pool_arg(arg_group)
        add_scheduler_arg(arg_group)
        add_env_config_path_arg(arg_group)
        add_cluster_config_directory_arg(arg_group)
        add_branch_or_tag_arg(arg_group)
        arg_group.add_argument(
            '--signal-root-directory',
            default='/code/signals',
            help='location of signal artifacts',
        )
        self.options = parser.parse_args()

    def configure(self) -> None:
        setup_config(self.options)
        self.logger = logger
        self.fetch_proc_count, self.run_proc_count = setup_signals_environment(
            self.options.pool,
            self.options.scheduler,
        )

    def run(self) -> None:
        env = os.environ.copy()
        args = env.get('CMAN_ARGS', '')
        if '--env-config-path' not in args:
            args += f' --env-config-path {self.options.env_config_path}'
        env['CMAN_ARGS'] = args
        supervisord_proc = subprocess.Popen(
            '/bin/bash -c "supervisord -c examples/supervisord.conf"',
            env=env,
            shell=True,
        )
        time.sleep(1)  # Give some time for the process to start
        with xmlrpc.client.ServerProxy(SUPERVISORD_ADDR) as rpc:
            skip_supervisord_cleanup = False
            try:
                wait_for_process(rpc, 'fetch_signals', num_procs=self.fetch_proc_count, terminal_state='EXITED')
                rpc.supervisor.startProcessGroup('run_signals')
                wait_for_process(rpc, 'run_signals', num_procs=self.run_proc_count)
                rpc.supervisor.startProcess('autoscaler')

                while (
                    rpc.supervisor.getProcessInfo('autoscaler')['statename'] in SUPERVISORD_RUNNING_STATES
                ):
                    time.sleep(5)
            except KeyboardInterrupt:
                # ctrl-c is propogated to the subprocess so don't do the shutdown call here
                skip_supervisord_cleanup = True
            finally:
                # supervisord won't clean up its child processes if we restart or an exception is thrown
                if not skip_supervisord_cleanup:
                    rpc.supervisor.shutdown()

        logger.info('Shutting down...')
        supervisord_proc.wait()


if __name__ == '__main__':
    setup_logging()
    batch = AutoscalerBootstrapBatch()
    batch.parse_args()
    batch.configure()
    batch.run()
