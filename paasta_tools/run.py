import errno
import os
import shlex
import signal
import threading
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT
from types import FrameType
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union

from paasta_tools.log_utils import _log
from paasta_tools.log_utils import ANY_CLUSTER
from paasta_tools.log_utils import ANY_INSTANCE
from paasta_tools.log_utils import DEFAULT_LOGLEVEL
from paasta_tools.text_utils import paasta_print


def _timeout(process: Popen) -> None:
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


def _run(
    command: Union[str, List[str]],
    env: Mapping[str, str]=os.environ,
    timeout: float=None,
    log: bool=False,
    stream: bool=False,
    stdin: Any=None,
    stdin_interrupt: bool=False,
    popen_kwargs: Dict={},
    **kwargs: Any,
) -> Tuple[int, str]:
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timeout
        seconds.
    :param log: If True, the _log will be handled by _run. If set, it is mandatory
        to pass at least a :service: and a :component: parameter. Optionally you
        can pass :cluster:, :instance: and :loglevel: parameters for logging.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    output = []
    if log:
        service = kwargs['service']
        component = kwargs['component']
        cluster = kwargs.get('cluster', ANY_CLUSTER)
        instance = kwargs.get('instance', ANY_INSTANCE)
        loglevel = kwargs.get('loglevel', DEFAULT_LOGLEVEL)
    try:
        if not isinstance(command, list):
            command = shlex.split(command)
        popen_kwargs['stdout'] = PIPE
        popen_kwargs['stderr'] = STDOUT
        popen_kwargs['stdin'] = stdin
        popen_kwargs['env'] = env
        process = Popen(command, **popen_kwargs)

        if stdin_interrupt:
            def signal_handler(signum: int, frame: FrameType) -> None:
                process.stdin.write("\n")
                process.stdin.flush()
                process.wait()
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, [process])
            proctimer.start()
        for linestr in iter(process.stdout.readline, b''):
            line = linestr.decode('utf-8')
            # additional indentation is for the paasta status command only
            if stream:
                if ('paasta_serviceinit status' in command):
                    if 'instance: ' in line:
                        paasta_print('  ' + line.rstrip('\n'))
                    else:
                        paasta_print('    ' + line.rstrip('\n'))
                else:
                    paasta_print(line.rstrip('\n'))
            else:
                output.append(line.rstrip('\n'))

            if log:
                _log(
                    service=service,
                    line=line.rstrip('\n'),
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
        # when finished, get the exit code
        process.wait()
        returncode = process.returncode
    except OSError as e:
        if log:
            _log(
                service=service,
                line=e.strerror.rstrip('\n'),
                component=component,
                level=loglevel,
                cluster=cluster,
                instance=instance,
            )
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
