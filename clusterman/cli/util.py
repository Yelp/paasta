import argparse
import signal
import socket

import colorlog

logger = colorlog.getLogger(__name__)
TIMEOUT_TIME_SECONDS = 10


def timeout_wrapper(main):
    def wrapper(args: argparse.Namespace):

        # After 10s, prints a warning message if the command is running from the wrong place
        def timeout_handler(signum, frame):
            warning_string = 'This command is taking a long time to run; are you running from the right account?'
            if 'yelpcorp' in socket.getfqdn():
                warning_string += '\nHINT: try ssh\'ing to adhoc-prod or another box in the right account.'
            logger.warning(warning_string)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(TIMEOUT_TIME_SECONDS)

        main(args)
    return wrapper
