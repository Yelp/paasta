import sys
from subprocess import run


def get_pid(batch_name):
    output = run(f'ps -ef | egrep "python -m {batch_name}(\s+|$)"', shell=True, capture_output=True)

    return output.stdout.split()[1].decode()


def check_status(batch_name):  # pragma: no cover
    # status written by BatchRunningSentinelMixin
    status_file = f'/tmp/{batch_name.split(".")[-1]}.running'

    try:
        with open(status_file) as f:
            status_pid = f.read()
        batch_pid = get_pid(batch_name)
    except FileNotFoundError:
        print(f'{batch_name} has not finished initialization')
        sys.exit(1)

    assert status_pid == batch_pid
    print(f'{batch_name} completed initialization and is running at PID {status_pid}')


if __name__ == '__main__':
    check_status(sys.argv[1])
