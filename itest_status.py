# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
