# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import copy
import errno
import json


DEFAULTS = {
    "debug": "false",
    "log_file": None,
    "log_level": "warning",
    "master": "localhost:5050",
    "max_workers": 5,
    "scheme": "http",
    "response_timeout": 5,
}


def load_mesos_config(config_path, profile="default"):
    on_disk = {}

    try:
        with open(config_path, 'rt') as f:
            on_disk = json.load(f)[profile]
    except ValueError as e:
        raise ValueError(
            'Invalid JSON: {} in {}'.format(str(e), config_path),
        )
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise

    config = copy.deepcopy(DEFAULTS)
    config.update(on_disk)
    return config
