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
from __future__ import absolute_import
from __future__ import print_function

import errno
import json
import os


class Config(object):

    _default_profile = "default"

    DEFAULTS = {
        "debug": "false",
        "log_file": None,
        "log_level": "warning",
        "master": "localhost:5050",
        "max_workers": 5,
        "scheme": "http",
        "response_timeout": 5
    }

    cfg_name = ".mesos.json"

    _default_config_location = os.path.join(os.path.expanduser("~"), cfg_name)

    search_path = [os.path.join(x, cfg_name) for x in [
        ".",
        os.path.expanduser("~"),
        "/etc",
        "/usr/etc",
        "/usr/local/etc"
    ]]

    def __init__(self):
        self.__items = {self._default_profile: self.DEFAULTS}
        self["profile"] = self._default_profile

        self.load()

    def __str__(self):
        return json.dumps(self.__items, indent=4)

    def _config_file(self):
        for path in self.search_path:
            if os.path.exists(path):
                return path

        # default to creating a user level config file
        return self._default_config_location

    def _get_path(self):
        return os.environ.get(
            'MESOS_CLI_CONFIG', self._config_file())

    @property
    def _profile_key(self):
        return self.__items.get("profile", self._default_profile)

    @property
    def _profile(self):
        return self.__items.get(self._profile_key, {})

    def __getitem__(self, item):
        if item == "profile":
            return self.__items[item]
        return self._profile.get(item, self.DEFAULTS[item])

    def __setitem__(self, k, v):
        if k == "profile":
            self.__items[k] = v
            return

        profile = self._profile
        profile[k] = v
        self.__items[self._profile_key] = profile

    def load(self):
        try:
            with open(self._get_path(), 'rt') as f:
                try:
                    data = json.load(f)
                except ValueError as e:
                    raise ValueError(
                        'Invalid %s JSON: %s [%s]' %
                        (type(self).__name__, e.message, self._get_path())
                    )
                self.__items.update(data)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise

    def save(self):
        with open(self._get_path(), "wb") as f:
            f.write(str(self))

CURRENT = Config()
