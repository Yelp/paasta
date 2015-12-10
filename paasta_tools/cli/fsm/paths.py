# Copyright 2015 Yelp Inc.
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

import os.path


class SrvPathBuilder(object):
    """Builds paths to files in different repos needed for service
    configuration.
    """

    def __init__(self, srvname, yelpsoa_config_root):
        self.srvname = srvname
        self.yelpsoa_config_root = yelpsoa_config_root

    @property
    def root_dir(self):
        return os.path.join(self.yelpsoa_config_root, self.srvname)

    def to_file(self, filename):
        """Return a filename under this service's directory"""
        return os.path.join(self.root_dir, filename)

    @property
    def service_yaml(self):
        """Path to the service.yaml for this service"""
        return os.path.join(
            self.root_dir, self.srvname + '.yaml')
