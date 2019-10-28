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
import warnings

import pytest
import staticconf.testing

try:
    import yelp_batch  # noqa
except ImportError:
    warnings.warn('Could not import yelp_batch, are you in a Yelp-y environment?  Skipping these tests')
    collect_ignore_glob = ['*']


@pytest.fixture(autouse=True)
def mock_setup_config_directory():
    with staticconf.testing.PatchConfiguration(
        {'cluster_config_directory': '/a/fake/directory/'}
    ):
        yield
