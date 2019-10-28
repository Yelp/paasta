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
import mock

# This is a ridiculous hack; we need this patch to be applied before pytest imports any other conftest
# files, because if any other conftest file imports anything that imports anything that imports CACHE_TTL_SECONDS
# that value won't get overridden correctly.  Initially I used pytest_configure/pytest_unconfigure to patch
# the value, but even that didn't work because pytest imports all conftests before executing pytest_configure.
# So instead, we just ensure that CACHE_TTL_SECONDS gets patched as soon as pytest imports the first conftest.py
# (this one), and since we want it patched for the entirety of the test run, we don't need to call _ttl_patch.__exit__()

# suffice it to say, you shouldn't put anything else in this file

_ttl_patch = mock.patch('clusterman.aws.CACHE_TTL_SECONDS', -1)
_ttl_patch.__enter__()
