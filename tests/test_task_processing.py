# Copyright 2015-2017 Yelp Inc.
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
# We just want to test that task_processing is available in the virtualenv
from __future__ import absolute_import
from __future__ import unicode_literals


def test_import():
    from task_processing.task_processor import TaskProcessor
    tp = TaskProcessor()
    tp.load_plugin('task_processing.plugins.mesos')
