# Copyright 2015-2016 Yelp Inc.
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
from behave import when

from paasta_tools import drain_lib


@when("a task has drained")
def when_a_task_has_drained(context):
    """Tell the TestDrainMethod to mark a task as safe to kill.
    Normal drain methods, like hacheck, require waiting for something to happen in the background. The bounce code can
    cause a task to go from up -> draining, but the draining->drained transition normally happens outside of Paasta.
    With TestDrainMethod, we can control the draining->drained transition to emulate that external code, and that's what
    this step does.
    """
    drain_lib.TestDrainMethod.mark_arbitrary_task_as_safe_to_kill()
