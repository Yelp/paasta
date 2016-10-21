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
import contextlib
import time

import mock
from behave import then
from behave import when

from paasta_tools.cli.cmds import logs


@when(u'we tail paasta logs and let threads run')
def tail_paasta_logs_let_threads_be_threads(context):
    """This test lets tail_paasta_logs() fire off processes to do work. We
    verify that the work was done, basically irrespective of how it was done.
    """
    service = 'fake_service'
    context.levels = ['fake_level1', 'fake_level2']
    context.components = ['deploy', 'monitoring']
    context.clusters = ['fake_cluster1', 'fake_cluster2']
    context.instances = ['fake_instance']
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.ScribeLogReader.scribe_tail', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.print_log', autospec=True),
        mock.patch('paasta_tools.cli.cmds.logs.scribereader'),
    ) as (
        context.determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        context.print_log_patch,
        mock_scribereader,
    ):
        context.determine_scribereader_envs_patch.return_value = ['env1', 'env2']

        def scribe_tail_side_effect(
            self,
            scribe_env,
            stream_name,
            service,
            levels,
            components,
            clusters,
            instances,
            queue,
            filter_fn,
            parse_fn=None
        ):
            # The print here is just for debugging
            print 'fake log line added for %s' % scribe_env
            queue.put('fake log line added for %s' % scribe_env)
            # This sleep() was the straw that broke the camel's back
            # and forced me to move this test into the integration
            # suite. The test is flaky without the sleep, and the
            # sleep make it a lousy unit test.
            time.sleep(0.05)
        scribe_tail_patch.side_effect = scribe_tail_side_effect

        context.scribe_log_reader = logs.ScribeLogReader(cluster_map={'env1': 'env1', 'env2': 'env2'})
        context.scribe_log_reader.tail_logs(
            service, context.levels, context.components, context.clusters, context.instances)


@then(u'one message is displayed from each scribe env')
def step_impl(context):
    for cluster in context.clusters:
        context.determine_scribereader_envs_patch.assert_any_call(context.scribe_log_reader, context.components,
                                                                  cluster)
    # NOTE: Assertions about scribe_tail_patch break under multiprocessing.
    # We think this is because the patched scribe_tail's attributes
    # (call_count, call_args, etc.) don't get updated here in the main
    # thread where we can inspect them. (The patched-in code does run,
    # however, since it prints debugging messages.)
    #
    # Instead, we'll rely on what we can see, which is the result of the
    # thread's work deposited in the shared queue.
    assert context.print_log_patch.call_count == 2
    context.print_log_patch.assert_any_call('fake log line added for env1', context.levels, False)
    context.print_log_patch.assert_any_call('fake log line added for env2', context.levels, False)
