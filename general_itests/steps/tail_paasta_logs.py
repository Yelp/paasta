import contextlib
import time

from behave import when, then
import mock

from paasta_tools.paasta_cli.cmds import logs


@when(u'we tail paasta logs and let threads run')
def tail_paasta_logs_let_threads_be_threads(context):
    """This test lets tail_paasta_logs() fire off processes to do work. We
    verify that the work was done, basically irrespective of how it was done.
    """
    service = 'fake_service'
    context.levels = ['fake_level1', 'fake_level2']
    context.components = ['deploy', 'monitoring']
    context.clusters = ['fake_cluster1', 'fake_cluster2']
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.logs.determine_scribereader_envs', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.scribe_tail', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.log', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.logs.print_log', autospec=True),
    ) as (
        context.determine_scribereader_envs_patch,
        scribe_tail_patch,
        log_patch,
        context.print_log_patch,
    ):
        context.determine_scribereader_envs_patch.return_value = ['env1', 'env2']

        def scribe_tail_side_effect(
            scribe_env,
            stream_name,
            service,
            levels,
            components,
            clusters,
            queue,
            filter_fn,
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

        logs.tail_paasta_logs(service, context.levels, context.components, context.clusters)


@then(u'one message is displayed from each scribe env')
def step_impl(context):
    for cluster in context.clusters:
        context.determine_scribereader_envs_patch.assert_any_call(context.components, cluster)
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
