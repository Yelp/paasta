#!/usr/bin/env python

import chronos_tools
import sys

"""
    Ensure that the set of deployed chronos jobs
    matches the set expected.
"""

def cleanup_jobs(client, jobs, kill_existing_tasks=False):
    """ Maps a list of jobs to cleanup to a list of responses from the api (or exception objects)
    """
    def catch_exception(client, job):
        try:
            return client.delete_job(job)
        except Exception as e:
            """ We *have* to catch an Exception, because the client catches
                the more specific exception thrown by the http clients
                and rethrows an Exception -_-.

                The chronos api returns a 204 No Content when the delete is
                successful, and chronos-python only returns the body of the
                response from all http calls. So, if this is successful,
                then None will be returned.
                https://github.com/asher/chronos-python/pull/9

                we catch it here, so that the other deletes are completed.
            """
            return e

    return map(lambda job: (job, catch_exception(client, job)), jobs)

def main():
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)

    # get_chronos_jobs_for_cluster returns (service_name, job)
    expected_jobs = map(lambda x: x[-1], chronos_tools.get_chronos_jobs_for_cluster())
    running_jobs = map(lambda x: x['name'], client.list())
    jobs_to_remove = set(expected_jobs).intersection(set(running_jobs))

    responses = cleanup_jobs(client, jobs_to_remove, False)
    successes = filter(lambda resp: not isinstance(resp[-1], Exception), responses)
    failures = filter(lambda resp: isinstance(resp[-1], Exception), responses)
    print 'Successes\n:%s' % '\n'.join(map(lambda x: x[0], successes))

    # if there are any failures, print and exit appropriately
    if len(failures > 0):
        print 'Failures\n:%s' % '\n'.join(map(lambda x: x[0], failures))
        sys.exit(1)

if __name__ == "__main__":
    main()
