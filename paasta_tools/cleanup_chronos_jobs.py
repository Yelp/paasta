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
            return client.delete(job)
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

def jobs_to_delete(expected_jobs, actual_jobs):
    return list(set(actual_jobs).difference(set(expected_jobs)))

def format_list_output(title, job_names):
    return '%s\n  %s' % (title, '\n  '.join(job_names))

def running_job_names(client):
    return [job['name'] for job in client.list()]

def expected_job_names(service_job_pairs):
    """ Expects a list of pairs in the form (service_name, job_name)
    and returns the list of pairs mapped to the job name of each pair.
    """
    return [job[-1] for job in service_job_pairs]

def main():
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)

    # get_chronos_jobs_for_cluster returns (service_name, job)
    expected_jobs = expected_job_names(chronos_tools.get_chronos_jobs_for_cluster())
    running_jobs = running_job_names(client)

    to_delete = jobs_to_delete(expected_jobs, running_jobs)
    responses = cleanup_jobs(client, to_delete, False)

    successes = [resp for resp in responses if not isinstance(resp[-1], Exception)]
    failures = [resp for resp in responses if isinstance(resp[-1], Exception)]

    if len(to_delete) == 0:
        print 'No Chronos Jobs to remove'
    else:
        if len(successes) > 0:
            print format_list_output("Successfully Removed:", [job[0] for job in successes])

        # if there are any failures, print and exit appropriately
        if len(failures) > 0:
            print format_list_output("Failed to Delete:", [job[0] for job in failures])
            sys.exit(1)


if __name__ == "__main__":
    main()
