#!/usr/bin/env python
from contextlib import contextmanager
import fcntl
import logging
import os
import marathon_tools


log = logging.getLogger(__name__)


@contextmanager
def bounce_lock(name):
    """Acquire a bounce lockfile for the name given. The name should generally
    be the service instance being bounced."""
    lockfile = '/var/lock/%s.lock' % name
    fd = open(lockfile, 'w')
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        raise IOError("Service %s is already being bounced!" % name)
    try:
        yield
    finally:
        fd.close()
        os.remove(lockfile)


def brutal_bounce(old_ids, new_config, client):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns a new app with the new_config via a
    Marathon client."""
    with bounce_lock(marathon_tools.remove_iteration_from_job_id(new_config['id'])):
        for app in old_ids:
            log.info("Killing %s", app)
            client.delete_app(app)
        log.info("Creating %s", new_config['id'])
        client.create_app(**new_config)