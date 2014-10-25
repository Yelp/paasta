#!/usr/bin/env python

import cleanup_marathon_orphaned_images


# These should be left running
mesos_deployed_old = {
    'Names': ['/mesos-deployed-old', ],
}
mesos_undeployed_young = {
    'Names': ['/mesos-undeployed-young', ],
}
nonmesos_undeployed_old = {
    'Names': ['/nonmesos-undeployed-old', ],
}
# These should be cleaned up
mesos_undeployed_old = {
    'Names': ['/mesos-undeployed-old', ],
}
running_images = [
    mesos_deployed_old,
    nonmesos_undeployed_old,
    mesos_undeployed_young,
    mesos_undeployed_old,
]


def test_get_mesos_images():
    assert nonmesos_undeployed_old in running_images
    actual = cleanup_marathon_orphaned_images.get_mesos_images(running_images)
    assert nonmesos_undeployed_old not in actual


def test_get_old_images():
    pass
