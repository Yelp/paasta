#!/usr/bin/env python

import calendar
import datetime

import cleanup_marathon_orphaned_images


fake_now = datetime.datetime(2014, 10, 24)
young_offset = datetime.timedelta(minutes=59)
old_offset = datetime.timedelta(minutes=61)
mesos_deployed_old = {
    'Created': calendar.timegm((fake_now + old_offset).timetuple()),
    'Names': ['/mesos-deployed-old', ],
}
mesos_undeployed_old = {
    'Created': calendar.timegm((fake_now + old_offset).timetuple()),
    'Names': ['/mesos-undeployed-old', ],
}
mesos_undeployed_young = {
    'Created': calendar.timegm((fake_now + young_offset).timetuple()),
    'Names': ['/mesos-undeployed-young', ],
}
nonmesos_undeployed_old = {
    'Created': calendar.timegm((fake_now + old_offset).timetuple()),
    'Names': ['/nonmesos-undeployed-old', ],
}
running_images = [
    mesos_deployed_old,
    mesos_undeployed_old,
    mesos_undeployed_young,
    nonmesos_undeployed_old,
]


def test_get_mesos_images():
    actual = cleanup_marathon_orphaned_images.get_mesos_images(running_images)
    assert mesos_deployed_old in actual
    assert mesos_undeployed_old in actual
    assert mesos_undeployed_young in actual
    assert nonmesos_undeployed_old not in actual


def test_get_old_images():
    actual = cleanup_marathon_orphaned_images.get_old_images(running_images)
    assert mesos_deployed_old in actual
    assert mesos_undeployed_old in actual
    assert mesos_undeployed_young not in actual
    assert nonmesos_undeployed_old in actual
