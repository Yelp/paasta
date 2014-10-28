#!/usr/bin/env python

import calendar
import datetime

import cleanup_marathon_orphaned_containers


fake_now = datetime.datetime(2014, 01, 01, 04, 20)
young_offset = datetime.timedelta(minutes=59)
old_offset = datetime.timedelta(minutes=61)
deployed_image = 'services-example_service:paasta-e682882f439de98bc9611f54563ee5c2a7785665'
mesos_deployed_old = {
    'Created': calendar.timegm((fake_now - old_offset).timetuple()),
    'Image': 'docker-registry.example.com:443/%s' % deployed_image,
    'Names': ['/mesos-deployed-old', ],
}
mesos_undeployed_old = {
    'Created': calendar.timegm((fake_now - old_offset).timetuple()),
    'Image': 'Desmond Lockheart\'s older mesos image',
    'Names': ['/mesos-undeployed-old', ],
}
mesos_undeployed_young = {
    'Created': calendar.timegm((fake_now - young_offset).timetuple()),
    'Image': 'Desmond Lockheart\'s newer mesos image',
    'Names': ['/mesos-undeployed-young', ],
}
nonmesos_undeployed_old = {
    'Created': calendar.timegm((fake_now - old_offset).timetuple()),
    'Image': 'Professor Calvert\'s development image',
    'Names': ['/nonmesos-undeployed-old', ],
}
running_containers = [
    mesos_deployed_old,
    mesos_undeployed_old,
    mesos_undeployed_young,
    nonmesos_undeployed_old,
]


def test_get_mesos_containers():
    actual = cleanup_marathon_orphaned_containers.get_mesos_containers(running_containers)
    assert mesos_deployed_old in actual
    assert mesos_undeployed_old in actual
    assert mesos_undeployed_young in actual
    assert nonmesos_undeployed_old not in actual


def test_get_old_containers():
    actual = cleanup_marathon_orphaned_containers.get_old_containers(running_containers, now=fake_now)
    assert mesos_deployed_old in actual
    assert mesos_undeployed_old in actual
    assert mesos_undeployed_young not in actual
    assert nonmesos_undeployed_old in actual


def test_get_undeployed_containers():
    deployed_images = set([deployed_image])
    actual = cleanup_marathon_orphaned_containers.get_undeployed_containers(running_containers, deployed_images)
    assert mesos_deployed_old not in actual
    assert mesos_undeployed_old in actual
    assert mesos_undeployed_young in actual
    assert nonmesos_undeployed_old in actual
