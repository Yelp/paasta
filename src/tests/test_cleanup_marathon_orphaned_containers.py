#!/usr/bin/env python

import mock
import pytest

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


@pytest.yield_fixture
def mock_get_running_images():
    return_value = [
        mesos_deployed_old,
        nonmesos_undeployed_old,
        mesos_undeployed_young,
        mesos_undeployed_old,
    ]
    with mock.patch(
        'cleanup_marathon_orphaned_images.get_running_images',
        autospec=True,
        return_value=return_value,
    ) as (
        mock_get_running_images
    ):
        yield mock_get_running_images


def test_get_running_mesos_images(mock_get_running_images):
    client = 'unused fake client'
    # this is ridiculous but just in case i actually want to leave this mocked
    running_images = cleanup_marathon_orphaned_images.get_running_images(client)
    actual = cleanup_marathon_orphaned_images.get_running_mesos_images(running_images)
    assert nonmesos_undeployed_old not in actual
