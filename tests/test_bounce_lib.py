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
import mock

from paasta_tools import bounce_lib
from paasta_tools import utils


class TestBounceLib:
    def fake_system_paasta_config(self):
        return utils.SystemPaastaConfig({"synapse_port": 123456}, "/fake/configs")

    def test_get_bounce_method_func(self):
        actual = bounce_lib.get_bounce_method_func("brutal")
        expected = bounce_lib.brutal_bounce
        assert actual == expected


class TestBrutalBounce:
    def test_brutal_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, brutal bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345"}
        happy_tasks = []

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_brutal_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, brutal bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_brutal_bounce_mid_bounce(self):
        """When marathon has the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_brutal_bounce_old_but_no_new(self):
        """When marathon does not have the desired app, but there are other copies of
        the service running, brutal bounce should stop the old ones and start
        the new one."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.brutal_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": True,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }


class TestUpthendownBounce:
    def test_upthendown_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, upthendown bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345"}
        happy_tasks = []

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_upthendown_bounce_old_but_no_new(self):
        """When marathon has the desired app, but there are other copies of
        the service running, upthendown bounce should start the new one. but
        not stop the old one yet."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=[],
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_upthendown_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of
        the service running, but the new app is not fully up, upthendown bounce
        should not stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_upthendown_bounce_cleanup(self):
        """When marathon has the desired app, and there are other copies of
        the service running, and the new app is fully up, upthendown bounce
        should stop the old ones."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_upthendown_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, upthendown bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.upthendown_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}


class TestCrossoverBounce:
    def test_crossover_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, crossover bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_crossover_bounce_old_but_no_new(self):
        """When marathon only has old apps for this service, crossover bounce should start the new one, but not kill any
        old tasks yet."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_crossover_bounce_old_app_is_happy_but_no_new_app_happy_tasks(self):
        """When marathon only has old apps for this service and margin_factor != 1,
        crossover bounce should start the new app and kill some old tasks."""

        new_config = {"id": "foo.bar.12345", "instances": 100}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(100)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.95,
        )
        assert actual["create_app"] is True
        assert len(actual["tasks_to_drain"]) == 5

    def test_crossover_bounce_some_unhappy_old_some_happy_old_no_new(self):
        """When marathon only has old apps for this service, and some of them are unhappy (maybe they've been recently
        started), the crossover bounce should start a new app and prefer killing the unhappy tasks over the happy ones.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set(old_app_live_unhappy_tasks)}

    def test_crossover_bounce_some_unhappy_old_no_happy_old_no_new_tasks_no_excess(
        self,
    ):
        """When marathon only has old apps for this service, and all of their tasks are unhappy, and there are no excess
        tasks, the crossover bounce should start a new app and not kill any old tasks.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}

    def test_crossover_bounce_lots_of_unhappy_old_no_happy_old_no_new(self):
        """When marathon has a new app and multiple old apps, no new tasks are up, all old tasks are unhappy, and there
        are too many tasks running, the crossover bounce should kill some (but not all) of the old
        tasks.

        This represents a situation where
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(10)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 5

    def test_crossover_bounce_lots_of_unhappy_old_some_happy_old_new_app_exists_no_new_tasks(
        self,
    ):
        """When marathon has a new app and multiple old apps, no new tasks are up, one of the old apps is healthy and
        the other is not, only unhealthy tasks should get killed.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert actual["tasks_to_drain"] == set(old_app_live_unhappy_tasks)
        # Since there are plenty of unhappy old tasks, we should not kill any new ones.
        assert len(actual["tasks_to_drain"] & set(old_app_live_happy_tasks)) == 0

    def test_crossover_bounce_mid_bounce(self):
        """When marathon has the desired app, and there are other copies of the service running, but the new app is not
        fully up, crossover bounce should only stop a few of the old instances."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 3

    def test_crossover_bounce_mid_bounce_some_happy_old_some_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, and some of those
        older tasks are unhappy, we should prefer killing the unhappy tasks."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(1)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 4
        # There are fewer unhappy old tasks than excess tasks, so we should kill all unhappy old ones, plus a few
        # happy ones.
        assert set(old_app_live_unhappy_tasks).issubset(actual["tasks_to_drain"])

    def test_crossover_bounce_mid_bounce_some_happy_old_lots_of_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, and there are more
        unhappy old tasks than excess tasks, we should only kill unhappy tasks.
        """

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(2)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(5)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )

        assert actual["create_app"] is False
        # There are as many unhappy old tasks as excess tasks, so all tasks that we kill should be old unhappy ones.
        assert len(actual["tasks_to_drain"]) == 5
        assert actual["tasks_to_drain"] == set(old_app_live_unhappy_tasks)

    def test_crossover_bounce_mid_bounce_no_happy_old_lots_of_unhappy_old(self):
        """When marathon has the desired app, and there are other copies of the service running, but none of the old
        tasks are happy, and there are excess tasks, we should kill some (but not all) unhappy old tasks."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(6)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 4

    def test_crossover_bounce_using_margin_factor_big_numbers(self):
        new_config = {"id": "foo.bar.12345", "instances": 500}
        happy_tasks = [mock.Mock() for _ in range(100)]
        old_app_live_happy_tasks = [mock.Mock() for _ in range(300)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(100)]

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.95,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 25

    def test_crossover_bounce_using_margin_factor_small_numbers(self):
        new_config = {"id": "foo.bar.12345", "instances": 3}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(3)]
        old_app_live_unhappy_tasks = []

        actual = bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
            margin_factor=0.66,
        )
        assert actual["create_app"] is False
        assert len(actual["tasks_to_drain"]) == 1

    def test_crossover_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of
        the service running, crossover bounce should neither start nor stop
        anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.crossover_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": False, "tasks_to_drain": set()}


class TestDownThenUpBounce:
    def test_downthenup_bounce_no_existing_apps(self):
        """When marathon is unaware of a service, downthenup bounce should try to
        create a marathon app."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = []
        old_app_live_unhappy_tasks = []

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {"create_app": True, "tasks_to_drain": set()}

    def test_downthenup_bounce_old_but_no_new(self):
        """When marathon has only old copies of the service, downthenup_bounce should kill them and not start a new one
        yet."""
        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = []
        old_app_live_happy_tasks = [mock.Mock() for _ in range(5)]
        old_app_live_unhappy_tasks = [mock.Mock() for _ in range(1)]

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=False,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=old_app_live_happy_tasks
            + old_app_live_unhappy_tasks,
        ) == {
            "create_app": False,
            "tasks_to_drain": set(
                old_app_live_happy_tasks + old_app_live_unhappy_tasks
            ),
        }

    def test_downthenup_bounce_done(self):
        """When marathon has the desired app, and there are no other copies of the service running, downthenup bounce
        should neither start nor stop anything."""

        new_config = {"id": "foo.bar.12345", "instances": 5}
        happy_tasks = [mock.Mock() for _ in range(5)]

        assert bounce_lib.downthenup_bounce(
            new_config=new_config,
            new_app_running=True,
            happy_new_tasks=happy_tasks,
            old_non_draining_tasks=[],
        ) == {"create_app": False, "tasks_to_drain": set()}


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
