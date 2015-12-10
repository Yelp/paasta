# Copyright 2015 Yelp Inc.
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
from pytest import raises
from pytest import yield_fixture

import paasta_tools.cli.cmds.fsm as fsm
from paasta_tools.cli.fsm.questions import get_clusternames_from_deploy_stanza


class TestQuestions:
    @yield_fixture
    def mock_ask(self):
        """Calling raw_input() from automated tests can ruin your day, so we'll
        mock it out even for those situations where we don't care about it and
        "shouldn't" call raw_input().
        """
        with mock.patch("paasta_tools.cli.fsm.questions.ask", autospec=True) as (
            mock_ask
        ):
            yield mock_ask


class TestGetSrvnameTestCase(TestQuestions):
    def test_arg_passed_in(self, mock_ask):
        """If a value is specified, use it."""
        srvname = "services/fake"
        auto = "UNUSED"
        expected = srvname
        actual = fsm.get_srvname(srvname, auto)
        assert expected == actual
        assert 0 == mock_ask.call_count

    def test_arg_not_passed_in_auto_true(self, mock_ask):
        """If a value is not specified but --auto was requested, calculate and
        use a sane default.

        In this specific case there is no sane default, so blow up.
        """
        srvname = None
        auto = True
        with raises(SystemExit) as err:
            fsm.get_srvname(srvname, auto)
        assert "I'd Really Rather You Didn't Use --auto Without --service-name" in err.value
        assert 0 == mock_ask.call_count

    def test_arg_not_passed_in_auto_false(self, mock_ask):
        """If a value is not specified but and --auto was not requested, prompt
        the user.
        """
        srvname = None
        auto = False
        fsm.get_srvname(srvname, auto)
        assert 1 == mock_ask.call_count


class TestGetSmartstackStanzaTestCase(TestQuestions):
    @yield_fixture
    def fake_suggested_port(self):
        fake_suggested_port = 12345
        yield fake_suggested_port

    @yield_fixture
    def fake_expected_stanza(self, fake_suggested_port):
        fake_expected_stanza = {
            "main": {
                "advertise": ["superregion"],
                "discover": "superregion",
                "proxy_port": fake_suggested_port,
                "extra_advertise": {
                    "ecosystem:testopia": ["ecosystem:testopia"]
                },
            }
        }
        yield fake_expected_stanza

    def test_arg_passed_in(self, mock_ask, fake_suggested_port, fake_expected_stanza):
        """If a port is specified, use it."""
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        port = fake_suggested_port
        auto = "UNUSED"

        actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        assert fake_expected_stanza == actual
        assert 0 == mock_ask.call_count

    def test_arg_not_passed_in_auto_true(self, mock_ask, fake_suggested_port, fake_expected_stanza):
        """If a value is not specified but --auto was requested, calculate and
        use a sane default.
        """
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        port = None
        auto = True

        with mock.patch(
            "paasta_tools.cli.fsm.questions.suggest_smartstack_proxy_port",
            autospec=True,
            return_value=fake_suggested_port,
        ) as (
            mock_suggest_smartstack_proxy_port
        ):
            actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        mock_suggest_smartstack_proxy_port.assert_called_once_with(
            yelpsoa_config_root)
        assert fake_expected_stanza == actual
        assert 0 == mock_ask.call_count

    def test_arg_not_passed_in_auto_false(self, mock_ask, fake_suggested_port, fake_expected_stanza):
        """If a value is not specified and --auto was not requested, prompt
        the user.
        """
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        port = None
        suggested_port = 12345
        auto = False

        mock_ask.return_value = suggested_port
        with mock.patch(
            "paasta_tools.cli.fsm.questions.suggest_smartstack_proxy_port",
            autospec=True,
            return_value=suggested_port,
        ) as (
            mock_suggest_smartstack_proxy_port
        ):
            actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        mock_suggest_smartstack_proxy_port.assert_called_once_with(
            yelpsoa_config_root)
        assert fake_expected_stanza == actual
        mock_ask.assert_called_once_with(
            mock.ANY,
            suggested_port,
        )


class TestGetMonitoringStanzaTestCase(TestQuestions):
    @yield_fixture
    def mock_list_teams(self):
        with mock.patch("paasta_tools.cli.fsm.questions.list_teams", autospec=True) as (
            mock_list_teams
        ):
            mock_list_teams.return_value = set([
                "red_jaguars",
                "blue_barracudas",
                "green_monkeys",
            ])
            yield mock_list_teams

    def test_valid_team_passed_in(self, mock_ask, mock_list_teams):
        team = "red_jaguars"
        auto = "UNUSED"

        actual = fsm.get_monitoring_stanza(auto, team)
        assert ("team", team) in actual.items()
        assert ("service_type", "marathon") in actual.items()

    def test_invalid_team_passed_in(self, mock_ask, mock_list_teams):
        team = "america world police"
        auto = "UNUSED"

        with raises(SystemExit):
            fsm.get_monitoring_stanza(auto, team)

    def test_team_not_passed_in_auto_true(self, mock_ask, mock_list_teams):
        """If a value is not specified but --auto was requested, calculate and
        use a sane default.
        """
        team = None
        auto = True

        with raises(SystemExit) as err:
            fsm.get_monitoring_stanza(auto, team)
        assert "I'd Really Rather You Didn't Use --auto Without --team" in err.value
        assert 0 == mock_ask.call_count

    def test_team_not_passed_in_auto_false(self, mock_ask, mock_list_teams):
        """If a value is not specified but --auto was not requested, prompt the
        user.
        """
        team = None
        auto = False

        mock_ask.return_value = "red_jaguars"
        actual = fsm.get_monitoring_stanza(auto, team)
        assert 1 == mock_ask.call_count
        assert ("team", mock_ask.return_value) in actual.items()


class TestGetDeployStanzaTestCase(TestQuestions):
    def test(self, mock_ask):
        actual = fsm.get_deploy_stanza()
        assert "pipeline" in actual.keys()
        actual["pipeline"] = actual["pipeline"]

        for expected_entry in (
            {"instancename": "itest"},
            {"instancename": "security-check"},
            {"instancename": "performance-check"},
            {"instancename": "pnw-stagea.main"},
            {
                "instancename": "nova-prod.canary",
                "trigger_next_step_manually": True,
            },
        ):
            assert expected_entry in actual["pipeline"]


class TestGetClusternamesFromDeployStanzaTestCase(TestQuestions):
    def test_empty(self, mock_ask):
        deploy_stanza = {}
        expected = set()
        actual = get_clusternames_from_deploy_stanza(deploy_stanza)
        assert expected == actual

    def test_non_empty(self, mock_ask):
        deploy_stanza = {}
        deploy_stanza["pipeline"] = [
            {"instancename": "itest", },
            {"instancename": "push-to-registry", },
            {"instancename": "mesosstage.canary", },
            {"instancename": "norcal-devc.main", "trigger_next_step_manually": True, },
            {"instancename": "nova-prod.main.with.extra.dots", },
            {"instancename": "clustername-without-namespace", },
        ]
        expected = set([
            "mesosstage",
            "norcal-devc",
            "nova-prod",
            "clustername-without-namespace",
        ])
        actual = get_clusternames_from_deploy_stanza(deploy_stanza)
        assert expected == actual


class TestGetMarathonStanzaTestCase(TestQuestions):
    def test(self, mock_ask):
        actual = fsm.get_marathon_stanza()
        assert "main" in actual.keys()
        assert "canary" in actual.keys()
