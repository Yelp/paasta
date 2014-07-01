import marathon_tools
import mock
import contextlib
import service_configuration_lib

class TestMarathonTools:

    fake_marathon_job_config = {
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'docker_image': 'test_docker:1.0',
        'iteration': 'testin',
    }
    fake_srv_config = {
        'runs_on': ['some-box'],
        'deployed_on': ['some-box'],
    }

    def test_read_srv_config(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        def conf_helper(name, filename, soa_dir="AAAAAAAAA"):
            if filename == 'marathon-amnesia':
                return {fake_instance: self.fake_marathon_job_config}
            elif filename == 'service':
                return self.fake_srv_config
            else:
                raise Exception
        expected = dict(self.fake_srv_config.items() + self.fake_marathon_job_config.items())
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        side_effect=conf_helper) as extra_patch:
            actual = marathon_tools.read_srv_config(fake_name, fake_instance,
                                                    fake_cluster, fake_dir)
            assert expected == actual
            extra_patch.assert_has_call(fake_name, "service", soa_dir=fake_dir)
            extra_patch.assert_has_call(fake_name, "marathon-amnesia", soa_dir=fake_dir)
            assert extra_patch.call_count == 2

    def test_brutal_bounce(self):
        old_ids = ["bbounce", "the_best_bounce_method"]
        new_config = {"now_featuring": "no_gracefuls", "guaranteed": "or_your_money_back", 'id': 'none'}
        fake_client = mock.MagicMock(delete_app=mock.Mock(), create_app=mock.Mock())
        marathon_tools.brutal_bounce(old_ids, new_config, fake_client)
        for oid in old_ids:
            fake_client.delete_app.assert_has_call(oid)
        assert fake_client.delete_app.call_count == len(old_ids)
        fake_client.create_app.assert_called_once_with(**new_config)
