# -*- coding: utf-8 -*-
import contextlib
import mock

from paasta_tools.diamond_collector import soa_collector
from paasta_tools import marathon_tools


def test_collect():
    fake_services = [('myservice.main', {'port': 1234})]
    fake_metrics_data = {
        'meters': {
            'my_meter': {
                'count': 1,
                'mean_rate': 2,
                'm1_rate': 3,
                'm5_rate': 4,
                'm15_rate': 5,
                'unit': 'seconds',
                'event_type': 'requests',
                'type': 'meter'
            }
        }
    }

    collector = soa_collector.SOACollector()

    with contextlib.nested(
        mock.patch.object(marathon_tools, 'get_services_running_here_for_nerve',
                          autospec=True,
                          return_value=fake_services),
        mock.patch.object(collector, 'publish', autospec=True),
        mock.patch.object(soa_collector, 'get_json_metrics',
                          autospec=True,
                          return_value=fake_metrics_data)
    ) as (
        get_services_patch,
        publish_patch,
        get_json_metrics_patch,
    ):
        collector.collect()

        assert publish_patch.call_count == 5
        publish_patch.assert_any_call('myservice.meters.my_meter.count', 1,
                                      metric_type='COUNTER', raw_value=1, precision=4)
        publish_patch.assert_any_call('myservice.meters.my_meter.mean_rate', 2,
                                      metric_type='GAUGE', raw_value=2, precision=4)
        publish_patch.assert_any_call('myservice.meters.my_meter.m1_rate', 3,
                                      metric_type='GAUGE', raw_value=3, precision=4)
        publish_patch.assert_any_call('myservice.meters.my_meter.m5_rate', 4,
                                      metric_type='GAUGE', raw_value=4, precision=4)
        publish_patch.assert_any_call('myservice.meters.my_meter.m15_rate', 5,
                                      metric_type='GAUGE', raw_value=5, precision=4)
