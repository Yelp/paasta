# Unit tests for soa_collector.json_to_metrics()
from soa_collector import json_to_metrics


def test_empty():
    json_dict = {}
    metrics = json_to_metrics(json_dict)
    assert len(metrics) == 0


def test_arbitrary_metric():
    json_dict = {
        "flippy": {
            "foo": 0,
        }
    }
    metrics = json_to_metrics(json_dict)
    assert len(metrics) == 1
    metric_segments, value, type = metrics.pop()
    assert metric_segments == ['flippy','foo']
    assert value == 0
    assert type is None


def test_deeply_nested_counter():
    json_dict = {
        "fee": {
            "fii": {
                "foe": {
                    "fum": {
                        "active-suspended-requests": {
                            "count": 0,
                            "type": "counter"
                        }
                    }
                }
            }
        }
    }
    metrics = json_to_metrics(json_dict)
    assert len(metrics) == 1


def test_invalid_metric_value_skipped():
    json_dict = {
        "meters": {
            "pyramid_uwsgi_metrics.tweens.2xx-responses": {
                "units": "events/second"
            }
        }
    }
    assert len(json_to_metrics(json_dict)) == 0


def test_invalid_metric_type_skipped():
    json_dict = {
        "meters": {
            "pyramid_uwsgi_metrics.tweens.2xx-responses": {
                "units": []
            }
        }
    }
    assert len(json_to_metrics(json_dict)) == 0
