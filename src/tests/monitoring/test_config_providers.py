import mock
import pytest

from paasta_tools.monitoring.config_providers import (
    extract_classic_monitoring_info,
    extract_monitoring_info,
    monitoring_keys
)


def test_extract_classic_monitoring_info():
    test_dict = {}
    for key in monitoring_keys:
        test_dict[key] = 'testing'
    test_dict['this_is_extra'] = 'so extra'
    test_dict['also_extra'] = 'moar extra'
    test_dict = {'monitoring': test_dict}

    extracted = extract_classic_monitoring_info(test_dict)
    for key in monitoring_keys:
        # Should not mutate the original
        assert key in test_dict['monitoring'] and key in extracted

    assert 'extra' in extracted
    assert extracted['extra']['this_is_extra'] == 'so extra'
    assert extracted['extra']['also_extra'] == 'moar extra'


def test_extract_monitoring_info():
    module = 'paasta_tools.monitoring.config_providers'
    extract_method = module + '.extract_classic_monitoring_info'

    with mock.patch(extract_method, return_value=-1):
        assert extract_monitoring_info('classic', {}) == -1

    with pytest.raises(Exception):
        extract_monitoring_info('foobar', {})
