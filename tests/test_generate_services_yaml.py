import mock

import generate_services_yaml


MOCK_NAMESPACES = [
    ('foo.main', {'proxy_port': 1024}),
    ('bar.canary', {'proxy_port': 1025}),
]


def test_generate_configuration():
    expected = {
        'foo.main': {
            'host': '169.254.255.254',
            'port': 1024
        },
        'bar.canary': {
            'host': '169.254.255.254',
            'port': 1025
        }
    }

    with mock.patch('generate_services_yaml.get_all_namespaces',
                    return_value=MOCK_NAMESPACES):
        actual = generate_services_yaml.generate_configuration()

    assert expected == actual
