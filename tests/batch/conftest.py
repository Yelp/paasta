import warnings

import pytest
import staticconf.testing

try:
    import yelp_batch  # noqa
except ImportError:
    warnings.warn('Could not import yelp_batch, are you in a Yelp-y environment?  Skipping these tests')
    collect_ignore_glob = ['*']


@pytest.fixture(autouse=True)
def mock_setup_config_directory():
    with staticconf.testing.PatchConfiguration(
        {'cluster_config_directory': '/a/fake/directory/'}
    ):
        yield
