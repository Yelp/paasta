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
"""
Creates a tween that cprofiles requests
"""
import pyramid
import pytz

from paasta_tools.api import settings as api_settings

try:
    # hackily patch pytz, since yelp_lib (used by yelp_profiling) type checks
    # using an older version of pytz where tzinfo didn't exist yet. this needs to
    # happen before importing `yelp_profiling` since it is used at import time.
    pytz.BaseTzInfo = pytz.tzinfo.BaseTzInfo  # type: ignore
    import yelp_profiling
    from yelp_profiling.cprofile import CProfileConfig
    from yelp_profiling.cprofile import CProfileContextManager
    from yelp_profiling.tweens import YelpSOARequestProcessor

    class PaastaCProfileConfig(CProfileConfig):
        """Paasta API version of yelp_profiling's CProfileConfig. Instead of reading
        from srv-configs, this reads configs from /etc/paasta.
        """

        def __init__(self, pyramid_settings):
            super().__init__(pyramid_settings)
            self._cprofile_config = (
                api_settings.system_paasta_config.get_api_profiling_config()
            )

        @property
        def enabled(self):
            return self._cprofile_config.get("cprofile_sampling_enabled", False)

        @property
        def output_prefix(self):
            return self._cprofile_config.get("cprofile_output_prefix", "cprofile")

        @property
        def scribe_log_name(self):
            return self._cprofile_config.get(
                "cprofile_scribe_log", "tmp_paasta_api_cprofiles"
            )

        @property
        def default_probability(self):
            return self._cprofile_config.get("cprofile_sampling_probability", 0)

        @property
        def path_probabilities(self):
            probabilities = {"patterns": []}
            probabilities.update(
                self._cprofile_config.get("cprofile_path_probabilities", {})
            )
            return probabilities

except ImportError:
    yelp_profiling = None


def includeme(config):
    if yelp_profiling is not None:
        config.add_tween(
            "paasta_tools.api.tweens.profiling.cprofile_tween_factory",
            under=pyramid.tweens.INGRESS,
        )


def cprofile_tween_factory(handler, registry):
    """Tween for profiling API requests and sending them to scribe.

    yelp_profiling does define a tween, but it is designed more for PaaSTA
    services. So, we need to define our own.
    """

    def cprofile_tween(request):
        if yelp_profiling is None:
            return handler(request)

        config = PaastaCProfileConfig(registry.settings)
        processor = YelpSOARequestProcessor(config, registry)
        context_manager = CProfileContextManager(config, processor)

        # uses the config and processor to decide whether or not to cprofile
        # the request
        with context_manager(request):
            processor.begin_request(request)
            status_code = 500
            try:
                response = handler(request)
                status_code = response.status_code
                return response
            finally:
                processor.end_request(request, status_code)

    return cprofile_tween
