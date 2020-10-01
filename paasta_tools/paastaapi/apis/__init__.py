# coding: utf-8

# flake8: noqa

# Import all APIs into this package.
# If you have many APIs here with many many models used in each API this may
# raise a `RecursionError`.
# In order to avoid this, import only the API that you directly need like:
#
#   from .api.autoscaler_api import AutoscalerApi
#
# or import this package, but before doing it, use:
#
#   import sys
#   sys.setrecursionlimit(n)

# Import APIs into API package:
from paasta_tools.paastaapi.api.autoscaler_api import AutoscalerApi
from paasta_tools.paastaapi.api.default_api import DefaultApi
from paasta_tools.paastaapi.api.marathon_dashboard_api import MarathonDashboardApi
from paasta_tools.paastaapi.api.resources_api import ResourcesApi
from paasta_tools.paastaapi.api.service_api import ServiceApi
