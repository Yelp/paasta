# Copyright 2019 Yelp Inc.
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


class ClustermanException(Exception):
    pass


class AllResourceGroupsAreStaleError(Exception):
    pass


class AutoscalerError(ClustermanException):
    pass


class ClustermanSignalError(ClustermanException):
    pass


class MetricsError(ClustermanException):
    pass


class NoLaunchTemplateConfiguredError(ClustermanException):
    pass


class NoSignalConfiguredException(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass


class PoolManagerError(ClustermanException):
    pass


class PoolConnectionError(PoolManagerError):
    """Raised when the pool master cannot be reached"""
    pass


class ResourceRequestError(ClustermanException):
    pass


class SignalValidationError(ClustermanSignalError):
    pass


class SignalConnectionError(ClustermanSignalError):
    pass


class SimulationError(ClustermanException):
    pass
