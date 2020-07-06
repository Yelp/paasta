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
from abc import ABCMeta
from abc import abstractmethod
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import arrow
import staticconf
from staticconf.errors import ConfigurationError

from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalValidationError

SignalResponseDict = Dict[str, Optional[float]]


class Signal(metaclass=ABCMeta):
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
    ) -> None:
        """ Create an encapsulation of the Unix sockets via which we communicate with signals

        :param cluster: the name of the cluster this signal is for
        :param pool: the name of the pool this signal is for
        :param app: the name of the application this signal is for
        :param config_namespace: the staticconf namespace we can find the signal config in
        :param metrics_client: the metrics client to use to populate signal metrics
        :param signal_namespace: the namespace in the signals repo to find the signal class
            (if this is None, we default to the app name)
        """
        reader = staticconf.NamespaceReaders(config_namespace)

        try:
            self.name: str = reader.read_string('autoscale_signal.name')
        except ConfigurationError:
            raise NoSignalConfiguredException(f'No signal was configured in {config_namespace}')

        self.cluster: str = cluster
        self.pool: str = pool
        self.scheduler: str = scheduler
        self.app: str = app

        self.period_minutes: int = reader.read_int('autoscale_signal.period_minutes')
        if self.period_minutes <= 0:
            raise SignalValidationError(f'Length of signal period must be positive, got {self.period_minutes}')

        self.parameters: Dict = {
            key: value
            for param_dict in reader.read_list('autoscale_signal.parameters', default=[])
            for (key, value) in param_dict.items()
        }
        # Even if cluster and pool were set in parameters, we override them here
        # as we want to preserve a single source of truth
        self.parameters.update(dict(
            cluster=self.cluster,
            pool=self.pool,
        ))

    @abstractmethod
    def evaluate(
            self,
            timestamp: arrow.Arrow,
            retry_on_broken_pipe: bool = True,
     ) -> Union[SignalResponseDict, List[SignalResponseDict]]:
        """ Compute a signal and return either a single response (representing an aggregate resource request), or a
        list of responses (representing per-pod resource requests)

        :param timestamp: a Unix timestamp to pass to the signal as the "current time"
        :param retry_on_broken_pipe: if the signal socket pipe is broken, restart the signal process and try again
        :returns: a dict of resource_name -> requested resources from the signal
        :raises SignalConnectionError: if the signal connection fails for some reason
        """
        pass
