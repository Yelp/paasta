import os
import socket
import struct
import time
from collections import defaultdict
from typing import Callable
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Tuple

import arrow
import colorlog
import simplejson as json
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import MetricsValuesDict
from clusterman_metrics import SYSTEM_METRICS
from mypy_extensions import TypedDict
from retry import retry
from simplejson.errors import JSONDecodeError
from staticconf.errors import ConfigurationError

from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import MetricsError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.exceptions import SignalValidationError
from clusterman.util import get_cluster_dimensions

logger = colorlog.getLogger(__name__)

ACK = bytes([1])
DEFAULT_SIGNALS_BUCKET = 'yelp-clusterman-signals'
SOCKET_MESG_SIZE = 4096
SOCKET_TIMEOUT_SECONDS = 300
SIGNAL_LOGGERS: Mapping[
    str,
    Tuple[
        Callable[[str], None],
        Callable[[str], None],
    ]
] = {}
SignalResponseDict = Dict[str, Optional[float]]


class MetricsConfigDict(TypedDict):
    name: str
    type: str
    minute_range: int
    regex: bool


class Signal:
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
        metrics_client: ClustermanMetricsBotoClient,
        signal_namespace: str,
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

        self.required_metrics: list = reader.read_list('autoscale_signal.required_metrics', default=[])

        self.metrics_client: ClustermanMetricsBotoClient = metrics_client
        self.signal_namespace = signal_namespace
        self._signal_conn: socket.socket = self._connect_to_signal_process()

    def evaluate(self, timestamp: arrow.Arrow, retry_on_broken_pipe: bool = True) -> SignalResponseDict:
        """ Communicate over a Unix socket with the signal to evaluate its result

        :param timestamp: a Unix timestamp to pass to the signal as the "current time"
        :param retry_on_broken_pipe: if the signal socket pipe is broken, restart the signal process and try again
        :returns: a dict of resource_name -> requested resources from the signal
        :raises SignalConnectionError: if the signal connection fails for some reason
        """
        # Get the required metrics for the signal
        metrics = self._get_metrics(timestamp)

        try:
            # First send the length of the metrics data
            metric_bytes = json.dumps({'metrics': metrics, 'timestamp': timestamp.timestamp}).encode()
            len_metrics = struct.pack('>I', len(metric_bytes))  # bytes representation of the length, packed big-endian
            self._signal_conn.send(len_metrics)
            response = self._signal_conn.recv(SOCKET_MESG_SIZE)
            if response != ACK:
                raise SignalConnectionError(f'Error occurred sending metric length to signal (response={response})')

            # Then send the actual metrics data, broken up into chunks
            for i in range(0, len(metric_bytes), SOCKET_MESG_SIZE):
                self._signal_conn.send(metric_bytes[i:i + SOCKET_MESG_SIZE])
            response = self._signal_conn.recv(SOCKET_MESG_SIZE)
            ack_bit = response[:1]
            if ack_bit != ACK:
                raise SignalConnectionError(f'Error occurred sending metric data to signal (response={response})')

            # Sometimes the signal sends the ack and the reponse "too quickly" so when we call
            # recv above it gets both values.  This should handle that case, or call recv again
            # if there's no more data in the previous message
            response = response[1:] or self._signal_conn.recv(SOCKET_MESG_SIZE)
            logger.info(response)

            return json.loads(response)['Resources']

        except JSONDecodeError as e:
            raise ClustermanSignalError('Signal evaluation failed') from e
        except BrokenPipeError as e:
            if retry_on_broken_pipe:
                logger.error('Signal connection failed; reloading the signal and trying again')
                time.sleep(5)  # give supervisord some time to restart the signal
                self._signal_conn = self._connect_to_signal_process()
                return self.evaluate(timestamp, retry_on_broken_pipe=False)
            else:
                raise ClustermanSignalError('Signal evaluation failed') from e

    @retry(exceptions=ConnectionRefusedError, tries=3, delay=5)  # retry signal connection in case it's slow to start
    def _connect_to_signal_process(self) -> socket.socket:
        """ Create a connection to the specified signal over a unix socket

        :returns: a socket connection which can read/write data to the specified signal
        """
        # this creates an abstract namespace socket which is auto-cleaned on program exit
        signal_conn = socket.socket(socket.AF_UNIX)
        signal_conn.connect(f'\0{self.signal_namespace}-{self.name}-{self.app}-socket')

        signal_kwargs = json.dumps({'parameters': self.parameters})
        signal_conn.send(signal_kwargs.encode())
        logger.info(f'Connected to signal {self.name} from {self.signal_namespace}')

        return signal_conn

    def _get_metrics(self, end_time: arrow.Arrow) -> MetricsValuesDict:
        """ Get the metrics required for a signal """

        metrics: MetricsValuesDict = defaultdict(list)
        metric_dict: MetricsConfigDict
        for metric_dict in self.required_metrics:
            if metric_dict['type'] not in (SYSTEM_METRICS, APP_METRICS):
                raise MetricsError(f"Metrics of type {metric_dict['type']} cannot be queried by signals.")

            # Need to add the cluster/pool to get the right system metrics
            # TODO (CLUSTERMAN-126) this should probably be cluster/pool/app eventually
            # TODO (CLUSTERMAN-446) if a mesos pool and a k8s pool share the same app_name,
            #      APP_METRICS will be used for both
            if metric_dict['type'] == SYSTEM_METRICS:
                dims_list = [get_cluster_dimensions(self.cluster, self.pool, self.scheduler)]
                if self.scheduler == 'mesos':  # handle old (non-scheduler-aware) metrics
                    dims_list.insert(0, get_cluster_dimensions(self.cluster, self.pool, None))
            else:
                dims_list = [{}]

            # We only support regex expressions for APP_METRICS
            if 'regex' not in metric_dict:
                metric_dict['regex'] = False

            start_time = end_time.shift(minutes=-metric_dict['minute_range'])
            for dims in dims_list:
                query_results = self.metrics_client.get_metric_values(
                    metric_dict['name'],
                    metric_dict['type'],
                    start_time.timestamp,
                    end_time.timestamp,
                    is_regex=metric_dict['regex'],
                    extra_dimensions=dims,
                    app_identifier=self.app,
                )
                for metric_name, ts in query_results.items():
                    metrics[metric_name].extend(ts)
                    # safeguard; the metrics _should_ already be sorted since we inserted the old
                    # (non-scheduler-aware) metrics before the new metrics above, so this should be fast
                    metrics[metric_name].sort()
        return metrics


def setup_signals_environment(pool: str, scheduler: str) -> Tuple[int, int]:
    app_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=scheduler)
    default_signal_version = staticconf.read_string('autoscale_signal.branch_or_tag')
    signal_versions = [default_signal_version]
    signal_namespaces = [staticconf.read_string('autoscaling.default_signal_role')]
    signal_names = [staticconf.read_string('autoscale_signal.name')]
    app_names = ['__default__']

    app_signal_name = staticconf.read_string(
        'autoscale_signal.name',
        namespace=app_namespace,
        default=None,
    )
    if app_signal_name:
        signal_names.append(app_signal_name)
        signal_versions.append(staticconf.read_string(
            'autoscale_signal.branch_or_tag',
            namespace=app_namespace,
            default=pool,
        ))
        signal_namespaces.append(
            staticconf.read_string('autoscale_signal.namespace', namespace=app_namespace, default=pool),
        )
        app_names.append(pool)

    versions_to_fetch = set(signal_versions)
    os.environ['CMAN_VERSIONS_TO_FETCH'] = ' '.join(versions_to_fetch)
    os.environ['CMAN_SIGNAL_VERSIONS'] = ' '.join(signal_versions)
    os.environ['CMAN_SIGNAL_NAMESPACES'] = ' '.join(signal_namespaces)
    os.environ['CMAN_SIGNAL_NAMES'] = ' '.join(signal_names)
    os.environ['CMAN_SIGNAL_APPS'] = ' '.join(app_names)
    os.environ['CMAN_NUM_VERSIONS'] = str(len(versions_to_fetch))
    os.environ['CMAN_NUM_SIGNALS'] = str(len(signal_versions))
    os.environ['CMAN_SIGNALS_BUCKET'] = staticconf.read_string('aws.signals_bucket', default=DEFAULT_SIGNALS_BUCKET)

    return len(versions_to_fetch), len(signal_versions)
