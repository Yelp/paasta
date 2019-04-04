"""
Provides functions to temporary boost autoscaler results
Useful during big service bounce or failovers to preemptively increase capacity.

This works by setting a temporary multiplier on the initial measured load.
The resulting increased capacity is guaranteed until the end of the boost.
If usage gets higher the pool will behave normally and scale up

Default duration of the boost factor is 40 minutes and default value is 1.5
"""
import logging
from collections import namedtuple
from datetime import datetime
from time import time as get_time

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool

clusterman_metrics, __ = get_clusterman_metrics()
DEFAULT_BOOST_FACTOR = 1.5
DEFAULT_BOOST_DURATION = 40

MIN_BOOST_FACTOR = 1.0
MAX_BOOST_FACTOR = 3.0

MAX_BOOST_DURATION = 240


BoostValues = namedtuple(
    'BoostValues',
    [
        'end_time',
        'boost_factor',
        'expected_load',
    ],
)


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_zk_cluster_boost_path(region: str, pool: str) -> str:
    return f'/paasta_cluster_autoscaler/{region}/{pool}/boost'


def get_boosted_load(zk_boost_path: str, current_load: float) -> float:
    """Return the load to use for autoscaling calculations, taking into
    account the computed boost, if any.

    This function will fail gracefully no matter what (returning the current load)
    so we don't block the autoscaler.
    """
    try:
        current_time = get_time()

        with ZookeeperPool() as zk:
            boost_values = get_boost_values(zk_boost_path, zk)

            if current_time >= boost_values.end_time:
                # If there is an expected_load value, that means we've just completed
                # a boost period. Reset it to 0
                if boost_values.expected_load > 0:
                    zk.set(zk_boost_path + '/expected_load', '0'.encode('utf-8'))

                # Boost is no longer active - return current load with no boost
                return current_load

            # Boost is active. If expected load wasn't already computed, set it now.
            if boost_values.expected_load == 0:
                expected_load = current_load * boost_values.boost_factor

                log.debug(f'Activating boost, storing expected load: {expected_load} in ZooKeeper')

                zk.ensure_path(zk_boost_path + '/expected_load')
                zk.set(zk_boost_path + '/expected_load', str(expected_load).encode('utf-8'))

            else:
                expected_load = boost_values.expected_load

            # We return the boosted expected_load, but only if the current load isn't greater.
            return expected_load if expected_load > current_load else current_load

    except Exception as e:
        # Fail gracefully in the face of ANY error
        log.error(f'get_boost failed with: {e}')
        return current_load


def get_boost_factor(zk_boost_path: str) -> float:
    """This function returns the boost factor value if a boost is active
    """
    current_time = get_time()

    with ZookeeperPool() as zk:
        boost_values = get_boost_values(zk_boost_path, zk)
        if current_time < boost_values.end_time:
            return boost_values.boost_factor
        else:
            return 1.0


def get_boost_values(
    zk_boost_path: str,
    zk: KazooClient,
) -> BoostValues:
    # Default values, non-boost.
    end_time: float = 0
    boost_factor: float = 1.0
    expected_load: float = 0

    try:
        end_time = float(zk.get(zk_boost_path + '/end_time')[0].decode('utf-8'))
        boost_factor = float(zk.get(zk_boost_path + '/factor')[0].decode('utf-8'))
        expected_load = float(zk.get(zk_boost_path + '/expected_load')[0].decode('utf-8'))

    except NoNodeError:
        # If we can't read boost values from zookeeper
        return BoostValues(
            end_time=0,
            boost_factor=1.0,
            expected_load=0,
        )

    return BoostValues(
        end_time=end_time,
        boost_factor=boost_factor,
        expected_load=expected_load,
    )


def set_boost_factor(
    zk_boost_path: str,
    region: str = '',
    pool: str = '',
    send_clusterman_metrics: bool = True,
    factor: float = DEFAULT_BOOST_FACTOR,
    duration_minutes: int = DEFAULT_BOOST_DURATION,
    override: bool = False,
) -> bool:
    """
    Set a boost factor for a path in zk

    Can be used to boost either cluster or service autoscalers.
    If using for cluster you must specify region, pool and set
    send_clusterman_metrics=True so that clusterman metrics are updated

    otherwise just zk_boost_path is enough.
    """
    if factor < MIN_BOOST_FACTOR:
        log.error(f'Cannot set a boost factor smaller than {MIN_BOOST_FACTOR}')
        return False

    if factor > MAX_BOOST_FACTOR:
        log.warning('Boost factor {} does not sound reasonable. Defaulting to {}'.format(
            factor,
            MAX_BOOST_FACTOR,
        ))
        factor = MAX_BOOST_FACTOR

    if duration_minutes > MAX_BOOST_DURATION:
        log.warning('Boost duration of {} minutes is too much. Falling back to {}.'.format(
            duration_minutes,
            MAX_BOOST_DURATION,
        ))
        duration_minutes = MAX_BOOST_DURATION

    current_time = get_time()
    end_time = current_time + 60 * duration_minutes

    if clusterman_metrics and send_clusterman_metrics:
        cluster = load_system_paasta_config().get_cluster()
        metrics_client = clusterman_metrics.ClustermanMetricsBotoClient(region_name=region, app_identifier=pool)
        with metrics_client.get_writer(clusterman_metrics.APP_METRICS) as writer:
            metrics_key = clusterman_metrics.generate_key_with_dimensions(
                'boost_factor',
                {'cluster': cluster, 'pool': pool},
            )
            writer.send((metrics_key, current_time, factor))
            if duration_minutes > 0:
                writer.send((metrics_key, end_time, 1.0))

    zk_end_time_path = zk_boost_path + '/end_time'
    zk_factor_path = zk_boost_path + '/factor'
    zk_expected_load_path = zk_boost_path + '/expected_load'

    with ZookeeperPool() as zk:
        if (
            not override and
            current_time < get_boost_values(zk_boost_path, zk).end_time
        ):
            log.error('Boost already active. Not overriding.')
            return False

        try:
            zk.ensure_path(zk_end_time_path)
            zk.ensure_path(zk_factor_path)
            zk.ensure_path(zk_expected_load_path)
            zk.set(zk_end_time_path, str(end_time).encode('utf-8'))
            zk.set(zk_factor_path, str(factor).encode('utf-8'))
            zk.set(zk_expected_load_path, '0'.encode('utf-8'))
        except Exception:
            log.error('Error setting the boost in Zookeeper')
            raise

        log.info('Load boost: Set capacity boost factor {} at path {} until {}'.format(
            factor,
            zk_boost_path,
            datetime.fromtimestamp(end_time).strftime('%c'),
        ))

        # Let's check that this factor has been properly written to zk
        return get_boost_values(zk_boost_path, zk) == BoostValues(
            end_time=end_time,
            boost_factor=factor,
            expected_load=0,
        )


def clear_boost(
    zk_boost_path: str,
    region: str = '',
    pool: str = '',
    send_clusterman_metrics: bool = True,
) -> bool:
    return set_boost_factor(
        zk_boost_path,
        region=region,
        pool=pool,
        send_clusterman_metrics=send_clusterman_metrics,
        factor=1,
        duration_minutes=0,
        override=True,
    )
