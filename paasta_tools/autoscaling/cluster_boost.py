"""
Provides functions to temporary boost cluster capacity.
Useful during big service bounce or failovers.

This works by setting a temporary multiplier on the cluster's measured load.
This applies to any resource (cpu, memory, disk)
If the usage increase overtime, the boost will still apply on top of that.
Default duration of the boost factor is 40 minutes and default value is 1.5
"""
import logging
from datetime import datetime

from paasta_tools.utils import ZookeeperPool

DEFAULT_BOOST_FACTOR = 1.5
DEFAULT_BOOST_DURATION = 40

MIN_BOOST_FACTOR = 1.0
MAX_BOOST_FACTOR = 3.0

MAX_BOOST_DURATION = 240

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class BoostAlreadyActiveError(Exception):
    pass


def get_zk_boost_path(region: str, pool: str) -> str:
    return '/paasta_cluster_autoscaler/{}/{}/boost'.format(region, pool)


def get_boosted_load(region: str, pool: str, current_load: float) -> float:
    """Return the load to use for autoscaling calculations, taking into
    account the computed boost, if any.
    """
    try:
        zk_boost_path = get_zk_boost_path(region, pool)
        current_time = int(datetime.now().strftime('%s'))

        with ZookeeperPool() as zk:
            end_time, boost_factor, expected_load = get_boost_values(region, pool, zk)

            if current_time >= end_time:
                # Boost period is over. If expected_load is still nozero, reset it to 0
                if expected_load > 0:
                    zk.set(zk_boost_path + '/expected_load', '0'.encode('utf-8'))

                return current_load

            # Boost is active. If expected load wasn't already computed, set it now.
            if expected_load == 0:
                expected_load = current_load * boost_factor

                log.debug('Activating boost, storing expected load: {} in ZooKeeper'.format(expected_load))

                zk.ensure_path(zk_boost_path + '/expected_load')
                zk.set(zk_boost_path + '/expected_load', str(expected_load).encode('utf-8'))

            # We return the boosted expected_load, but only if the current load isn't greater.
            return expected_load if expected_load > current_load else current_load

    except Exception as e:
        # Fail gracefully in the face of ANY error
        log.error('get_boost failed with: {}'.format(e))
        return current_load


def is_boost_active(
    region: str,
    pool: str,
    zk: ZookeeperPool,
) -> bool:
    current_time = int(datetime.now().strftime('%s'))
    end_time, _, _ = get_boost_values(region, pool, zk)
    return current_time < end_time


def get_boost_values(
    region: str,
    pool: str,
    zk: ZookeeperPool,
) -> (int, float, float):
    # Default values, non-boost.
    end_time = 0
    boost_factor = 1.0
    expected_load = 0

    try:
        zk_boost_path = get_zk_boost_path(region, pool)
        end_time = int(zk.get(zk_boost_path + '/end_time')[0].decode('utf-8'))
        boost_factor = float(zk.get(zk_boost_path + '/factor')[0].decode('utf-8'))
        expected_load = float(zk.get(zk_boost_path + '/expected_load')[0].decode('utf-8'))

    except Exception as e:
        log.info(e)
        log.debug('No boost data in Zookeeper for pool {} in region {}.'.format(
            pool,
            region,
        ))

    return end_time, boost_factor, expected_load


def set_boost_factor(
    region: str,
    pool: str,
    factor=DEFAULT_BOOST_FACTOR,
    duration_minutes=DEFAULT_BOOST_DURATION,
    override=False,
) -> bool:
    if factor < MIN_BOOST_FACTOR:
        raise ValueError('Cannot set a boost factor smaller than {}'.format(MIN_BOOST_FACTOR))

    if factor > MAX_BOOST_FACTOR:
        raise ValueError('Boost factor > {} does not sound reasonable'.format(MAX_BOOST_FACTOR))

    if duration_minutes > MAX_BOOST_DURATION:
        raise ValueError(
            'Boost duration > {} minutes does not sound reasonable'.format(MAX_BOOST_DURATION),
        )

    zk_boost_path = get_zk_boost_path(region, pool)
    current_time = int(datetime.now().timestamp())
    end_time = current_time + 60 * duration_minutes

    zk_end_time_path = zk_boost_path + '/end_time'
    zk_factor_path = zk_boost_path + '/factor'
    zk_expected_load_path = zk_boost_path + '/expected_load'

    with ZookeeperPool() as zk:
        if not override and is_boost_active(region, pool, zk):
            raise BoostAlreadyActiveError

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

        log.info('Cluster boost: Setting capacity boost factor {} for pool {} in region {} until {}'.format(
            factor,
            pool,
            region,
            datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S UTC'),
        ))

        # Let's check that this factor has been properly written to zk
        return get_boost_values(region, pool, zk) == (end_time, factor, 0)


def clear_boost(region: str, pool: str):
    return set_boost_factor(region, pool, factor=1, duration_minutes=0, override=True)
