import logging
import random
import re
import time
from bisect import bisect
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal  # noqa (only used in type-checking)
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Set
from typing import Tuple

import boto3  # noqa (only used in type-checking)
import staticconf
from boto3.dynamodb.conditions import Key
from clusterman_metrics.util.aws import get_metrics_session
from clusterman_metrics.util.constants import APP_METRICS
from clusterman_metrics.util.constants import CLUSTERMAN_NAME
from clusterman_metrics.util.constants import CONFIG_NAMESPACE
from clusterman_metrics.util.constants import METADATA
from clusterman_metrics.util.constants import METRIC_TYPES
from clusterman_metrics.util.meteorite import generate_key_with_dimensions
from clusterman_metrics.util.misc import convert_decimal

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


GSI_NAME = 'metrics_key_lookup'
GSI_PK = 'gsi_partition'
GSI_SORT = 'app_timestamp'
RESERVED_KEYS = frozenset(['key', 'timestamp', 'expiration_timestamp', GSI_PK, GSI_SORT])
_APP_KEY_PREFIX = '{app_identifier},'
_GSI_PARTITIONS = 10
_CACHE_DELAY = 300  # Don't store anything in the cache newer than 5 minutes
MetricsValuesDict = DefaultDict[str, List[Tuple[int, float]]]


class CacheEntry:  # pragma: no cover
    def __init__(self, time_start: int = 0, data: Optional[MetricsValuesDict] = None):
        self.time_start: int = time_start
        self.data: MetricsValuesDict = data or defaultdict(list)

    def __eq__(self, other):
        return (self.time_start == other.time_start) and (self.data == other.data)

    def __repr__(self):
        return '<{start}, {data}>'.format(start=self.time_start, data=self.data)


class ClustermanMetricsBotoClient(object):
    """
    Client for interacting with Clusterman metrics directly through the AWS boto API.
    """

    def __init__(
        self,
        region_name: str,
        app_identifier: Optional[str] = None,
        ttl_days: Optional[int] = None,
    ) -> None:
        """
        :param region_name: name of AWS region to use instead of the default.
        :param app_identifier: prefix for all application metric names.
            Required from client applications to avoid name collisions.
        :param ttl_days: number of days after which data written by this client should expire.
            Use -1 if data should never expire, and leave as None to use the default value.
        """
        self.region_name = region_name
        ttl_days = ttl_days or staticconf.read_int('dynamodb.ttl_days', namespace=CONFIG_NAMESPACE)

        self.ddb = get_metrics_session().resource(
            'dynamodb',
            region_name=self.region_name,
        )
        self.app_identifier = app_identifier
        if ttl_days == -1:
            # Never expire
            self.ttl_seconds = None
        else:
            self.ttl_seconds = int(timedelta(days=ttl_days).total_seconds())

        self._cache: MutableMapping[str, CacheEntry] = defaultdict(CacheEntry)

    @contextmanager
    def get_writer(self, metric_type: str):
        """ Returns a co-routine for writing metrics data.

        :param metric_type: string, must be one of :ref:`Metric Types`
        :raises: ``ValueError`` if the metric type is not one of :ref:`Metric Types`

        To write data timeseries data with the co-routine, call ``send`` with tuples of the form ``(metric_name,
        timestamp, value)``, where

          - ``metric_name`` is a string representing the timeseries name
          - ``timestamp`` is an integer representing epoch seconds
          - ``value`` is a numeric value of the timeseries at timestamp (optionally, this can be a dictionary with
            multiple datapoints at a single timestamp)

        For example::

            with get_writer(...) as writer:
                writer.send(('metric_a', 1501872445, 3))

        """
        key_prefix = self._get_key_prefix(metric_type)
        table_name = self._get_table_name(metric_type)
        table = self.ddb.Table(table_name)

        def write_batch(self):
            with table.batch_writer() as batch:
                while True:
                    (metric_name, timestamp, value) = (yield)

                    metric_key = key_prefix + metric_name
                    timestamp = int(timestamp)
                    # these keys should be included in RESERVED_KEYS
                    item = {
                        'key': metric_key,
                        'timestamp': timestamp,
                    }

                    # We only support regex metric requirements for application metrics, so
                    # don't pollute the other tables with useless keys
                    if metric_type == APP_METRICS:
                        item.update({
                            GSI_PK: random.randrange(_GSI_PARTITIONS),
                            GSI_SORT: key_prefix + str(timestamp),
                        })
                    if self.ttl_seconds is not None:
                        item['expiration_timestamp'] = timestamp + self.ttl_seconds

                    # Only numeric values (int, float, Decimal, or numeric string) are valid,
                    # or a dict of numeric values for metadata.
                    # Convert to Decimal because DynamoDB only accepts Decimal instead of floats,
                    # and meteorite doesn't like numeric strings.
                    #
                    # TODO (CLUSTERMAN-115) split out the dictionary writer to a separate function
                    if metric_type == METADATA and isinstance(value, dict):
                        for val_key, val_num in value.items():
                            if val_key not in RESERVED_KEYS:
                                item[val_key] = convert_decimal(val_num)
                            else:
                                logger.warning(
                                    'Column "{name}" is reserved; skipping "{name}" with value '
                                    '"{val}" in metric "{key}" at time {ts}'.format(
                                        name=val_key,
                                        val=val_num,
                                        key=metric_key,
                                        ts=timestamp,
                                    )
                                )
                    else:
                        value = convert_decimal(value)
                        item['value'] = value

                    batch.put_item(Item=item)

        coroutine = write_batch(self)
        try:
            next(coroutine)
            yield coroutine
        finally:
            coroutine.close()

    def get_metric_values(
        self,
        metric_query: str,
        metric_type: str,
        time_start: int,
        time_end: int,
        is_regex: bool = False,
        check_deprecated: bool = True,
        use_cache: bool = True,
        extra_dimensions: Optional[Dict[str, str]] = None,
        app_identifier: Optional[str] = None,
    ) -> MetricsValuesDict:
        """Returns the values over a time range for some metric.

        :param metric_query: the name of the metric to get values for.
        :param metric_type: must be one of :ref:`Metric Types`. Type of metric_name
        :param time_start: start of the time range of values to return (Unix timestamp)
        :param time_end: end (inclusive) of the time range of values to return (Unix timestamp)
        :param is_regex: if True, interpret the metric_query as a regular expression against which keys can match
        :param check_deprecated: if True, check to see if there's any data using deprecated key names
        :param use_cache: if True, read and store metrics to a local cache
        :param extra_dimensions: a dictionary of extra dimensions to include in the query but that will not be
            present in the return value
        :param app_identifier: a string identifying the app to get metrics for (overrides self.app_identifier if set)
        :returns: a mapping from metric name -> timeseries data; the metric name does _not_ include the app_identifier
            or any dimensions specified in ``extra_dimensions``
        :raises: ``ValueError`` if the metric type is not one of :ref:`Metric Types` or if application metrics are
            requested with no app identifier
        """
        if metric_type != APP_METRICS:
            if is_regex:
                raise ValueError('regex expressions are not supported for {type}'.format(type=metric_type))
            app_identifier = ''
        else:
            app_identifier = app_identifier or self.app_identifier
            assert app_identifier  # make mypy happy
        key_prefix = self._get_key_prefix(metric_type, app_identifier)
        query_with_dims = key_prefix + generate_key_with_dimensions(metric_query, extra_dimensions)

        cached_data: MetricsValuesDict = defaultdict(list)
        if use_cache:
            cached_data, time_start, time_end = self._cache_lookup(query_with_dims, time_start, time_end)

        new_data: MetricsValuesDict = defaultdict(list)
        if time_start < time_end:  # Skip querying dynamodb if all the metrics were in the cache
            new_data = self._get_new_metric_values(
                key_prefix,
                metric_query,
                metric_type,
                time_start,
                time_end,
                is_regex,
                extra_dimensions,
            )

        if new_data and use_cache:
            self._cache_store(query_with_dims, time_start, new_data)

        data: MetricsValuesDict = defaultdict(list)
        for metric_key in set(cached_data) | set(new_data):
            ts = cached_data[metric_key] + new_data[metric_key]
            metric_key_without_app = metric_key[len(app_identifier) + 1:] if metric_type == APP_METRICS else metric_key
            data[metric_key_without_app] = ts
        return data

    def _get_new_metric_values(
        self,
        key_prefix: str,
        metric_query: str,
        metric_type: str,
        time_start: int,
        time_end: int,
        is_regex: bool = False,
        extra_dimensions: Optional[Dict[str, str]] = None,
    ) -> MetricsValuesDict:
        """
        Query the backend datastore to get any new metrics values

        Override this method in any subclasses to take advantage of pre-/post-processing of the return value,
        as well as the metrics cache.

        Arguments/return value are the same as for `get_metrics_values`
        """
        logger.info('Querying datastore for {key} between {start} and {end} (is_regex={is_regex})'.format(
            key=metric_query, start=time_start, end=time_end, is_regex=is_regex,
        ))
        new_data: MetricsValuesDict = defaultdict(list)
        table_name = self._get_table_name(metric_type)
        table = self.ddb.Table(table_name)

        if is_regex:
            metric_keys = self._get_keys_from_query(key_prefix, metric_query, time_start, time_end, table)
        else:
            metric_keys = {key_prefix + metric_query}
        time_start = convert_decimal(time_start)
        time_end = convert_decimal(time_end)

        # keys come back from self._get_keys_from_query with key_prefix prepended so we don't have to do it here
        for metric_key in metric_keys:
            full_query_key = generate_key_with_dimensions(metric_key, extra_dimensions)
            query_condition = Key('key').eq(full_query_key) & Key('timestamp').between(time_start, time_end)
            response = table.query(KeyConditionExpression=query_condition)

            new_data[metric_key] = self._extract_timestamp_and_value_from_items(response['Items'])

            # Results are possibly paginated if too large.
            while response.get('LastEvaluatedKey') is not None:
                response = table.query(
                    ExclusiveStartKey=response.get('LastEvaluatedKey'),
                    KeyConditionExpression=query_condition,
                )
                new_data[metric_key].extend(self._extract_timestamp_and_value_from_items(response['Items']))

        return new_data

    def _get_table_name(self, metric_type):
        if metric_type not in METRIC_TYPES:
            raise ValueError('Metric type must be one of {types}'.format(types=METRIC_TYPES))

        return '{prefix}_{metric_type}'.format(prefix=CLUSTERMAN_NAME, metric_type=metric_type)

    def _get_key_prefix(self, metric_type: str, app_identifier: Optional[str] = None) -> str:
        app_identifier = app_identifier or self.app_identifier
        if metric_type == APP_METRICS:
            if not app_identifier:
                raise ValueError('app_identifier must be set for APP_METRICS')
            return _APP_KEY_PREFIX.format(app_identifier=app_identifier)
        else:
            return ''

    def _extract_timestamp_and_value_from_items(self, items):
        try:
            res = [(item['timestamp'], item['value']) for item in items]
        except KeyError:
            res = [
                (item['timestamp'], {k: v for k, v in item.items() if k not in RESERVED_KEYS})
                for item in items
            ]
        return res

    def _get_keys_from_query(
        self,
        key_prefix: str,
        metric_query: str,
        time_start: int,
        time_end: int,
        table: 'boto3.dynamodb.Table',
    ) -> Set[str]:
        """ Query the global secondary index for any keys matching the metric query """
        metric_keys: Set[str] = set()

        for i in range(_GSI_PARTITIONS):
            query_condition = Key(GSI_PK).eq(i) & Key(GSI_SORT).between(
                key_prefix + str(time_start),
                key_prefix + str(time_end),
            )
            response = table.query(
                IndexName=GSI_NAME,
                KeyConditionExpression=query_condition,
            )
            metric_keys |= {
                item['key'] for item in response['Items']
                if re.search(metric_query, item['key'])
            }

        return metric_keys

    def _cache_lookup(self, metric_query: str, time_start: int, time_end: int) -> Tuple[MetricsValuesDict, int, int]:
        """ Look up an range of items in the locally-stored metrics cache

        The cache is stored as a mapping of dictionary -> timeseries lists (i.e., a sorted list of tuples [(time1,
        value1), (time2, value2), ...]), indexed by a metrics query.

        Note that we assume that writes to the metric store happen sequentially.  In other words, we aren't going
        back to fill in values "after the fact", because this could result in a cache with incomplete entries.  There
        *could* be cases where writes to the datastore haven't completed when we read the cache, but this will only
        result in incomplete information if write 1 finishes after write 2, but write 1 has an earlier timestamp.  To
        protect against this case, we don't store data that's newer than 5 minutes (see self._cache_store, below).

        :param metric_query: the name of the metric
        :param time_start: int, unix timestamp of first requested time to look up
        :param time_end: int, unix timestamp of last requested time to look up
        :returns: (cached_data, new_start, new_end) tuple -- [new_start, new_end] is the range of timestamps we need
            to look up in the datastore
        """
        centry = self._cache[metric_query]
        cache_size = len(centry.data)

        if time_start >= time_end:
            raise ValueError('time_start must be earlier than time_end')
        elif cache_size == 0:
            return defaultdict(list), time_start, time_end
        elif time_start < centry.time_start:
            # If the start time is before the beginning of the cache, we return nothing, since otherwise we'd
            # need more complicated logic to know what to fetch from the datastore.  The cache will then get
            # invalidated when we try to store the data back in.
            #
            # In normal circumstances this should never happen because we assume the requests are monotonically
            # increasing in time_start
            logger.warning('time_start was before the first cached value; this should not happen ({ts} < {c})'.format(
                ts=time_start,
                c=centry.time_start,
            ))
            return defaultdict(list), time_start, time_end
        else:
            cached_metrics: MetricsValuesDict = defaultdict(list)
            min_time, max_time = float('inf'), 0
            for metric_name, ts in centry.data.items():
                # using -inf and inf here ensures that cache_start and cache_end are inclusive
                cache_start = bisect(ts, (time_start, float('-inf')))
                cache_end = bisect(ts, (time_end, float('inf')))
                if len(ts) > 0 and cache_start < len(ts):
                    cached_metrics[metric_name] = ts[cache_start:cache_end]
                    min_time = min(ts[cache_start][0], min_time)
                    max_time = max(ts[cache_end - 1][0], max_time)

                # Prune off everything before the original time_start request to keep the cache small-ish
                num_cache_entries_to_prune = len(centry.data[metric_name][:cache_start])
                if num_cache_entries_to_prune > 0:
                    logger.info('Removing {num} cache entries before {t} for {key}'.format(
                        num=num_cache_entries_to_prune,
                        t=time_start,
                        key=metric_name,
                    ))
                    del centry.data[metric_name][:cache_start]

            centry.data = defaultdict(list, {metric_name: ts for metric_name, ts in centry.data.items() if ts})

            if cached_metrics:
                logger.info('Using cached metrics from {start} to {end}'.format(
                    start=min_time,
                    end=max_time,
                ))

                # The first time we need to look up in the datastore is 1 second after the last thing
                # we retreived from the cache
                next_uncached_time = max_time + 1
            else:
                logger.info('No cached metrics found in range {start} to {end} for {query}'.format(
                    start=time_start,
                    end=time_end,
                    query=metric_query,
                ))
                next_uncached_time = time_start

            centry.time_start = time_start
            return cached_metrics, next_uncached_time, time_end

    def _cache_store(self, metric_query: str, time_start: int, data: MetricsValuesDict) -> None:
        """ Store items in the metrics cache; as discussed above, items newer than "five minutes ago" are not stored

        :param metric_query: the metric query to index data by
        :param items: mapping from metric name -> timeseries data, corresponding to the given query
        """
        centry = self._cache[metric_query]
        cache_size = len(centry.data)
        if cache_size == 0:
            centry.time_start = time_start

        min_time, max_time = float('inf'), 0
        five_minutes_ago = time.time() - _CACHE_DELAY
        should_invalidate = False
        stored_data: MetricsValuesDict = defaultdict(list)
        for metric_name, ts in data.items():
            cache_end = bisect(ts, (five_minutes_ago, float('-inf')))  # Don't cache the element exactly 5 minutes ago
            stored_data[metric_name] = ts[:cache_end]

            if ts:
                min_time = min(ts[0][0], min_time)
                max_time = max(ts[cache_end - 1][0], max_time)

                if len(centry.data[metric_name]) > 0 and ts[0][0] <= centry.data[metric_name][-1][0]:
                    logger.warning('First new item time {first} is before the last cache entry {last}; '.format(
                        first=ts[0][0],
                        last=centry.data[metric_name][-1][0],
                    ) + 'invalidating the cache')
                    should_invalidate = True

        if should_invalidate:
            self._cache[metric_query] = CacheEntry(time_start, stored_data)
        elif min_time <= max_time:
            logger.info('Caching entries for {key} between {s} and {t}'.format(
                key=metric_query,
                s=min_time,
                t=max_time,
            ))
            for metric_name, ts in stored_data.items():
                centry.data[metric_name] += ts
