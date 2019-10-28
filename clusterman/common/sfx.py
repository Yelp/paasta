import colorlog
import signalfx
from arrow import Arrow
from signalfx.signalflow.messages import DataMessage


logger = colorlog.getLogger(__name__)


TS_QUERY_PROGRAM_TEMPLATE = """data(
    "{metric}",
    filter={filters},
    extrapolation="{extrapolation}",
    maxExtrapolations={max_extrapolations},
    rollup={rollup}
){aggregation}.publish()
"""


class Aggregation:
    def __init__(self, method, by=None, over=None):
        self.method = method
        if by and over:
            raise ValueError(f'by and over cannot both be set: {by}, {over}')
        self.by = by
        self.over = over

    def __str__(self):
        if self.by:
            args = f'by={str(self.by)}'
        elif self.over:
            args = f'over={self.over}'
        else:
            args = ''
        return '{method}({args})'.format(
            method=self.method,
            args=args
        )

    def __eq__(self, other):
        return self.method == other.method and self.by == other.by and self.over == other.over


def _make_ts_label(raw_data, tsid, dimensions):
    """ Make a label for a timeseries data point returned from SignalFX

    :param raw_data: a processed data stream from SFX
    :param tsid: the timeseries ID for a datapoint in the SFX stream
    :param dimensions: a list of dimensions to create the label from
    :returns: a comma-separated list of the specified dimension values for this tsid
    """
    if not dimensions:
        return ''
    metadata = raw_data.get_metadata(tsid)
    return ','.join([metadata[dim] for dim in sorted(dimensions)])


def _make_filter_string(filters):
    """ Create a filter string used to modify a SignalFX query

    :param filters: a list of (filter_name, value) tuples
    :returns: a SignalForm filter string -- 'filter("filter_1", "value_1") and filter("filter_2", "value_2")'
    """
    if not filters:
        return 'None'

    fstring = ''
    for name, value in filters:
        fstring += f'filter("{name}", "{value}") and '
    return fstring[:-5]


def execute_sfx_program(api_token, program, start_time, end_time, dimensions=None, resolution=60):
    """ Execute an arbitrary SignalFlow program

    :param api_token: a valid SFX API query token (you can get this from the SignalFX dashboard)
    :param program: a valid signalflow program to execute
    :param start_time: beginning of program execution range, as an Arrow object
    :param end_time: end of program execution range, as an Arrow object
    :param dimensions: list of strings to group the returned timeseries by
    :param resolution: smallest time interval (in seconds) to evaluate the program on
        note: SignalFX has a maximum resolution of 1 minute, and only for the most recent data;
              setting a resolution higher than this (or even 1 minute for older data) will be ignored
    :returns: a list of (timestamp, data_points) tuples, where data_points is a dict of timeseries_name -> value
    """
    with signalfx.SignalFx().signalflow(api_token) as sfx:
        curr_time = start_time
        datapoints = []
        while curr_time < end_time:
            # To prevent overloading SignalFX we grab a maximum of 5 days worth of data at a time
            next_time = min(curr_time.shift(days=5), end_time)
            logger.info(f'Querying SignalFX from {curr_time} to {next_time}')
            raw_data = sfx.execute(
                program,

                # SignalFX operates on millisecond timescales
                start=curr_time.timestamp * 1000,
                stop=next_time.timestamp * 1000,
                resolution=resolution * 1000,
            )

            # We can only call _make_ts_label after all of the entries in the raw_data.stream() have been processed
            data_messages = [msg for msg in raw_data.stream() if isinstance(msg, DataMessage)]
            new_datapoints = sorted([(
                Arrow.utcfromtimestamp(msg.logical_timestamp_ms / 1000),
                {_make_ts_label(raw_data, key, dimensions): value for key, value in msg.data.items()}
            ) for msg in data_messages])

            # SignalFX sometimes gives us duplicate datapoints at the beginning of one chunk/the start of
            # the next chunk.  This doesn't play nicely with the metrics client so detect and remove those here
            if datapoints and new_datapoints[0][0] == datapoints[-1][0]:
                new_datapoints = new_datapoints[1:]
            datapoints.extend(new_datapoints)

            curr_time = next_time
    return datapoints


def basic_sfx_query(api_token, metric, start_time, end_time,
                    rollup='average',
                    extrapolation='null',
                    max_extrapolations=0,
                    filters=None,
                    resolution=60,
                    aggregation=Aggregation('sum')
                    ):
    """ Run the simplest of all SignalFX queries: specify a metric name to query and (optionally) some filters, and sum
    the results into a single timeseries.

    :param api_token: a valid SFX API query token (you can get this from the SignalFX dashboard)
    :param metric: name of the metric to query
    :param start_time: beginning of program execution range, as an Arrow object
    :param end_time: end of program execution range, as an Arrow object
    :param rollup: a valid SignalFX rollup string, or None for the default
    :param extrapolation: one of 'null', 'zero', or 'last_value'
    :param max_extrapolations: how many times to apply the extrapolation policy
    :param filters: a list of (filter_name, filter_value) tuples
    :param resolution: smallest time interval (in seconds) to evaluate the program on
        note: SignalFX has a maximum resolution of 1 minute, and only for the most recent data;
              setting a resolution higher than this (or even 1 minute for older data) will be ignored
    :param aggregation: an Aggregation object describing how to group the results
    :returns: a list of (timestamp, value) tuples
    """
    rollup = f'"{rollup}"' if rollup else 'None'
    agg_string = f'.{aggregation}' if aggregation else ''
    program = TS_QUERY_PROGRAM_TEMPLATE.format(
        metric=metric,
        filters=_make_filter_string(filters),
        rollup=rollup,
        extrapolation=extrapolation,
        max_extrapolations=max_extrapolations,
        aggregation=agg_string,
    )
    return execute_sfx_program(
        api_token,
        program,
        start_time,
        end_time,
        resolution=resolution,
        dimensions=(aggregation.by if aggregation else [])
    )
