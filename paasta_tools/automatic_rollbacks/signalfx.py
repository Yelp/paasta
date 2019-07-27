import time
from typing import Callable
from typing import Dict
from typing import List

from signalfx import SignalFx
from signalfx.signalflow.messages import DataMessage
from signalfx.signalflow.messages import MetadataMessage


def _convert_sfx_timestamp(ts: int) -> float:
    """SignalFx uses millisecond int timestamps, we want floating point seconds"""
    return float(ts) / 1000


def tail_signalfx(
    query: str, lookback_seconds: float, callback: Callable, sfx_api_token: str
) -> None:
    if lookback_seconds > 0:
        start_timestamp_milliseconds = (time.time() - lookback_seconds) * 1000
    else:
        start_timestamp_milliseconds = None

    with SignalFx().signalflow(sfx_api_token) as flow:
        tsid_metadata_map = {}

        computation = flow.execute(program=query, start=start_timestamp_milliseconds)
        for msg in computation.stream():
            if isinstance(msg, MetadataMessage):
                # The MetadataMessage occurs before the associated DataMessage.
                # Extract out the important properties since a lot of it is not important.
                tsid, props = msg.tsid, msg.properties

                # in SignalFX, query looks like ... .publish('<stream_label>')
                stream_label: str = props.get("sf_streamLabel", tsid)

                # sf_key seems to have the list of dimensions with a couple extra stuff
                # The SignalFX query should minimally specify the 'service' dimension.
                # .sum(by=['dim1','dim2'])
                sf_key: List[str] = props.get("sf_key", [])

                dimensions: Dict[str, str] = {dim: props[dim] for dim in sf_key}
                tsid_metadata_map[tsid] = {
                    "stream_label": stream_label,
                    "dimensions": dimensions,
                }
            elif isinstance(msg, DataMessage):
                for tsid, datapoint in msg.data.items():
                    props = tsid_metadata_map[tsid]
                    callback(
                        props,
                        datapoint,
                        _convert_sfx_timestamp(msg.logical_timestamp_ms),
                    )
