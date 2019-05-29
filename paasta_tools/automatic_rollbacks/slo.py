import functools
import os
import textwrap
import threading
import time
import traceback
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple

import pytimeparse
try:
    from slo_utils.yelpsoa_configs import get_slo_files_from_soaconfigs
    from slo_transcoder.composite_sinks.signalform_detectors import make_signalform_detector_composite_sink
    from slo_transcoder.sources.yelpsoaconfigs import YelpSoaConfigsSource
    SLO_TRANSCODER_LOADED = True
except ImportError:
    SLO_TRANSCODER_LOADED = False

import tempfile

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.automatic_rollbacks.signalfx import tail_signalfx


def get_slos_for_service(service, soa_dir=DEFAULT_SOA_DIR) -> Generator:
    if not SLO_TRANSCODER_LOADED:
        return

    slo_files = [f for f in get_slo_files_from_soaconfigs(soa_dir) if f.startswith(os.path.join(soa_dir, service))]
    with tempfile.TemporaryDirectory() as temp_dir:
        SFDCS = make_signalform_detector_composite_sink(output_directory=temp_dir)
        sources = [YelpSoaConfigsSource(file) for file in slo_files]
        if not sources:
            return   # SFDCS instantiation crashes with IndexError if sources is an empty list.
        composite_sink = SFDCS(service=service, sources=sources)

        for sink in composite_sink.sinks():
            signalflow, rules = sink.generate_signalflow_signals_and_rules()
            query = textwrap.dedent('\n'.join(signalflow))
            yield sink, query


class SLODemultiplexer:
    def __init__(
        self,
        sink: Any,
        individual_slo_callback: Callable[['SLOWatcher'], None],
        start_timestamp: Optional[float] = None,
        max_duration: float = 300,
    ) -> None:
        self.sink = sink
        self.individual_slo_callback = individual_slo_callback
        self.max_duration = max_duration
        if start_timestamp is None:
            self.start_timestamp = time.time()
        else:
            self.start_timestamp = start_timestamp

        self.slo_watchers_by_label: Dict[str, 'SLOWatcher'] = {}
        for slo in sink.slos:
            label = sink._get_detector_label(slo)
            watcher = SLOWatcher(slo, individual_slo_callback, self.start_timestamp, label, max_duration=max_duration)
            self.slo_watchers_by_label[label] = watcher

    def process_datapoint(self, props, datapoint, timestamp) -> None:
        slo_label = props['dimensions']['sf_metric'].rsplit('.', 1)[0]
        watcher = self.slo_watchers_by_label[slo_label]
        watcher.process_datapoint(props, datapoint, timestamp)


class SLOWatcher:
    def __init__(
        self,
        slo: Any,
        callback: Callable[['SLOWatcher'], Any],
        start_timestamp: float,
        label: str,
        max_duration: float = 300,
    ) -> None:
        self.slo = slo
        self.window: List[Tuple[float, float]] = []
        self.max_duration = max_duration
        self.failing: Optional[bool] = None
        self.bad_after_mark: Optional[bool] = None
        self.bad_before_mark: Optional[bool] = None
        self.callback = callback
        self.start_timestamp = start_timestamp
        self.label = label

    def process_datapoint(self, props, datapoint, timestamp) -> None:
        self.window.append((timestamp, datapoint))
        self.trim_window()

        if timestamp > self.start_timestamp:
            self.bad_after_mark = self.is_window_bad()
        else:
            self.bad_before_mark = self.is_window_bad()

        old_failing = self.failing
        self.failing = self.bad_after_mark and not self.bad_before_mark

        if self.failing == (not old_failing):
            self.callback(self)

    def trim_window(self) -> None:
        old_window = self.window
        min_ts = max(ts for ts, d in old_window) - self.window_duration()
        new_window = [(ts, d) for (ts, d) in old_window if ts > min_ts]
        self.window = new_window

    def window_duration(self) -> float:
        """Many of our SLOs are defined with durations of 1 hour or more; this is great if you're trying to avoid being
        paged, but not helpful for a deployment that you expect to finish within a few minutes. self.max_duration allows
        us to cap the length of time that we consider. This should make us a bit more sensitive."""
        return min(self.max_duration, pytimeparse.parse(str(self.slo.config.duration)))

    def is_window_bad(self) -> bool:
        bad_datapoints = len([1 for ts, d in self.window if d > self.slo.config.threshold])
        return (bad_datapoints / len(self.window)) >= 1. - self.slo.config.percent_of_duration / 100.


def print_exceptions_wrapper(fn):
    def inner(*args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception:
            traceback.print_exc()
            raise
    return inner


def watch_slos_for_service(
    service: str,
    individual_slo_callback: Callable[[str, bool], Any],
    all_slos_callback: Callable[[bool], Any],
    sfx_api_token: str,
    start_timestamp: Optional[float] = None,
) -> Tuple[List[threading.Thread], List[SLOWatcher]]:
    threads = []
    watchers: List[SLOWatcher] = []

    failing = False

    def callback_wrapper(watcher: 'SLOWatcher') -> None:
        nonlocal failing
        old_failing = failing
        new_failing = any(w.failing for w in watchers)
        individual_slo_callback(watcher.label, watcher.failing)

        failing = new_failing

        if new_failing == (not old_failing):
            all_slos_callback(new_failing)

    for sink, query in get_slos_for_service(service):
        demux = SLODemultiplexer(
            sink,
            individual_slo_callback=callback_wrapper,
            start_timestamp=start_timestamp,
        )
        thread = threading.Thread(
            target=print_exceptions_wrapper(functools.partial(
                tail_signalfx,
                query,
                lookback_seconds=30,
                callback=print_exceptions_wrapper(demux.process_datapoint),
                sfx_api_token=sfx_api_token,
            )),
            daemon=True,
        )
        threads.append(thread)
        thread.start()
        watchers.extend(demux.slo_watchers_by_label.values())

    return threads, watchers
