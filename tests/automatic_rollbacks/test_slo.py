from unittest import mock

from paasta_tools.automatic_rollbacks.slo import SLODemultiplexer
from paasta_tools.automatic_rollbacks.slo import SLOSFXWatcher
from paasta_tools.automatic_rollbacks.slo import watch_slos_for_service


def test_SLOSFXWatcher_window_trimming():
    watcher = SLOSFXWatcher(
        slo=mock.Mock(config=mock.Mock(duration="300s", threshold=1.0, percent_of_duration=50.0)),
        callback=mock.Mock(),
        start_timestamp=100.0,
        label="fake",
        max_duration=50,
    )

    assert watcher.window_duration() == 50

    for timestamp in range(0, 100, 2):  # from 0 to 198
        watcher.process_datapoint(
            props=mock.Mock(),
            datapoint=0.0,
            timestamp=timestamp,
        )

    earliest_ts, _ = watcher.window[0]
    latest_ts, _ = watcher.window[-1]
    assert latest_ts - earliest_ts <= watcher.window_duration()


def test_SLOSFXWatcher_alerting():
    callback = mock.Mock()
    watcher = SLOSFXWatcher(
        slo=mock.Mock(
            config=mock.Mock(
                duration="300s",
                threshold=1.0,
                percent_of_duration=50.0,
            ),
        ),
        callback=callback,
        start_timestamp=100.0,
        label="fake",
        max_duration=50,
    )

    good = 0.0
    bad = 2.0

    watcher.process_datapoint(props=None, datapoint=bad, timestamp=50.0)
    assert watcher.bad_before_mark is True
    assert watcher.bad_after_mark is None

    watcher.process_datapoint(props=None, datapoint=good, timestamp=51.0)
    watcher.process_datapoint(props=None, datapoint=good, timestamp=52.0)
    assert watcher.bad_before_mark is False
    assert watcher.bad_after_mark is None

    assert callback.call_count == 0

    # these are new enough that it should push the old data out of the window
    watcher.process_datapoint(props=None, datapoint=bad, timestamp=103.0)
    watcher.process_datapoint(props=None, datapoint=bad, timestamp=104.0)
    assert watcher.bad_before_mark is False
    assert watcher.bad_after_mark is True
    callback.assert_called_once_with(watcher)
    callback.reset_mock()

    watcher.process_datapoint(props=None, datapoint=good, timestamp=105.0)
    watcher.process_datapoint(props=None, datapoint=good, timestamp=106.0)
    watcher.process_datapoint(props=None, datapoint=good, timestamp=107.0)
    callback.assert_called_once_with(watcher)


def test_SLODemultiplexer():
    slo_config = mock.Mock(duration='300s', threshold=1.0, percent_of_duration=50.0)
    good = 0.0
    bad = 2.0

    sink = mock.Mock(
        source=mock.Mock(slos=[
            mock.Mock(label='slo_1', config=slo_config),
            mock.Mock(label='slo_2', config=slo_config),
            mock.Mock(label='slo_3', config=slo_config),
        ]),
        _get_detector_label=lambda slo: slo.label,
    )
    individual_slo_callback = mock.Mock()

    demux = SLODemultiplexer(
        sink=sink,
        individual_slo_callback=individual_slo_callback,
        start_timestamp=100.0,
    )

    for label in demux.slo_watchers_by_label.keys():
        demux.slo_watchers_by_label[label] = mock.Mock(wraps=demux.slo_watchers_by_label[label])

    demux.process_datapoint(props={'dimensions': {'sf_metric': 'slo_1.0'}}, datapoint=good, timestamp=0.0)
    assert demux.slo_watchers_by_label['slo_1'].process_datapoint.call_count == 1
    demux.slo_watchers_by_label['slo_1'].process_datapoint.reset_mock()
    assert demux.slo_watchers_by_label['slo_2'].process_datapoint.call_count == 0
    assert demux.slo_watchers_by_label['slo_3'].process_datapoint.call_count == 0

    demux.process_datapoint(props={'dimensions': {'sf_metric': 'slo_2.0'}}, datapoint=good, timestamp=0.0)
    assert demux.slo_watchers_by_label['slo_1'].process_datapoint.call_count == 0
    assert demux.slo_watchers_by_label['slo_2'].process_datapoint.call_count == 1
    demux.slo_watchers_by_label['slo_2'].process_datapoint.reset_mock()
    assert demux.slo_watchers_by_label['slo_3'].process_datapoint.call_count == 0

    demux.process_datapoint(props={'dimensions': {'sf_metric': 'slo_3.0'}}, datapoint=bad, timestamp=0.0)
    assert demux.slo_watchers_by_label['slo_1'].process_datapoint.call_count == 0
    assert demux.slo_watchers_by_label['slo_2'].process_datapoint.call_count == 0
    assert demux.slo_watchers_by_label['slo_3'].process_datapoint.call_count == 1
    demux.slo_watchers_by_label['slo_3'].process_datapoint.reset_mock()

    assert individual_slo_callback.call_count == 0


def test_watch_slos_for_service_alerting():
    slo_config = mock.Mock(duration='300s', threshold=1.0, percent_of_duration=50.0)
    good = 0.0
    bad = 2.0

    sink = mock.Mock(
        source=mock.Mock(slos=[
            mock.Mock(label='slo_1', config=slo_config),
            mock.Mock(label='slo_2', config=slo_config),
            mock.Mock(label='slo_3', config=slo_config),
        ]),
        _get_detector_label=lambda slo: slo.label,
    )
    individual_slo_callback = mock.Mock()
    all_slos_callback = mock.Mock()

    with mock.patch(
        'paasta_tools.automatic_rollbacks.slo.get_slos_for_service',
        return_value=(((sink), ("fake query")),),
        autospec=True,
    ), mock.patch(
        'paasta_tools.automatic_rollbacks.slo.tail_signalfx',
        autospec=True,
    ):
        threads, watchers = watch_slos_for_service(
            service='service',
            individual_slo_callback=individual_slo_callback,
            all_slos_callback=all_slos_callback,
            sfx_api_token='fake',
            start_timestamp=100.0,
        )

        for watcher in watchers:
            watcher.process_datapoint(
                props={'not': 'used'},
                datapoint={
                    'slo_1': good,
                    'slo_2': good,
                    'slo_3': bad,
                }[watcher.label],
                timestamp=0.0,
            )
            watcher.process_datapoint(props={'not': 'used'}, datapoint=bad, timestamp=200.0)

        assert individual_slo_callback.call_count == 2
        individual_slo_callback.assert_any_call('slo_1', True)
        individual_slo_callback.assert_any_call('slo_2', True)
        # not slo_3 because it was bad before start_timestamp.
        assert all_slos_callback.call_count == 1
