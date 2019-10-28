import arrow
import mock

from clusterman.common.sfx import Aggregation
from clusterman.tools.signalfx_scraper import get_parser
from clusterman.tools.signalfx_scraper import main


@mock.patch('clusterman.tools.signalfx_scraper.basic_sfx_query', autospec=True)
@mock.patch('clusterman.tools.signalfx_scraper.write_object_to_compressed_json', autospec=True)
@mock.patch('clusterman.tools.signalfx_scraper.ask_for_choice', autospec=True)
def test_main(mock_metric_choice, mock_write, mock_query):
    mock_metric_choice.side_effect = ['system_metrics', 'app_metrics']
    mock_query.side_effect = [['a1', 'a2'], ['b1', 'b2', 'b3']]

    parser = get_parser()
    args = parser.parse_args([
        '--start-time', '2017-10-01',
        '--end-time', '2017-10-01T12:00:00',
        '--src-metric-names', 'src.first.name', 'src.second.name',
        '--dest-file', 'destfile',
        '--api-token', 'token',
        '--filter', 'region:us-west-2a', 'cluster:releng',
    ])
    main(args)

    expected_start = arrow.get('2017-10-01').replace(tzinfo='US/Pacific')
    expected_end = expected_start.shift(hours=12)
    expected_filters = [['region', 'us-west-2a'], ['cluster', 'releng']]
    assert mock_query.call_args_list == [
        mock.call(
            'token',
            'src.first.name',
            expected_start, expected_end,
            filters=expected_filters,
            aggregation=Aggregation('sum', by=['AZ', 'inst_type']),
            extrapolation='last_value',
            max_extrapolations=3,
        ),
        mock.call(
            'token',
            'src.second.name',
            expected_start, expected_end,
            filters=expected_filters,
            aggregation=Aggregation('sum', by=['AZ', 'inst_type']),
            extrapolation='last_value',
            max_extrapolations=3,
        )
    ]

    expected_values = {
        'system_metrics': {'src.first.name': ['a1', 'a2']},
        'app_metrics': {'src.second.name': ['b1', 'b2', 'b3']},
    }
    assert mock_write.call_args_list == [
        mock.call(expected_values, 'destfile'),
    ]
