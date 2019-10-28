from argparse import ArgumentError
from argparse import Namespace

import mock
import pytest

from clusterman.cli.simulate import main


@pytest.fixture
def args():
    return Namespace(
        start_time='2018-01-01 00:00:00',
        end_time='2018-01-01 00:00:00',
        cluster='foo',
        pool='bar',
        cluster_config_dir='baz',
        metrics_data_files=None,
        simulation_result_file=None,
        comparison_operator='div',
        output_prefix='',
        join_delay_params=[0, 0],
        cpus_per_weight=10,
        ebs_volume_size=0,
    )


def test_main_too_many_compares(args):
    args.compare = ['sim1', 'sim2', 'sim3']
    with pytest.raises(ArgumentError):
        main(args)


@pytest.mark.parametrize('compare', [[], ['sim1'], ['sim1', 'sim2']])
def test_main_compare_param(compare, args):
    args.compare = compare
    with mock.patch('clusterman.cli.simulate.read_object_from_compressed_json') as mock_read, \
            mock.patch('clusterman.cli.simulate.write_object_to_compressed_json') as mock_write, \
            mock.patch('clusterman.cli.simulate._load_metrics') as mock_load_metrics, \
            mock.patch('clusterman.cli.simulate._run_simulation') as mock_run_simulation, \
            mock.patch('clusterman.cli.simulate.operator') as mock_operator, \
            mock.patch('clusterman.cli.simulate.make_report'):
        main(args)
        expected_call_count = 1 if len(compare) < 2 else 0
        assert mock_load_metrics.call_count == expected_call_count
        assert mock_run_simulation.call_count == expected_call_count
        assert mock_read.call_count == len(compare)
        assert mock_write.call_count == 0
        assert mock_operator.div.call_count == (len(compare) > 0)
