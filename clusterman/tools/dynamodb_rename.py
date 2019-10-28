import argparse
import sys
import time

from clusterman_metrics import METRIC_TYPES

from clusterman.args import add_env_config_path_arg
from clusterman.args import add_region_arg
from clusterman.aws.client import dynamodb
from clusterman.config import setup_config

BATCH_WRITE_SIZE = 25


# Print iterations progress
# Borrowed from https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = '{0:.' + str(decimals) + 'f}'
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def main(args):
    with open(args.mapping_file) as f:
        for line in f.readlines():
            old, new = line.split()
            table_name = f'clusterman_{args.metric_type}'
            query = dynamodb.get_paginator('query')
            print(f'Updating {old} to {new}')
            for page in query.paginate(
                    TableName=table_name,
                    KeyConditionExpression=f'#key = :val',
                    ExpressionAttributeNames={'#key': 'key'},
                    ExpressionAttributeValues={':val': {'S': old}}
            ):
                for item in page['Items']:
                    item['key']['S'] = new

                request_item_list = [{'PutRequest': {'Item': item}} for item in page['Items']] + \
                    [{'DeleteRequest': {'Key': {'key': {'S': old}, 'timestamp': item['timestamp']}}}
                        for item in page['Items']]
                for i in range(0, len(request_item_list), BATCH_WRITE_SIZE):
                    request_items = {
                        table_name: request_item_list[i:min(i + BATCH_WRITE_SIZE, len(request_item_list))]
                    }
                    print_progress(i, len(request_item_list), prefix='Page progress')
                    while request_items:
                        response = dynamodb.batch_write_item(RequestItems=request_items)
                        request_items = response.get('UnprocessedItems', {})
                        if request_items:
                            time.sleep(5)
                print('\n')


def parse_args():
    parser = argparse.ArgumentParser()
    add_env_config_path_arg(parser)
    add_region_arg(parser, required=True)
    parser.add_argument(
        '--metric-type',
        choices=list(METRIC_TYPES),
        required=True,
        help='The type of metric to rename',
    )
    parser.add_argument(
        '--mapping-file',
        required=True,
        help='A file containing a list of from -> two mappings to rename, one per line, separated by white space'
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    setup_config(args)
    main(args)
