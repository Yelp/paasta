#!/usr/bin/env python3
import argparse

from boto3 import session


def get_client(endpoint):
    s = session.Session(
        region_name='foo',
        aws_access_key_id='foo',
        aws_secret_access_key='bar',
    )
    client = s.client(
        service_name='dynamodb',
        endpoint_url=endpoint,
    )
    return client


def create_table(client, table_name):
    return client.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'task_id',
                'KeyType': 'HASH',
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE',
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'task_id',
                'AttributeType': 'S',
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'N',
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a dummy dynamodb table')
    parser.add_argument('endpoint', type=str, help='the dynamodb endpoint')
    parser.add_argument('table_name', type=str, help='the name of the table to create')
    args = parser.parse_args()
    create_table(get_client(args.endpoint), args.table_name)
