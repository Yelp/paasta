import boto3
session = boto3.Session(profile_name='dev-read-only')
client = session.client('s3')
client.list_buckets()
