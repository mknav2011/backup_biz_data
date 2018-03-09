import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('move-dataeng-s3catalog')

import boto.dynamodb
conn = boto.dynamodb.connect_to_region(
        'us-west-2',
        )

for el in conn.get_table('move-dataeng-s3catalog').scan():
    print el
