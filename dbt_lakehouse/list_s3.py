import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='admin', aws_secret_access_key='password123', region_name='us-east-1')
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='lake-staging', Prefix='my_data/data'):
    for obj in page.get('Contents', []):
        print(obj['Key'])
