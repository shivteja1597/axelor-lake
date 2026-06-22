import boto3

s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='admin', aws_secret_access_key='password123', region_name='us-east-1')

response = s3.list_buckets()
for bucket in response['Buckets']:
    bucket_name = bucket['Name']
    print(f"Bucket: {bucket_name}")
    try:
        objs = s3.list_objects_v2(Bucket=bucket_name)
        for obj in objs.get('Contents', []):
            print(f"  {obj['Key']} ({obj['Size']} bytes)")
    except Exception as e:
        print(f"  Error listing objects: {e}")
