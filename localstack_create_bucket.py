import boto3

s3_client = boto3.client(
    's3',
    region_name='us-east-1',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummykey',
    aws_secret_access_key='dummysecret',
    use_ssl=False
) 

s3_client.create_bucket(Bucket='Hire_aquil_bucket')  