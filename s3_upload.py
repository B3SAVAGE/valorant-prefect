import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from prefect import task

@task
def upload(file):
    access_key = os.environ.get('AWS_ACCESS_KEY')
    secret_key = os.environ.get('AWS_SECRET_KEY')

    # Set the S3 bucket name and the file you want to upload
    bucket_name = "valorant-bucket"
    file_name = "agents.parquet"
    object_name = "valorant.parquet"

    # Create an S3 client object

    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    try:
        s3.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
