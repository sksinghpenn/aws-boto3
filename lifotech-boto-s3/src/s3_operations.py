import boto3

import json
import os
from boto3.s3.transfer import  TransferConfig

import threading
import sys




BUCKET_NAME = 'lifotech-learn-s3-operations'


def s3_client():
    s3 = boto3.client('s3')

    """ :type: pyboto3.s3 """
    

    return s3


def s3_resource():
    """ :type :pyboto3.s3 """
    return boto3.resource('s3')


def create_bucket(bucket_name):
    return s3_client().create_bucket(
        Bucket=bucket_name
    )


def create_bucket_policy(bucket_name):
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AddPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource":["arn:aws:s3:::" + bucket_name + "/*"]
            }
        ]
        
    }

    return json.dumps(bucket_policy)


def add_bucket_policy(bucket_policy, bucket_name):
    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=bucket_policy
    )


def list_buckets():
    return s3_client().list_buckets()


def get_bucket_policy(bucket_name):
    return s3_client().get_bucket_policy(Bucket=bucket_name)


def get_bucket_encryption(bucket_name):
    return s3_client().get_bucket_encryption(Bucket=bucket_name)


def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                'Resource': 'arn:aws:s3:::' + bucket_name + '/*'
            }
        ]
    }

    bucket_policy_string = json.dumps(bucket_policy)
    s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=bucket_policy_string
    )


def server_side_encryption_bucket(bucket_name):
    return s3_client().put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }

            ]
        }
    )


def delete_bucket(bucket_name):
    return s3_client().delete_bucket(Bucket=bucket_name)


def upload_small_file(bucket_name, file_path):
    return s3_client().upload_file(file_path, bucket_name, "readme.txt")


def upload_large_file(bucket_name):

    transfer_config = TransferConfig(multipart_threshold=1024 * 5, multipart_chunksize=1024 * 5,
                                     max_concurrency=10, use_threads=True)

    file_path = os.path.dirname(__file__) + '/BIG_DATA_ANALYTICS_WITH_JAVA.pdf'
    key_path = 'multipart_files/big_data_analytics_with_java.pdf'
    s3_resource().meta.client.upload_file(file_path, bucket_name, key_path,
                                          ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                                          Config=transfer_config,
                                          Callback=ProgressPercentage(file_path))
    

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = os.path.getsize(filename)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" %(
                    self._filename, self._seen_so_far, self._size, percentage
                )
            )

            sys.stdout.flush()


def read_object_from_bucket():
    object_key = 'readme.txt'
    return s3_client().get_object(Bucket=BUCKET_NAME, Key=object_key)


def version_bucket_files():
    return s3_client().put_bucket_versioning(Bucket=BUCKET_NAME, VersioningConfiguration={'Status': 'Enabled'})


def put_lifecycle_policy():

    lifecycle_policy = {
        "Rules": [
            {
                "ID": "Move readme file to Glacier",
                "Prefix": "readme",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2019-05-15T00:00:000Z",
                        "StorageClass": "GLACIER"
                    }
                ]


            },
            {
                "ID": "Move old version to glacier",
                "Status": "Enabled",
                "Prefix": "",
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 2,
                        "StorageClass": "GLACIER"
                    }
                ]
            }


        ]
    }

    s3_client().put_bucket_lifecycle_configuration(Bucket=BUCKET_NAME, LifecycleConfiguration=lifecycle_policy)


if __name__ == '__main__':
    # create_bucket_response = create_bucket(BUCKET_NAME)
    # print(create_bucket_response)

    #bucket_policy_response = create_bucket_policy(BUCKET_NAME)
    #add_bucket_policy(bucket_policy_response, BUCKET_NAME)
    # list_bucket_response = list_buckets()
    # print(list_bucket_response)
    
    # print(get_bucket_policy(BUCKET_NAME))

    #print(get_bucket_encryption(BUCKET_NAME))

    #update_bucket_policy(BUCKET_NAME)

    #encryption_response = server_side_encryption_bucket(BUCKET_NAME)

    #print(encryption_response)

    #delete_bucket_response = delete_bucket(BUCKET_NAME)

    #print(delete_bucket_response)

    #file_path_location = os.path.dirname(__file__) + '/readme.txt'
    #upload_small_file(BUCKET_NAME, file_path_location)

    #upload_large_file(BUCKET_NAME)

    # print(read_object_from_bucket())
    # print(version_bucket_files())

    put_lifecycle_policy()
