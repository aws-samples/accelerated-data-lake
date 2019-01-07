import json
import os
import random
import re
import string
import time
import traceback
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


class StartFileProcessing(Exception):
    pass


sns = boto3.client('sns')
sfn = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
s3_cache_table = os.environ['S3_CACHE_TABLE_NAME']
sns_failure_arn = os.environ['SNS_FAILURE_ARN']
state_machine_arn = os.environ['STEP_FUNCTION']


def lambda_handler(event, context):
    try:
        # Only proceed if the request id that triggered this lambda
        # is not already being processed. This protects us against
        # multiple lambdas processing the same request.
        if is_request_in_processing_cache(
                s3_cache_table, context.aws_request_id) is False:

            for record in event['Records']:
                # Get the s3 object details from the event
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']

                try:
                    file_name = os.path.basename(key)
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
                    keystring = re.sub('\W+', '_', key)  # Remove special chars
                    step_function_name = timestamp + id_generator() + \
                        '_' + keystring

                    step_function_name = step_function_name[:80]

                    sfn_Input = {
                        'fileDetails': {
                            'bucket': bucket,
                            'key': key,
                            'fileName': file_name
                        },
                        'settings': {
                            'dataSourceTableName':
                                os.environ['DATA_SOURCE_TABLE_NAME'],
                            'dataCatalogTableName':
                                os.environ['DATA_CATALOG_TABLE_NAME'],
                            'defaultSNSErrorArn':
                                os.environ['SNS_FAILURE_ARN'],
                            's3_cache_table':
                                os.environ['S3_CACHE_TABLE_NAME'],
                            'stagingBucket':
                                os.environ['STAGING_BUCKET_NAME'],
                            'failedBucket':
                                os.environ['FAILED_BUCKET_NAME']
                        }
                    }

                    # Start step function
                    step_function_input = json.dumps(sfn_Input)
                    sfn.start_execution(
                        stateMachineArn=state_machine_arn,
                        name=step_function_name, input=step_function_input)

                    print('Started step function with input:{}'.
                          format(step_function_input))
                except Exception as e:
                    record_failure_to_start_file_processing(
                        bucket, key, e)
                    raise
        else:
            print('Request id {} is already in processing cache'
                  .format(context.aws_request_id))
    except Exception as e:
        traceback.print_exc()
        send_failure_sns_message(bucket, key)
        raise StartFileProcessing(e)


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def is_request_in_processing_cache(s3_cache_table, request_id):
    try:
        # Add the request to the cache table, with
        # the condition that it's not already present
        ddb_cache_table = dynamodb.Table(s3_cache_table)
        ddb_cache_table.put_item(
            Item={'lastRequestId': request_id},
            ConditionExpression="attribute_not_exists(lastRequestId)")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return True
        else:
            raise(e)


def send_failure_sns_message(bucket, key):
    message = \
        "An error occurred starting file processing for Bucket: {}, Key : {}".\
        format(bucket, key)

    sns.publish(
        TopicArn=sns_failure_arn,
        Subject='Error starting file processing',
        Message=message)


def record_failure_to_start_file_processing(bucket, key, exception):
    try:
        data_catalog_table = os.environ['DATA_CATALOG_TABLE_NAME']

        dynamodb_item = {
            'rawKey': key,
            'catalogTime': int(time.time() * 1000),
            'rawBucket': bucket,
            'error': "Failed to start file processing",
            'errorCause': {
                'errorType': type(exception).__name__,
                'errorMessage': str(exception)
            }
        }

        dynamodb_table = dynamodb.Table(data_catalog_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception:
        traceback.print_exc()
