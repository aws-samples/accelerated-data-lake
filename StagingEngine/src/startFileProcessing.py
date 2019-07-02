import json
import os
import random
import re
import string
import time
import traceback
import urllib
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


class StartFileProcessingException(Exception):
    pass


sns = boto3.client('sns')
sfn = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
s3_cache_table = os.environ['S3_CACHE_TABLE_NAME']
sns_failure_arn = os.environ['SNS_FAILURE_ARN']
state_machine_arn = os.environ['STEP_FUNCTION']


def lambda_handler(event, context):
    '''
    lambda_handler Top level lambda handler ensuring all exceptions
    are caught and logged.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    :raises StartFileProcessingException: On any error or exception
    '''
    try:
        return start_file_processing(event, context)
    except StartFileProcessingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise StartFileProcessingException(e)


def start_file_processing(event, context):
    '''
    start_file_processing Confirm the lambda context request id is
    not already being processed, check this is not just a folder being 
    created, then start file processing.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    if is_request_in_processing_cache(
            s3_cache_table, context.aws_request_id) is False:

        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

        if key.endswith('/') is False:
            start_step_function_for_file(bucket, key)
    else:
        print('Request id {} is already in processing cache'
              .format(context.aws_request_id))

    return event


def start_step_function_for_file(bucket, key):
    '''
    start_step_function_for_file Starts the data lake staging engine
    step function for this file.

    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    '''
    try:
        file_name = os.path.basename(key)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        keystring = re.sub('\W+', '_', key)  # Remove special chars
        step_function_name = timestamp + id_generator() + '_' + keystring

        step_function_name = step_function_name[:80]

        sfn_Input = {
            'fileDetails': {
                'bucket': bucket,
                'key': key,
                'fileName': file_name,
                'stagingExecutionName': step_function_name
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

        print('Started step function with input:{}'
              .format(step_function_input))
    except Exception as e:
            record_failure_to_start_step_function(
                bucket, key, e)
            raise


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    '''
    id_generator Creates a random id to add to the step function
    name - duplicate names will cause errors.

    :param size: The required length of the id, defaults to 6
    :param size: Python Integer, optional
    :param chars: Chars used to generate id, defaults to uppercase alpha+digits
    :param chars: Python String
    :return: The generated id
    :rtype: Python String
    '''
    return ''.join(random.choice(chars) for _ in range(size))


def is_request_in_processing_cache(s3_cache_table, request_id):
    '''
    is_request_in_processing_cache Checks that the request id is
    not already in the cache by doing a write that is conditional
    that the value does not already exist.

    :param s3_cache_table: The DynamnoDB table name for S3 caching
    :type s3_cache_table: Python String
    :param request_id: The request id
    :type request_id: Python String
    :return: True if the request is already in the prccessing cache
    :rtype: Python Boolean]
    '''
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
    '''
    send_failure_sns_message Sends an SNS notification alerting subscribers
    to the failure.
    
    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    '''
    message = \
        "An error occurred starting file processing for Bucket: {}, Key : {}".\
        format(bucket, key)

    sns.publish(
        TopicArn=sns_failure_arn,
        Subject='Error starting file processing',
        Message=message)


def record_failure_to_start_step_function(bucket, key, exception):
    '''
    record_failure_to_start_step_function Record failure to start the
    staging engine step function in the data catalog. Any exceptions
    raised by this method are caught.

    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    :param exception: The exception raised by the failure
    :type exception: Python Exception
    '''
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
