import json
import time
import traceback

import boto3


class RecordFailedStagingException(Exception):
    pass


sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')


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
    :raises RecordFailedStagingException: On any error or exception
    '''
    try:
        return record_failed_staging(event, context)
    except RecordFailedStagingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RecordFailedStagingException(e)


def record_failed_staging(event, context):
    '''
    record_failed_staging Records the failed staging in the data catalog
    and raises an SNS notification.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    record_failed_staging_in_data_catalog(event, context)
    send_failed_staging_sns(event, context)
    return event


def record_failed_staging_in_data_catalog(event, context):
    '''
    record_failed_staging_in_data_catalog Records the failed staging
    in the data catalog.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    raw_key = event['fileDetails']['key']
    raw_bucket = event['fileDetails']['bucket']
    error = event['error-info']['Error']
    error_cause = json.loads(event['error-info']['Cause'])
    staging_execution_name = event['fileDetails']['stagingExecutionName']

    # Remove the stack trace as its inconsistency
    # confuses elasticsearch autoindexing
    if 'stackTrace' in error_cause:
        del error_cause['stackTrace']

    data_catalog_table = event["settings"]["dataCatalogTableName"]

    dynamodb_item = {
        'rawKey': raw_key,
        'catalogTime': int(time.time() * 1000),
        'rawBucket': raw_bucket,
        'error': error,
        'errorCause': error_cause,
        'stagingExecutionName': staging_execution_name
    }

    # The values present in the event will depend on how far this file
    # progressed through staging before it failed.
    if 'fileType' in event:
        dynamodb_item['fileType'] = event['fileType']
    if 'contentLength' in event['fileDetails']:
        dynamodb_item['contentLength'] = \
            event['fileDetails']['contentLength']
    if 'fileSettings' in event:
        if 'stagingPartitionSettings' in event['fileSettings']:
            dynamodb_item['stagingPartitionSettings'] = \
                event['fileSettings']['stagingPartitionSettings']
    if 'stagingBucket' in event['settings']:
        dynamodb_item['stagingBucket'] = \
            event['settings']['stagingBucket']

    dynamodb_table = dynamodb.Table(data_catalog_table)
    dynamodb_table.put_item(Item=dynamodb_item)


def send_failed_staging_sns(event, context):
    '''
    send_failed_staging_sns Sends an SNS notifying subscribers
    that staging failed.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    raw_key = event['fileDetails']['key']
    raw_bucket = event['fileDetails']['bucket']
    error = event['error-info']['Error']
    error_cause = json.loads(event['error-info']['Cause'])

    subject = 'Data Lake - ingressed file staging failed'
    message = 'File:{} in Bucket:{} failed staging with '\
        'error:{} and cause:{}' \
        .format(raw_key, raw_bucket, error, error_cause)

    if 'fileSettings' in event \
            and 'failureSNSTopicARN' in event['fileSettings']:
        failed_sns_topic_arn = event['fileSettings']['failureSNSTopicARN']
        send_sns(failed_sns_topic_arn, subject, message)

    if 'settings' in event \
            and 'defaultSNSErrorArn' in event['settings']:
        default_sns_error_arn = event['settings']['defaultSNSErrorArn']
        send_sns(default_sns_error_arn, subject, message)


def send_sns(topic_arn, subject, message):
    '''
    send_sns Sends an SNS with the given subject and message to the
    specified ARN.

    :param topic_arn: The SNS ARN to send the notification to
    :type topic_arn: Python String
    :param subject: The subject of the SNS notification
    :type subject: Python String
    :param message: The SNS notification message
    :type message: Python String
    '''
    print('Sending failure SNS with subject:{} to topic:{}'
          .format(subject, topic_arn))
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
