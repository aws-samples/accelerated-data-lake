import time
import traceback

import boto3


class RecordSuccessfulStagingException(Exception):
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
    :raises RecordSuccessfulStagingException: On any error or exception
    '''
    try:
        return record_successfull_staging(event, context)
    except RecordSuccessfulStagingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulStagingException(e)


def record_successfull_staging(event, context):
    """
    record_successfull_staging Records the successful staging in the data
    catalog and raises an SNS notification.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    record_successful_staging_in_data_catalog(event, context)
    send_successful_staging_sns(event, context)
    return event


def record_successful_staging_in_data_catalog(event, context):
    '''
    record_successful_staging_in_data_catalog Records the successful staging
    in the data catalog.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    try:
        raw_key = event['fileDetails']['key']
        raw_bucket = event['fileDetails']['bucket']
        staging_key = event['fileDetails']['stagingKey']
        content_length = event['fileDetails']['contentLength']
        staging_execution_name = event['fileDetails']['stagingExecutionName']
        file_type = event["fileType"]

        staging_bucket = event['settings']['stagingBucket']
        data_catalog_table = event["settings"]["dataCatalogTableName"]

        tags = event['requiredTags']
        metadata = event['attachedMetadata']

        if 'stagingPartitionSettings' in event['fileSettings']:
            staging_partition_settings = \
                event['fileSettings']['stagingPartitionSettings']

        dynamodb_item = {
            'rawKey': raw_key,
            'catalogTime': int(time.time() * 1000),
            'rawBucket': raw_bucket,
            'stagingKey': staging_key,
            'stagingBucket': staging_bucket,
            'contentLength': content_length,
            'fileType': file_type,
            'stagingExecutionName': staging_execution_name,
            'stagingPartitionSettings': staging_partition_settings,
            'tags': tags,
            'metadata': metadata
        }
        dynamodb_table = dynamodb.Table(data_catalog_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulStagingException(e)


def send_successful_staging_sns(event, context):
    '''
    send_successful_staging_sns Sends an SNS notifying subscribers
    that staging was successful.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    raw_key = event['fileDetails']['key']
    raw_bucket = event['fileDetails']['bucket']
    file_type = event['fileType']

    subject = 'Data Lake - ingressed file staging success'
    message = 'File:{} in Bucket:{} for DataSource:{} successfully staged' \
        .format(raw_key, raw_bucket, file_type)

    if 'fileSettings' in event \
            and 'successSNSTopicARN' in event['fileSettings']:
        successSNSTopicARN = event['fileSettings']['successSNSTopicARN']
        send_sns(successSNSTopicARN, subject, message)


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
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
