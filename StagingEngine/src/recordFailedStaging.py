import json
import time
import traceback

import boto3


class RecordFailedStagingException(Exception):
    pass


sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    record_failed_staging_in_data_catalog(event, context)
    send_failed_staging_sns(event, context)
    return event


def record_failed_staging_in_data_catalog(event, context):
    try:
        raw_key = event['fileDetails']['key']
        raw_bucket = event['fileDetails']['bucket']
        error = event['error-info']['Error']
        error_cause = json.loads(event['error-info']['Cause'])

        # Remove the stack trace as its inconsistency
        # confuses elasticsearch autoindexing
        del error_cause['stackTrace']

        data_catalog_table = event["settings"]["dataCatalogTableName"]

        dynamodb_item = {
            'rawKey': raw_key,
            'catalogTime': int(time.time() * 1000),
            'rawBucket': raw_bucket,
            'error': error,
            'errorCause': error_cause
        }

        # The values present in the event will depend on how far this file
        # progressed through staging before it failed.
        if 'fileType' in event:
            dynamodb_item['fileType'] = event['fileType']
        if 'contentLength' in event['fileDetails']:
            dynamodb_item['contentLength'] = \
                event['fileDetails']['contentLength']
        if 'stagingPartitionSettings' in event['fileSettings']:
            dynamodb_item['stagingPartitionSettings'] = \
                event['fileSettings']['stagingPartitionSettings']
        if 'stagingBucket' in event['settings']:
            dynamodb_item['stagingBucket'] = \
                event['settings']['stagingBucket']

        dynamodb_table = dynamodb.Table(data_catalog_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordFailedStagingException(e)


def send_failed_staging_sns(event, context):
    try:
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
    except Exception as e:
        traceback.print_exc()
        raise RecordFailedStagingException(e)


def send_sns(topic_arn, subject, message):
    print('Sending failure SNS with subject:{} to topic:{}'
          .format(subject, topic_arn))
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
