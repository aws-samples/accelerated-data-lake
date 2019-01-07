import time
import traceback

import boto3


class RecordSuccessfulStagingException(Exception):
    pass


sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    record_successful_staging_in_data_catalog(event, context)
    send_successful_staging_sns(event, context)
    return event


def record_successful_staging_in_data_catalog(event, context):
    try:
        raw_key = event['fileDetails']['key']
        raw_bucket = event['fileDetails']['bucket']
        staging_key = event['fileDetails']['stagingKey']
        content_length = event['fileDetails']['contentLength']
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
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
