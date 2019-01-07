import traceback

import boto3


class GetFileSettingsException(Exception):
    pass


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    attach_file_settings_to_event(event, context)
    attach_existing_metadata_to_event(event, context)
    return event


def attach_file_settings_to_event(event, context):
    table = event["settings"]["dataSourceTableName"]

    try:
        ddb_table = dynamodb.Table(table)
        # Get the item. There can only be one or zero - it is the table's
        # partition key - but use strong consistency so we respond instantly
        # to any change. This can be revisited if we want to conserve RCUs
        # by, say, caching this value and updating it every minute.
        response = ddb_table.get_item(
            Key={'fileType': event['fileType']}, ConsistentRead=True)
        item = response['Item']

        schema = item['schema'] \
            if 'schema' in item \
            else None
        event.update({'schema': schema})
        event.update({'fileSettings': item['fileSettings']})
        event.update({'requiredMetadata': item['metadata']})
        event.update({'requiredTags': item['tags']})
        return event

    except Exception as e:
        traceback.print_exc()
        raise GetFileSettingsException(e)


def attach_existing_metadata_to_event(event, context):
    try:
        file_header = s3.head_object(
            Bucket=event['fileDetails']['bucket'],
            Key=event['fileDetails']['key']
        )
        event.update({'existingMetadata': file_header['Metadata']})
        event['fileDetails'].update(
            {'contentLength': file_header['ContentLength']})

        return event

    except Exception as e:
        traceback.print_exc()
        raise GetFileSettingsException(e)
