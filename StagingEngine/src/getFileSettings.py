import traceback

import boto3


class GetFileSettingsException(Exception):
    pass


s3 = boto3.client('s3')
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
    :raises GetFileSettingsException: On any error or exception
    '''
    try:
        return get_file_settings(event, context)
    except GetFileSettingsException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise GetFileSettingsException(e)


def get_file_settings(event, context):
    """
    get_file_settings Retrieves the settings for the new file in the
    data lake.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    attach_file_settings_to_event(event, context)
    attach_existing_metadata_to_event(event, context)
    return event


def attach_file_settings_to_event(event, context):
    '''
    attach_file_settings_to_event Attach the configured file settings
    to the lambda event.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    table = event["settings"]["dataSourceTableName"]
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


def attach_existing_metadata_to_event(event, context):
    '''
    attach_existing_metadata_to_event Attach the S3 object's
    current metadata to the lambda event. This is because we
    need to apply it later when we copy the object to avoid
    eventual consistency issues.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    file_header = s3.head_object(
        Bucket=event['fileDetails']['bucket'],
        Key=event['fileDetails']['key']
    )
    event.update({'existingMetadata': file_header['Metadata']})
    event['fileDetails'].update(
        {'contentLength': file_header['ContentLength']})
