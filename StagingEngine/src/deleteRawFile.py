import boto3
import traceback


class DeleteRawFileException(Exception):
    pass


s3 = boto3.client('s3')


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
    :raises DeleteRawFileException: On any error or exception
    '''
    try:
        return delete_raw_file(event, context)
    except DeleteRawFileException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise DeleteRawFileException(e)


def delete_raw_file(event, context):
    '''
    delete_raw_file Deletes the file from the raw bucket.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    raw_bucket = event['fileDetails']['bucket']
    raw_key = event['fileDetails']['key']

    print('Deleting raw object {} in bucket {}'.format(raw_key, raw_bucket))

    # Delete the file from raw.
    s3.delete_object(Bucket=raw_bucket, Key=raw_key)

    return event
