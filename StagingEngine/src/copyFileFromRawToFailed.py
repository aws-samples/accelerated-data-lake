import boto3
import traceback


class CopyFileFromRawToFailedException(Exception):
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
    :raises CopyFileFromRawToFailedException: On any error or exception
    '''
    try:
        return copy_file_from_raw_to_failed(event, context)
    except CopyFileFromRawToFailedException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise CopyFileFromRawToFailedException(e)


def copy_file_from_raw_to_failed(event, context):
    '''
    copy_file_from_raw_to_failed Copies the file from the data lake raw
    bucket to the failed bucket.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    raw_bucket = event['fileDetails']['bucket']
    raw_key = event['fileDetails']['key']
    failed_bucket = event['settings']['failedBucket']

    print('Copying object {} from bucket {} to key {} in failed bucket {}'
          .format(raw_key, raw_bucket, raw_key, failed_bucket))

    # Copy the failed file to the failed bucket.
    copy_source = {'Bucket': raw_bucket, 'Key': raw_key}
    s3.copy(copy_source, failed_bucket, raw_key)

    # Delete the failed file from raw.
    s3.delete_object(Bucket=raw_bucket, Key=raw_key)

    return event
