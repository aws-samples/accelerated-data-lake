import base64
import hashlib
import time
import traceback

import boto3


class CalculateMetaDataForFileException(Exception):
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
    :raises CalculateMetaDataForFileException: On any error or exception
    '''
    try:
        return calculate_additional_metadata(event, context)
    except CalculateMetaDataForFileException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise CalculateMetaDataForFileException(e)


def calculate_additional_metadata(event, context):
    '''
    calculate_additional_metadata Calculated additional required metadata
    and adds it to the event. Add any additional metadata calculation here.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :raises CalculateMetaDataForFileException: On any error or exception
    '''
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']

        existing_metadata = event['existingMetadata']
        required_metadata = event['requiredMetadata']
        required_metadata.update({'staging_time': str(int(time.time() * 1000))})
        required_metadata.update({'created_date': get_created_date(bucket, key)})

        if 'calculateMD5' in event['fileSettings'] \
                and event['fileSettings']['calculateMD5'] == 'True':
            required_metadata.update({'staged_md5': get_md5(bucket, key)})

        combinedMetadata = {}
        combinedMetadata.update(existing_metadata)
        combinedMetadata.update(required_metadata)

        event.update({'combinedMetadata': combinedMetadata})

        return event

    except Exception as e:
        traceback.print_exc()
        raise CalculateMetaDataForFileException(e)


def get_created_date(bucket, key):
    '''
    get_created_date Gets the LastModified date (in this case, the
    created date) of the file.

    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    :return: The created date
    :rtype: Python String
    '''
    file_header = s3.head_object(Bucket=bucket, Key=key)
    return str(file_header['LastModified'])


def get_md5(bucket, key):
    '''
    get_md5 Returns the MDS of the given S3 object

    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    :return: The MD5 of the file contents
    :rtype: Python String
    '''
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    content = s3_object['Body'].read()
    md5_bytes = hashlib.md5(content).digest()
    md5_base64 = base64.b64encode(md5_bytes).decode('ascii')

    return md5_base64
