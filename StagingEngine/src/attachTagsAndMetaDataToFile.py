import base64
import hashlib
import time
import traceback

import boto3


class AttachTagsAndMetaDataToFileException(Exception):
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
    :raises AttachTagsAndMetaDataToFileException: On any error or exception
    '''
    try:
        return attach_tags_and_metadata_to_file(event, context)
    except AttachTagsAndMetaDataToFileException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise AttachTagsAndMetaDataToFileException(e)


def attach_tags_and_metadata_to_file(event, context):
    '''
    attach_tags_and_metadata_to_file Attach the given (in the event)
    tags and metadata to the file.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    add_tags_to_file(event, context)
    add_meta_data_to_file(event, context)
    return event


def add_tags_to_file(event, context):
    '''
    add_tags_to_file Adds the tags in the event to the file.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :raises AttachTagsAndMetaDataToFileException: On any error or exception
    '''
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']

        tag_list = []
        for tag_key in event['requiredTags']:
            tag = {'Key': tag_key, 'Value': event['requiredTags'][tag_key]}
            tag_list.append(tag)

        s3.put_object_tagging(
            Bucket=bucket, Key=key, Tagging={'TagSet': tag_list})
    except Exception as e:
        traceback.print_exc()
        raise AttachTagsAndMetaDataToFileException(e)


def add_meta_data_to_file(event, context):
    '''
    add_meta_data_to_file Adds the metadata in the event to the file.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :raises AttachTagsAndMetaDataToFileException: On any error or exception
    '''
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']
        required_metadata = event['requiredMetadata']
        existing_metadata = event['existingMetadata']

        metadata = {}
        metadata.update(existing_metadata)
        metadata.update(required_metadata)
        metadata.update({'staging_time': str(int(time.time() * 1000))})
        metadata.update({'created_date': get_created_date(bucket, key)})

        if 'calculateMD5' in event['fileSettings'] \
                and event['fileSettings']['calculateMD5'] == 'True':

            metadata.update({'staged_md5': get_md5(bucket, key)})

        copy_source = {'Bucket': bucket, 'Key': key}
        s3.copy(
            copy_source,
            bucket,
            key,
            ExtraArgs={
                "Metadata": metadata,
                "MetadataDirective": "REPLACE"
            }
        )

        event.update({'attachedMetadata': metadata})

        return event

    except Exception as e:
        traceback.print_exc()
        raise AttachTagsAndMetaDataToFileException(e)


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
