import base64
import hashlib
import time
import traceback

import boto3


class AttachTagsAndMetaDataToFileException(Exception):
    pass


s3 = boto3.client('s3')


def lambda_handler(event, context):
    add_tags_to_file(event, context)
    add_meta_data_to_file(event, context)
    return event


def add_tags_to_file(event, context):
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']

        tag_list = []
        for tag_key in event['requiredTags']:
            tag = {'Key': tag_key, 'Value': event['requiredTags'][tag_key]}
            tag_list.append(tag)

        s3.put_object_tagging(
            Bucket=bucket, Key=key, Tagging={'TagSet': tag_list}
        )
        return "Object tagged successfully"
    except Exception as e:
        traceback.print_exc()
        raise AttachTagsAndMetaDataToFileException(e)


def add_meta_data_to_file(event, context):
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']
        required_metadata = event['requiredMetadata']
        existing_metadata = event['existingMetadata']

        metadata = {}
        metadata.update(existing_metadata)
        metadata.update(required_metadata)
        metadata.update({'stagingtime': str(int(time.time() * 1000))})
        metadata.update({'createddate': get_created_date(bucket, key)})

        if 'calculateMD5' in event['fileSettings'] \
                and event['fileSettings']['calculateMD5'] == 'True':

            metadata.update({'stagedmd5': get_md5(bucket, key)})

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
    file_header = s3.head_object(Bucket=bucket, Key=key)
    return str(file_header['LastModified'])


def get_md5(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    content = s3_object['Body'].read()
    md5_bytes = hashlib.md5(content).digest()
    md5_base64 = base64.b64encode(md5_bytes).decode('ascii')

    return md5_base64
