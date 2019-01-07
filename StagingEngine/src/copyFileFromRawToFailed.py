import boto3
import traceback


class CopyFileFromRawToFailedException(Exception):
    pass


s3 = boto3.client('s3')


def lambda_handler(event, context):
    copy_file_to_failed(event, context)
    return event


def copy_file_to_failed(event, context):
    try:
        raw_bucket = event['fileDetails']['bucket']
        raw_key = event['fileDetails']['key']
        failed_bucket = event['settings']['failedBucket']

        print(
            'Copying object {} from bucket {} to key {} in failed bucket {}'
            .format(raw_key, raw_bucket, raw_key, failed_bucket)
        )

        # Copy the failed file to the failed bucket.
        copy_source = {'Bucket': raw_bucket, 'Key': raw_key}
        s3.copy(copy_source, failed_bucket, raw_key)

        # Delete the failed file from raw.
        s3.delete_object(Bucket=raw_bucket, Key=raw_key)
    except Exception as e:
        traceback.print_exc()
        raise CopyFileFromRawToFailedException(e)
