import boto3
import traceback
from dateutil import parser
from dateutil.tz import gettz


class CopyFileFromRawToStagingException(Exception):
    pass


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    copy_file_to_staging(event, context)
    return event


def copy_file_to_staging(event, context):
    try:
        raw_bucket = event['fileDetails']['bucket']
        raw_key = event['fileDetails']['key']
        raw_file_name = event['fileDetails']['fileName']
        staging_bucket = event['settings']['stagingBucket']
        staging_folder_path = event['fileSettings']['stagingFolderPath']
        metadata = event['attachedMetadata']
        created_date = metadata['createddate']

        # Generate the staging key, including partitioning info if required.
        if 'stagingPartitionSettings' in event['fileSettings']:
            staging_key = get_staging_key(
                raw_file_name,
                staging_folder_path,
                event['fileSettings']
                     ['stagingPartitionSettings']['expression'],
                event['fileSettings']['stagingPartitionSettings']['timezone'],
                created_date)
        else:
            staging_key = staging_folder_path + '/' + raw_file_name

        # Tags and metadata follow s3's read after first write consistency -
        # everything else is eventual. So, it is possible the tags and metadata
        # we added to the raw file AFTER it was put in the raw bucket will not
        # be copied across to the staging bucket. To eliminate any chance of
        # this happening, we specify exactly what metadata we want copying to
        # staging, and then re-apply the tags.
        print('Copying object {} from bucket {} to key {} in bucket {}'.format(
            raw_key, raw_bucket, staging_key, staging_bucket))
        copy_source = {'Bucket': raw_bucket, 'Key': raw_key}
        s3.copy(
            copy_source,
            staging_bucket,
            staging_key,
            ExtraArgs={"Metadata": metadata, "MetadataDirective": "REPLACE"})
        event['fileDetails'].update({"stagingKey": staging_key})

        # Re-generat the tag list.
        tagList = []
        for tagKey in event['requiredTags']:
            tag = {'Key': tagKey, 'Value': event['requiredTags'][tagKey]}
            tagList.append(tag)

        # Re-apply the tag list.
        s3.put_object_tagging(
            Bucket=staging_bucket,
            Key=staging_key,
            Tagging={'TagSet': tagList})
    except Exception as e:
        traceback.print_exc()
        raise CopyFileFromRawToStagingException(e)


def get_staging_key(raw_file_name, staging_folder, staging_expression,
                    tz_staging_timezone, date_time_string):
    # Get the dateTime in the specified timezone. Ensure TZ format:
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    # ie, "Australia/Brisbane"
    datetime = parser.parse(date_time_string)
    datetme_in_timezone = datetime.astimezone(gettz(tz_staging_timezone))

    staging_key = "{}/{}/{}".format(
        staging_folder,
        datetme_in_timezone.strftime(staging_expression),
        raw_file_name)
    print(
        "Source DateTime:{} tzDateTime:{} stagingKey:{}",
        datetime, datetme_in_timezone, staging_key)

    return staging_key
