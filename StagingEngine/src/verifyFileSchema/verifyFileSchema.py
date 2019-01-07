import json
import traceback

import boto3
from jsonschema import validate


class VerifyFileSchemaException(Exception):
    pass


s3 = boto3.resource('s3')


def lambda_handler(event, context):
    try:
        bucket = event['fileDetails']['bucket']
        key = event['fileDetails']['key']
        file_settings = event['fileSettings']
        file_type = event["fileType"]

        if 'schema' in event.keys() and event['schema'] is not None:
            if 'fileFormat' in file_settings.keys():
                verify_schema(
                    bucket,
                    key,
                    file_settings['fileFormat'],
                    event['schema'])
            else:
                raise VerifyFileSchemaException(
                    "Filetype: {} has a defined schema but no "
                    " file format specified".format(file_type))
        else:
            print("Filetype: {} has no defined schema so no "
                  " verification will take place.".format(file_type))

        return event
    except VerifyFileSchemaException:
        traceback.print_exc()
        raise
    except Exception as e:
        traceback.print_exc()
        raise VerifyFileSchemaException(e)


def verify_schema(bucket, key, format, schema):
    if format != 'json':
        raise VerifyFileSchemaException(
            "File format: {} is not supported for schema validation"
            .format(format))

    file_content = load_object_content(bucket, key)
    file_object = json.loads(file_content)

    # If no exception is raised by validate(), the instance is valid.
    validate(file_object, schema)


def load_object_content(bucket, key):
    s3_object = s3.Object(bucket, key)
    return s3_object.get()["Body"].read()
