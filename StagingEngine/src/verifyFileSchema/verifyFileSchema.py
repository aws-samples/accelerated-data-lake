import csv
import json
import re
import traceback

import boto3
from jsonschema import validate
from jsonschema.exceptions import ValidationError

import csvvalidator


class VerifyFileSchemaException(Exception):
    pass


s3 = boto3.resource('s3')


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
    :raises VerifyFileSchemaException: On any error or exception
    '''
    try:
        return _verify_file_schema(event, context)
    except VerifyFileSchemaException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise VerifyFileSchemaException(e)


def _verify_file_schema(event, context):
    '''
    verify_file_schema Verifies the schema of the new file if schema
    and format information has been added to the data source config.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    :raises VerifyFileSchemaException: When insufficient config information
    '''
    bucket = event['fileDetails']['bucket']
    key = event['fileDetails']['key']
    file_settings = event['fileSettings']
    file_type = event["fileType"]

    if 'schema' in event and event['schema'] is not None:
        if 'fileFormat' in file_settings:
            file_content = _load_object_content(bucket, key)
            if file_settings['fileFormat'] == 'json':
                _verify_json_schema(file_content, event['schema'])
            elif file_settings['fileFormat'] == 'csv':
                _verify_csv_schema(file_content, event['schema'])
            else:
                raise VerifyFileSchemaException(
                    "Filetype: {} has a defined schema but no "
                    " file format specified".format(file_type))
        else:
            print("Filetype: {} has no defined fileFormat so no "
                  " verification will take place.".format(file_type))
    else:
        print("Filetype: {} has no defined schema so no "
              " verification will take place.".format(file_type))

    return event


def _verify_json_schema(file_content, schema):
    '''
    _verify_json_schema Verifies the schema of json data. The while loop
    is present to allow json documents batched into the same file by firehose
    to be processed and verified.

    :param file_content: The content of the file
    :type file_content: Python String
    :param schema: The jsonschema we are expecting
    :type schema: Python String
    :raises Exception: When file_content schema is incorrect
    '''
    decoder = json.JSONDecoder()
    start_position = 0
    while True:
        match = re.search('[{\[]', file_content[start_position:])
        if not match:
            break
        start_position = match.start() + start_position

        json_object, end_position = decoder.raw_decode(
            file_content[start_position:])

        try:
            validate(json_object, schema)
        except ValidationError as ve:
            raise VerifyFileSchemaException(ve.message[:10240])

        start_position = start_position + end_position


def _verify_csv_schema(file_content, schema):
    '''
    _verify_csv_schema Verifies the schema of csv data. Only required
    column names are confirmed

    :param file_content: The content of the file
    :type file_content: Python String
    :param schema: The csv schema we are expecting
    :type schema: Python String
    :raises Exception: When file_content schema is incorrect
    '''
    file_content_lines = file_content.splitlines()
    csv_reader = csv.reader(file_content_lines)
    field_names = tuple(schema['field_names'])

    validator = csvvalidator.CSVValidator(field_names)
    validator.add_header_check('EX1', 'bad header')
    problems = validator.validate(csv_reader)

    if len(problems) > 0:
        raise VerifyFileSchemaException(str(problems))


def _load_object_content(bucket, key):
    '''
    load_object_content Loads the given object (identified by
    bucket and key) from S3

    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    :return: Contents of S3 object as a string
    :rtype: Python String
    '''
    s3_object = s3.Object(bucket, key)
    return s3_object.get()["Body"].read().decode('utf-8')
