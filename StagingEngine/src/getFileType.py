import re
import traceback

import boto3


class GetFileTypeException(Exception):
    pass


dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    return get_file_type(event, context)


def get_file_type(event, context):
    key = event['fileDetails']['key']
    data_source_table = event["settings"]["dataSourceTableName"]

    try:
        ddb_table = dynamodb.Table(data_source_table)
        response = ddb_table.scan(
            ProjectionExpression="fileType, fileSettings.fileNamePattern")

        file_type = None
        for item in response['Items']:
            file_name_pattern = item['fileSettings']['fileNamePattern']
            result = re.match(file_name_pattern, key)
            if result:
                if file_type is None:
                    file_type = item['fileType']
                else:
                    raise GetFileTypeException(
                        "More than one fileNamePattern matches "
                        "key:{}".format(key))

        if file_type is None:
            raise GetFileTypeException(
                "No dataSource fileNamePatterns match key:" + key)
        else:
            event.update({"fileType": file_type})
            return event
    except GetFileTypeException:
        traceback.print_exc()
        raise
    except Exception as e:
        traceback.print_exc()
        raise GetFileTypeException(e)
