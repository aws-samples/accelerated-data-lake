import re
import traceback

import boto3


class GetFileTypeException(Exception):
    pass


dynamodb = boto3.resource('dynamodb')


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
    :raises GetFileTypeException: On any error or exception
    '''
    try:
        return get_file_type(event, context)
    except GetFileTypeException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise GetFileTypeException(e)


def get_file_type(event, context):
    '''
    get_file_type Goes through the Data Lakes configured Data Sources
    and returns the matching filetype with the greatest specificity.
    This allows us to have wildcards in a filetype's folder structue,
    and the datasource with deepest wildcard will be selected.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :raises GetFileTypeException: If <> 1 matching file types found
    :return: The input event object but with the matching fileType added
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    key = event['fileDetails']['key']

    data_source_details = _get_all_data_source_details(event)
    matching_data_sources = _filter_matching_data_sources(
        key,
        data_source_details)
    filetype = _get_most_specific_filetype(
        key,
        matching_data_sources)

    event.update({"fileType": filetype})

    return event


def _get_all_data_source_details(event):
    '''
    _get_all_data_source_details Scans and retrieves the fileType and
    fileNamePattern of all datasources in DynamoDB.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :return: Collection of fileType and pattern for each data source
    :rtype: Python List
    '''
    data_source_table = event["settings"]["dataSourceTableName"]

    ddb_table = dynamodb.Table(data_source_table)
    response = ddb_table.scan(
        ProjectionExpression="fileType, fileSettings.fileNamePattern")

    return response['Items']


def _filter_matching_data_sources(key, data_source_details):
    '''
    _filter_matching_data_sources Filters the list of data sources
    details and only returs those with a fileNamePattern that matches
    the given key.

    :param key: [description]
    :type key: [type]
    :param data_source_details: [description]
    :type data_source_details: [type]
    :return: [description]
    :rtype: [type]
    '''
    matching_data_sources = [
        data_source for data_source in data_source_details
        if (re.fullmatch(
            data_source['fileSettings']['fileNamePattern'],
            key) is not None)
        ]

    return matching_data_sources


def _get_most_specific_filetype(key, matching_data_sources):
    '''
    _get_most_specific_filetype Returns the most specific filetype
    from the matching data sources.
    Rules:
    If no matching data sources are supplied, an exception is raised.
    If one matching data source is supplied, it is returned as the match.
    If multiple matching data sources are supplied, the one wih the
    deepest first wildcard is returned as the match.
    If multiple matching data sources are supplied and more then one have
    a wildcard at the deepest level, an exception is raised.

    :param key: The filename we wish to find the most specific filetype of.
    :type key: Python String
    :param matching_data_sources: The datasources that match this filename
    :type matching_data_sources: Python List
    :raises GetFileTypeException: If a single specific filetype cannot be found
    :return: The name of the most specific filetype
    :rtype: Python String
    '''
    if matching_data_sources is None or len(matching_data_sources) == 0:
        raise GetFileTypeException(
            "No dataSource fileNamePatterns match key:" + key)

    if (len(matching_data_sources) == 1):
        return matching_data_sources[0]['fileType']

    deepest_folder_depth = None
    deepest_wildcard = None
    deepest_wildcard_count = None
    deepest_wildcard_filetype = None

    for data_source in matching_data_sources:
        data_source_folders = data_source['fileSettings']['fileNamePattern']\
            .split('/')
        data_source_filetype = data_source['fileType']

        folder_depth = len(data_source_folders)

        if deepest_folder_depth is None:
            deepest_folder_depth = folder_depth
        elif folder_depth != deepest_folder_depth:
            raise GetFileTypeException(
                "Matching data sources have inconsistent folder depths")

        first_wildcard_depth = _get_first_wildcard_depth(
                data_source_folders,
                data_source_filetype)

        if deepest_wildcard is None or first_wildcard_depth > deepest_wildcard:
            deepest_wildcard = first_wildcard_depth
            deepest_wildcard_count = 1
            deepest_wildcard_filetype = data_source_filetype
        elif first_wildcard_depth == deepest_wildcard:
            deepest_wildcard_count = deepest_wildcard_count + 1
            deepest_wildcard_filetype = None

    if deepest_wildcard_count == 1:
        return deepest_wildcard_filetype
    else:
        raise GetFileTypeException(
                "{} datasources had a wildcard depth of {}"
                .format(deepest_wildcard_count, deepest_wildcard))


def _get_first_wildcard_depth(data_source_folders, data_source_filetype):
    '''
    _get_first_wildcard_depth Examines the datasource's folders and
    returns the depth of the first that contains regex wildcards.

    :param data_source_folders: Collection of the filetype's folders
    :type data_source_folders: Python List
    :param data_source_filetype: The name of the filetype
    :type data_source_filetype: Python String
    :raises GetFileTypeException: If no wildcards exist in structure
    :return: The depth of the first wildcard
    :rtype: Python Integer
    '''
    wildcard_depth = None
    for depth in range(len(data_source_folders)):
        folder = data_source_folders[depth]
        if re.fullmatch(r"^[a-zA-Z0-9-_.*'()+]+$", folder) is None:
            wildcard_depth = depth
            break

    if wildcard_depth is None:
        raise GetFileTypeException(
                "Filetype:{} fileNamePattern does not include wildcards"
                .format(data_source_filetype))

    return wildcard_depth
