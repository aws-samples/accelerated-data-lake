import datetime
import json
import logging
import os
import time
import traceback

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.endpoint import BotocoreHTTPSession
from botocore.session import Session
from boto3.dynamodb.types import TypeDeserializer


elasticsearch_endpoint = os.environ['ELASTICSEARCH_ENDPOINT']
# Python formatter to generate index name from the DynamoDB
# table name
DOC_TABLE_FORMAT = '{}'
# Python formatter to generate type name from the DynamoDB
# tablename, default is to add '_type' suffix
DOC_TYPE_FORMAT = '{}_type'
# Max number of retries for exponential backoff
ES_MAX_RETRIES = 3
# Set verbose debugging information
DEBUG = True

logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


class SendDataCatalogUpdateToElasticsearch(Exception):
    pass


class ES_Exception(Exception):
    status_code = 0
    payload = ''

    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload
        Exception.__init__(
            self,
            'ES_Exception: status_code={}, payload={}'.format(
                status_code, payload))


# Subclass of boto's TypeDeserializer for DynamoDB to adjust
# for DynamoDB Stream format.
class StreamTypeDeserializer(TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value)

    def _deserialize_b(self, value):
        return value  # Already in Base64


# Global lambda handler - catches all exceptions to avoid
# dead letter in the DynamoDB Stream
def lambda_handler(event, context):
    try:
        return _lambda_handler(event, context)
    except Exception:
        logger.error(traceback.format_exc())


def _lambda_handler(event, context):
    records = event['Records']
    now = datetime.datetime.utcnow()

    ddb_deserializer = StreamTypeDeserializer()
    es_actions = []  # Items to be added/updated/removed from ES - for bulk API
    for record in records:
        ddb = record['dynamodb']
        ddb_table_name = get_table_name_from_arn(record['eventSourceARN'])
        doc_seq = ddb['SequenceNumber']

        # Compute DynamoDB table, type and index for item
        doc_table = DOC_TABLE_FORMAT.format(ddb_table_name.lower())
        doc_type = DOC_TYPE_FORMAT.format(ddb_table_name.lower())
        doc_index = compute_doc_index(ddb['Keys'], ddb_deserializer)

        # Get the event type
        event_name = record['eventName'].upper()  # INSERT, MODIFY, REMOVE

        # If DynamoDB INSERT or MODIFY, send 'index' to ES
        if (event_name == 'INSERT') or (event_name == 'MODIFY'):
            if 'NewImage' not in ddb:
                logger.warning(
                    'Cannot process stream if it does not contain NewImage')
                continue

            # Deserialize DynamoDB type to Python types
            doc_fields = ddb_deserializer.deserialize({'M': ddb['NewImage']})
            # Add metadata
            doc_fields['@timestamp'] = now.isoformat()
            doc_fields['@SequenceNumber'] = doc_seq

            # Generate JSON payload
            doc_json = json.dumps(doc_fields)

            # Generate ES payload for item
            action = {
                'index': {
                    '_index': doc_table,
                    '_type': doc_type,
                    '_id': doc_index}}
            es_actions.append(json.dumps(action))
            es_actions.append(doc_json)

    # Prepare bulk payload
    es_actions.append('')  # Add one empty line to force final \n
    es_payload = '\n'.join(es_actions)
    print("PAYLOAD:{}".format(es_payload))

    post_to_es(es_payload)  # Post to ES with exponential backoff


# High-level POST data to Amazon Elasticsearch Service with exponential backoff
def post_to_es(payload):

    # Get aws_region and credentials to post signed URL to ES
    es_region = os.environ['AWS_REGION']
    session = Session({'region': es_region})
    creds = get_credentials(session)

    # Post data with exponential backoff
    retries = 0
    while retries < ES_MAX_RETRIES:
        if retries > 0:
            seconds = (2 ** retries) * .1
            time.sleep(seconds)

        try:
            es_ret_str = post_data_to_es(
                payload,
                es_region,
                creds,
                elasticsearch_endpoint,
                '/_bulk')
            es_ret = json.loads(es_ret_str)

            if es_ret['errors']:
                logger.error(
                    'ES post unsuccessful, errors present, took=%sms',
                    es_ret['took'])
                # Filter errors
                es_errors = \
                    [item for item in es_ret['items'] if
                        item.get('index').get('error')]
                logger.error(
                    'List of items with errors: %s',
                    json.dumps(es_errors))
            else:
                logger.info('ES post successful, took=%sms', es_ret['took'])
            break  # Sending to ES was ok, break retry loop
        except ES_Exception as e:
            if (e.status_code >= 500) and (e.status_code <= 599):
                retries += 1  # Candidate for retry
        else:
            raise  # Stop retrying, re-raise exception


def post_data_to_es(
        payload, region, creds, host,
        path, method='POST', proto='https://'):

    print("URL:{}".format(proto+host+path))
    req = AWSRequest(
        method=method,
        url=proto+host+path,
        data=payload,
        headers={'Host': host, 'Content-Type': 'application/json'})
    SigV4Auth(creds, 'es', region).add_auth(req)
    http_session = BotocoreHTTPSession()
    res = http_session.send(req.prepare())
    print("STATUS_CODE:{}".format(res.status_code))
    print("CONTENT:{}".format(res._content))
    print("ALL:{}".format(res))

    if res.status_code >= 200 and res.status_code <= 299:
        return res._content
    else:
        raise ES_Exception(res.status_code, res._content)


# Extracts the DynamoDB table from an ARN
def get_table_name_from_arn(arn):
    return arn.split(':')[5].split('/')[1]


# Compute a compound doc index from the key(s) of the object in
# lexicographic order: "k1=key_val1|k2=key_val2"
def compute_doc_index(keys_raw, deserializer):
    index = []
    for key in sorted(keys_raw):
        index.append('{}={}'.format(
            key,
            deserializer.deserialize(keys_raw[key])))
    return '|'.join(index)
