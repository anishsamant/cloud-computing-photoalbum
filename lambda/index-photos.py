import requests
import boto3
import datetime
import json
import logging
import os

es_endpoint = os.environ.get('ES_ENDPOINT')
es_index = os.environ.get('ES_INDEX')
es_username = os.environ.get('ES_USERNAME')
es_password = os.environ.get('ES_PASSWORD')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def try_ex(func):
    try:
        return func()
    except KeyError:
        return None

def get_photo_labels(bucket, photo):
    client = boto3.client('rekognition')
    print(bucket)
    print(photo)
    response = client.detect_labels(Image={'S3Object':{'Bucket': bucket, 'Name': photo}}, MaxLabels=10)
    labels = try_ex(lambda: response['Labels'])
    all_labels = [l['Name'] for l in labels]
    return all_labels

def put_to_es(index, type, new_doc):

    endpoint = '{}/{}/{}'.format(es_endpoint, index, type)

    headers = {
        'Content-Type': 'application/json'
    }
    
    print("endpoint=", endpoint)
    print(es_username, es_password, new_doc, headers)
    r = requests.post(endpoint, auth=(es_username, es_password), data=new_doc, headers=headers)
    print(r.content)

def get_s3_metadata(bucket, photo):
    s3 = boto3.client('s3')
    metadata = s3.head_object(Bucket=bucket, Key=photo)
    logger.debug('Getting metadata: ')
    logger.debug(metadata)
    
    if metadata['Metadata']:
        return metadata['Metadata']['customlabels'] if metadata['Metadata']['customlabels'] is not None else ''
    else:
        ''

def lambda_handler(event, context):
    print("index-photos updated via codebuild and codepipeline")
    logger.debug('LF is invoked by S3')
    logger.debug(event)

    # Extract new img from the S3 event
    s3obj = try_ex(lambda: event['Records'])
    if not s3obj:
        return {
        'statusCode': 500,
        'errorMsg': 'Event message does not follow S3 JSON structure ver 2.1'
    }

    for obj in s3obj:
        bucket = try_ex(lambda: obj['s3']['bucket']['name'])
        photo = try_ex(lambda: obj['s3']['object']['key'])

        # Get all labels
        logger.debug('Getting label for  %s::%s' % (bucket, photo))
        labels = get_photo_labels(bucket, photo)
        logger.debug('Labels for %s::%s are %s' % (bucket, photo, labels))

        # Get custom labels
        custom_labels = get_s3_metadata(bucket, photo)
        custom_labels = custom_labels.split(',') if custom_labels is not None else []

        labels = labels + custom_labels
        logger.debug('Combined Labels: ')
        logger.debug(labels)

        # Index the photo
        doc = {
            'objectKey': photo,
            'bucket': bucket,
            'createdTimestamp': datetime.datetime.now().strftime('%Y-%d-%m-T%H:%M:%S'),
            'labels': labels
        }
        
        print("Document=", doc)
        put_to_es(es_index, 'photo', json.dumps(doc))

    return {
        'statusCode': 200,
        'body': doc
    }