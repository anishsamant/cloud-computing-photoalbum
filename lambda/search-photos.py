import boto3
import logging
import requests
import json
import os
import inflect

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#Set Constants
botId = os.environ.get('BOT_ID')
botAliasId = os.environ.get('BOT_ALIAS_ID')
localeId = os.environ.get('LOCALE')
es_endpoint = os.environ.get('ES_ENDPOINT')
es_index = os.environ.get('ES_INDEX')
es_username = os.environ.get('ES_USERNAME')
es_password = os.environ.get('ES_PASSWORD')


# Validating result
def try_func(func):
    try:
        return func()
    except KeyError:
        return None

def lambda_handler(event, context):
    print(event)
    logger.debug('EVENT:')
    logger.debug(event)
    logger.debug(context)

    query = try_func(lambda: event['queryStringParameters']['q'])
    logger.debug(query)
    if not query:
        return {
            'statusCode': 400,
            'body': 'No query found in event'
        }
        
    p = inflect.engine()


    # Using Lex to handle search queries
    keywords = []
    client = boto3.client('lexv2-runtime')
    response = client.recognize_text(
        botId=botId,
        botAliasId=botAliasId,
        localeId=localeId,
        sessionId='searchPhotos',
        text=query
    )
    
    logger.debug('LEX Response:')
    logger.debug(response)

    slots = try_func(lambda: response['interpretations'][0]['intent']['slots'])
    print(slots)
    
    for _, v in slots.items():
        if v:
            word = v['value']['interpretedValue']
            keywords.append(word)
            if (p.singular_noun(word) == False):
                keywords.append(p.plural(word))
            else:
                keywords.append(p.singular_noun(word))


    credentials = boto3.Session().get_credentials()

    es_query = '{}/{}/_search'.format(es_endpoint, es_index)

    headers = {'Content-Type': 'application/json'}
    prepared_q = []
    print('keywords: ',  keywords)
    for k in keywords:
        prepared_q.append({"match": {"labels": k}})
        
    q = {"query": {"bool": {"should": prepared_q}}}

    r = requests.post(es_query,  auth=(es_username, es_password), headers=headers, data=json.dumps(q))
    data = json.loads(r.content.decode('utf-8'))
    
    logger.debug('Elastic Search Result')
    logger.debug(data)

    # Extract images
    all_photos = []
    prepend_url = 'https://s3.amazonaws.com'
    hits = try_func(lambda: data['hits']['hits'])
    for h in hits:
        photo = {}
        obj_bucket = try_func(lambda: h['_source']['bucket'])
        obj_key = try_func(lambda: h['_source']['objectKey'])
        full_photo_path = '/'.join([prepend_url, obj_bucket, obj_key])
        photo['url'] = full_photo_path
        photo['labels'] = try_func(lambda: h['_source']['labels'])
        all_photos.append(photo)
        print(photo)

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        },
        'body': json.dumps(all_photos)
    }