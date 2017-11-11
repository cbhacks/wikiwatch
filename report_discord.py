import os
import yaml
import boto3
import requests

assert 'DISCORD_WEBHOOK' in os.environ
assert 'ADMIN_EMAIL' in os.environ

req_headers = {
    'From': os.environ['ADMIN_EMAIL']
}

s3 = boto3.client('s3')

def lambda_handler(event, context):
    assert len(event['Records']) == 1
    s3ev = event['Records'][0]['s3']
    r = s3.get_object(
        Bucket=s3ev['bucket']['name'],
        Key=s3ev['object']['key']
    )
    rvmeta = yaml.safe_load(r['Body'])
    assert 'pageid' in rvmeta
    assert 'title' in rvmeta
    assert 'url' in rvmeta
    assert 'revid' in rvmeta
    assert 'parentid' in rvmeta
    assert 'user' in rvmeta
    assert 'timestamp' in rvmeta
    assert 'comment' in rvmeta
    if rvmeta['parentid'] == 0:
        desc = 'Page created.'
    else:
        desc = 'Page edited.'
    comment = rvmeta['comment']
    if comment == '':
        comment = '(no comment)'
    requests.post(
        os.environ['DISCORD_WEBHOOK'],
        params={
            'wait': 'true'
        },
        json={
            'embeds': [
                {
                    'title': rvmeta['title'],
                    'description': desc,
                    'url': rvmeta['url'],
                    'timestamp': rvmeta['timestamp'],
                    'author': {
                        'name': rvmeta['user']
                    },
                    'fields': [
                        {
                            'name': 'Comment',
                            'value': comment,
                            'inline': False
                        }
                    ]
                }
            ]
        },
        headers=req_headers
    ).raise_for_status()
