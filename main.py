import os
import yaml
import boto3
import botocore
import requests
import io
import gzip

assert 'CONFIG_BUCKET' in os.environ
assert 'CONFIG_KEY' in os.environ
assert 'ADMIN_EMAIL' in os.environ

req_headers = {
    'From': os.environ['ADMIN_EMAIL']
}

s3 = boto3.client('s3')

def get_config():
    """Downloads the configuration file from S3."""
    r = s3.get_object(
        Bucket=os.environ['CONFIG_BUCKET'],
        Key=os.environ['CONFIG_KEY']
    )
    return yaml.safe_load(r['Body'].read().decode())

class WikiError(Exception):
    pass

def mwapi_query(endpoint, params):
    """Runs a 'query' request against the given MediaWiki API endpoint."""

    # Build the base request parameters.
    base_params = params.copy()
    base_params['action'] = 'query'
    base_params['format'] = 'json'
    base_params['rawcontinue'] = ''
    base_params['maxlag'] = '1'

    # Start with the initial "query-continue" parameters as empty.
    continue_params = {}

    # Repeatedly make the request until there are no remaining continues.
    while True:
        current_params = base_params.copy()
        current_params.update(continue_params)

        # Make the request and parse the response.
        r = requests.post(endpoint, params=current_params, headers=req_headers)
        r.raise_for_status()
        j = r.json()

        # Abort on any errors returned. Errors may still pass raise_for_status
        # because they can be returned in the JSON response even for HTTP 200.
        if 'error' in j:
            raise WikiError(j['error'])

        # FIXME - unsure what this does. described on MediaWiki's API page
        if 'warnings' in j:
            raise WikiError(j['warnings'])

        # Yield the query result, or this portion of it at least.
        if 'query' in j:
            yield j['query']

        # Handle query continuation. This is done using the old rawcontinue
        # format for compatibility with older wikis (e.g. Wikia).
        if 'query-continue' in j:
            continue_params = {}
            for k1 in j['query-continue']:
                for k2 in j['query-continue'][k1]:
                    if not k2.startswith('g'):
                        continue_params[k2] = j['query-continue'][k1][k2]
            if not continue_params:
                for k in j['query-continue']:
                    continue_params.update(j['query-continue'][k])
            continue
        else:
            break

def lambda_handler(event, context):
    """Entry point for AWS Lambda."""
    config = get_config()
    assert 'wikis' in config
    for wiki in config['wikis']:
        assert 'api' in wiki
        assert 's3_bucket' in wiki
        assert 's3_prefix' in wiki
        assert 'sources' in wiki
        for source in wiki['sources']:
            params = { 'prop': 'revisions', 'rvprop': 'ids' }
            params.update(source)
            for result in mwapi_query(wiki['api'], params):
                assert 'pages' in result
                for _, page in result['pages'].items():
                    assert 'pageid' in page
                    assert 'revisions' in page
                    assert len(page['revisions']) == 1
                    revision = page['revisions'][0]
                    assert 'revid' in revision
                    handle_revision(wiki, page['pageid'], revision['revid'])

def s3_key_exists(bucket, key):
    """Checks if a key exists in an S3 bucket."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print('[FAIL] HEAD on S3: ({:s}): {:s}'.format(bucket, key))
            return False
        else:
            raise
    print('[ OK ] HEAD on S3: ({:s}): {:s}'.format(bucket, key))
    return True

def s3_key_for_revision_metadata(wiki, pageid, revid):
    """Computes the key for the S3 object storing metadata about a revision."""
    return '{:s}page_{:08d}/rev_{:08d}.yaml'.format(
        wiki['s3_prefix'],
        pageid,
        revid
    )

def s3_key_for_revision_content(wiki, pageid, revid):
    """Computes the key for the S3 object storing gzipped revision content."""
    return '{:s}page_{:08d}/rev_{:08d}_data.gz'.format(
        wiki['s3_prefix'],
        pageid,
        revid
    )

def s3_key_for_revision_metadata_exists(wiki, pageid, revid):
    """Checks if a revision's metadata object exists on S3."""
    return s3_key_exists(
        wiki['s3_bucket'],
        s3_key_for_revision_metadata(wiki, pageid, revid)
    )

def s3_key_for_revision_content_exists(wiki, pageid, revid):
    """Checks if a revision's gzipped content object exists on S3."""
    return s3_key_exists(
        wiki['s3_bucket'],
        s3_key_for_revision_content(wiki, pageid, revid)
    )

def s3_put_revision_metadata(wiki, pageid, revid, value):
    """Puts the given value in YAML format as the metadata object on S3."""
    bucket = wiki['s3_bucket']
    key = s3_key_for_revision_metadata(wiki, pageid, revid)
    body = yaml.dump(value, default_flow_style=False)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body
    )
    print('[ OK ] PUT on S3: ({:s}): {:s}'.format(bucket, key))
    print(body)

def s3_put_revision_content(wiki, pageid, revid, content):
    """Gzips the given content and puts it as the content object on S3."""
    bucket = wiki['s3_bucket']
    key = s3_key_for_revision_content(wiki, pageid, revid)
    stream = io.BytesIO()
    stream_gz = gzip.GzipFile(None, 'wb', 9, stream)
    stream_gz.write(content.encode())
    stream_gz.close()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=stream.getvalue()
    )
    print('[ OK ] PUT on S3: ({:s}): {:s}'.format(bucket, key))

def handle_revision(wiki, pageid, revid):
    """Adds a revision (and its ancestors) to S3.
    
    If an object with the revision's key already exists in the S3 bucket, it is
    not downloaded or added, and all of its ancestors are excluded from
    consideration entirely.

    The ancestral revisions are added before this revision, and those ones are
    added most-ancestral to least. This ensures that, should a latter S3 PUT
    operation fail, all of the revisions ancestral to that one have already
    been stored and thus it is acceptable to exclude them from download later.
    """
    if revid == 0:
        return
    if s3_key_for_revision_metadata_exists(wiki, pageid, revid):
        return
    req_params = {
        'prop': 'revisions|info',
        'rvprop': 'ids|user|timestamp|comment',
        'inprop': 'url',
        'revids' : revid
    }
    need_content = not s3_key_for_revision_content_exists(wiki, pageid, revid)
    if need_content:
        req_params['rvprop'] += '|content'
        if 'slots' in wiki:
            req_params['rvslots'] = wiki['slots']
    result = next(mwapi_query(
        wiki['api'],
        req_params
    ))
    assert 'pages' in result
    assert str(pageid) in result['pages']
    assert len(result['pages']) == 1
    page = result['pages'][str(pageid)]
    assert 'pageid' in page
    assert page['pageid'] == pageid
    assert 'title' in page
    assert 'fullurl' in page
    assert 'revisions' in page
    assert len(page['revisions']) == 1
    revision = page['revisions'][0]
    assert 'revid' in revision
    assert revision['revid'] == revid
    assert 'parentid' in revision
    parentid = revision['parentid']
    assert 'user' in revision
    assert 'timestamp' in revision
    assert 'comment' in revision
    if need_content:
        if 'slots' in wiki:
            assert 'main' in revision['slots']
            assert '*' in revision['slots']['main']
            content = revision['slots']['main']['*']
        else:
            assert '*' in revision
            content = revision['*']
        s3_put_revision_content(wiki, pageid, revid, content)
    handle_revision(wiki, pageid, revision['parentid'])
    s3_put_revision_metadata(wiki, pageid, revid, {
        'pageid': page['pageid'],
        'title': page['title'],
        'url': page['fullurl'],
        'revid': revid,
        'parentid': parentid,
        'user': revision['user'],
        'timestamp': revision['timestamp'],
        'comment': revision['comment']
    })

if __name__ == "__main__":
    lambda_handler(None, None)
