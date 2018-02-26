import logging
import os
import urllib

import boto3
from botocore.client import Config

PKGSTORE_BUCKET = os.environ.get('PKGSTORE_BUCKET')


def dump_steps(*parts, final=False):
    handle_non_tabular = False if final else True
    if os.environ.get('PLANNER_LOCAL'):
        return [('dump.to_path',
                 {
                     'force-format': False,
                     'handle-non-tabular': handle_non_tabular,
                     'add-filehash-to-path': True,
                     'out-path': '/'.join(str(p) for p in parts),
                     'pretty-descriptor': True,
                     'counters': {
                         "datapackage-rowcount": "datahub.stats.rowcount",
                         "datapackage-bytes": "datahub.stats.bytes",
                         "datapackage-hash": "datahub.hash",
                         "resource-rowcount": "rowcount",
                         "resource-bytes": "bytes",
                         "resource-hash": "hash",
                     }
                 })]
    else:
        return [('assembler.dump_to_s3',
                 {
                     'force-format': False,
                     'handle-non-tabular': handle_non_tabular,
                     'add-filehash-to-path': True,
                     'bucket': PKGSTORE_BUCKET,
                     'path': '/'.join(str(p) for p in parts),
                     'pretty-descriptor': True,
                     'acl': 'private',
                     'final': final,
                     'counters': {
                         "datapackage-rowcount": "datahub.stats.rowcount",
                         "datapackage-bytes": "datahub.stats.bytes",
                         "datapackage-hash": "datahub.hash",
                         "resource-rowcount": "rowcount",
                         "resource-bytes": "bytes",
                         "resource-hash": "hash",
                     }
                 })]


def get_s3_client():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    s3_client = boto3.client(
        's3',
        config=Config(signature_version='s3v4'),
        endpoint_url=endpoint_url)
    if endpoint_url:
        try:
            s3 = boto3.resource(
                's3',
                config=Config(signature_version='s3v4'),
                endpoint_url=endpoint_url)
            s3.create_bucket(Bucket=PKGSTORE_BUCKET)
            bucket = s3.Bucket(PKGSTORE_BUCKET)
            bucket.Acl().put(ACL='public-read')
        except: # noqa
            logging.exception('Failed to create the bucket')
            pass
    return s3_client


client = get_s3_client()


def s3_path(*parts):
    if os.environ.get('PLANNER_LOCAL'):
        path = '/'.join(str(p) for p in parts)
        return path
    else:
        path = '/'.join(str(p) for p in parts)
        bucket = PKGSTORE_BUCKET
        if path.startswith('http'):
            parsed_url = urllib.parse.urlparse(path)
            path = parsed_url.path.lstrip('/')
            if path.startswith(bucket):
                _, path = path.split('/', 1)
        url = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket,
                'Key': path
            },
            ExpiresIn=3600*6)
        return url
