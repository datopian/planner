import os


def dump_steps(*parts, final=False):
    dumper = 'assembler.dump_to_s3' if final else 'aws.dump.to_s3'
    handle_non_tabular = False if final else True
    if os.environ.get('PLANNER_LOCAL'):
        return [('dump.to_path',
                 {
                     'force-format': False,
                     'handle-non-tabular': handle_non_tabular,
                     'add-filehash-to-path': True,
                     'out-path': '/'.join(str(p) for p in parts),
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
        return [(dumper,
                 {
                     'force-format': False,
                     'handle-non-tabular': handle_non_tabular,
                     'add-filehash-to-path': True,
                     'bucket': os.environ['PKGSTORE_BUCKET'],
                     'path': '/'.join(str(p) for p in parts),
                     'counters': {
                         "datapackage-rowcount": "datahub.stats.rowcount",
                         "datapackage-bytes": "datahub.stats.bytes",
                         "datapackage-hash": "datahub.hash",
                         "resource-rowcount": "rowcount",
                         "resource-bytes": "bytes",
                         "resource-hash": "hash",
                     }
                 })]


def s3_path(*parts):
    if os.environ.get('PLANNER_LOCAL'):
        path = '/'.join(str(p) for p in parts)
        return path
    else:
        path = '/'.join(str(p) for p in parts)
        bucket = os.environ['PKGSTORE_BUCKET']
        # Handle other s3 compatible server as well (for testing)
        protocol = os.environ.get('S3_ENDPOINT_URL') or 'https://'
        return '{}{}/{}'.format(protocol, bucket, path)
