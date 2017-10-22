import os


def dump_steps(*parts):
    if os.environ.get('PLANNER_LOCAL'):
        return [('dump.to_path',
                 {
                     'force-format': False,
                     'handle-non-tabular': True,
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
        return [('assembler.dump_to_s3',
                 {
                     'force-format': False,
                     'handle-non-tabular': True,
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
        return 'https://{}/{}'.format(bucket, path)
