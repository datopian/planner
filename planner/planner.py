# -*- coding: utf-8 -*-
import os

from datapackage_pipelines.generators import steps

from .nodes.planner import planner
from .utilities import s3_path, dump_steps

FLOWMANAGER_HOOK_URL = os.environ.get('FLOWMANAGER_HOOK_URL')


def _plan(revision, spec, **config):
    """Plan a flow according to spec"""
    meta = spec['meta']

    def pipeline_id(r=None):
        if r is not None:
            return '{ownerid}/{dataset}/{revision}/{suffix}'.format(**meta, suffix=r, revision=revision)
        else:
            return '{ownerid}/{dataset}/{revision}'.format(**meta, revision=revision)

    def flow_id():
        return '{ownerid}/{dataset}'.format(**meta)

    ownerid = meta['ownerid']
    dataset = meta['dataset']
    owner = meta.get('owner')
    findability = meta.get('findability', 'published')
    acl = 'public-read'
    if findability == 'private':
        acl = 'private'
    update_time = meta.get('update_time')

    inputs = spec.get('inputs', [])
    assert len(inputs) == 1, 'Only supporting one input atm'

    input = inputs[0]
    assert input['kind'] == 'datapackage', 'Only supporting datapackage inputs atm'

    urls = []
    inner_pipeline_ids = []

    outputs = spec.get('outputs', [])
    zip_there = any(output['kind'] == 'zip' for output in outputs)
    if not zip_there:
        zip_output = {
            'kind': 'zip',
            'parameters': {
                'out-file': '/tmp/%s.%s.%s.zip' % (meta['ownerid'], meta['dataset'], revision)
            }
        }
        outputs.append(zip_output)

    def planner_pipelines():
        planner_gen = planner(input,
                              spec.get('processing', []),
                              outputs,
                              **config)
        datapackage_url = None
        while True:
            inner_pipeline_id, pipeline_steps, dependencies, title = planner_gen.send(datapackage_url)
            inner_pipeline_id = pipeline_id(inner_pipeline_id)
            inner_pipeline_ids.append(inner_pipeline_id)

            pid_without_revision = inner_pipeline_id.replace('/{}/'.format(revision), '/')
            datapackage_url = s3_path(pid_without_revision, 'datapackage.json')
            urls.append(datapackage_url)

            path_without_revision = inner_pipeline_id.replace(
                '/{}/'.format(revision), '/')
            pipeline_steps.extend(dump_steps(path_without_revision))
            dependencies = [dict(pipeline=pipeline_id(r)) for r in dependencies]

            pipeline = {
                'pipeline': steps(*pipeline_steps),
                'dependencies': dependencies,
                'hooks': [FLOWMANAGER_HOOK_URL],
                'title': title
            }
            yield inner_pipeline_id, pipeline

    yield from planner_pipelines()

    dependencies = [dict(pipeline=pid) for pid in inner_pipeline_ids]
    final_steps = [
        ('load_metadata',
         {
             'url': input['url']
         }),
        ('assembler.update_metadata',
         {
             'ownerid': ownerid,
             'owner': owner,
             'findability': findability,
             'stats': {
                 'rowcount': 0,
                 'bytes': 0,
             },
             'modified': update_time,
             'id': flow_id()
         }),
        ('assembler.load_modified_resources',
         {
             'urls': urls
         }),
        ('assembler.sample',),
    ]
    final_steps.extend(dump_steps(ownerid, dataset, 'latest', final=True))
    final_steps.append(
        ('elasticsearch.dump.to_index',
         {
             'indexes': {
                 'datahub': [
                     {
                         'resource-name': '__datasets',
                         'doc-type': 'dataset'
                     }
                 ]
             }
         })
    )
    if not os.environ.get('PLANNER_LOCAL'):
        final_steps.append(('aws.change_acl', {
            'bucket': os.environ['PKGSTORE_BUCKET'],
            'path': '{}/{}'.format(ownerid, dataset),
            'acl': acl
        }))
    pipeline = {
        'update_time': update_time,
        'dependencies': dependencies,
        'pipeline': steps(*final_steps),
        'hooks': [FLOWMANAGER_HOOK_URL],
        'title': 'Creating Package'
    }
    # print('yielding', pipeline_id(), pipeline)
    yield pipeline_id(), pipeline


def plan(revision, spec, **config):
    return list(_plan(revision, spec, **config))
