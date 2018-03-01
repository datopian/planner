# -*- coding: utf-8 -*-
import os

from datapackage_pipelines.generators import steps

from .nodes.planner import planner
from .utilities import s3_path, dump_steps


def _plan(revision, spec, **config):
    """Plan a flow according to spec"""
    meta = spec['meta']

    def pipeline_id(r=None):
        if r is not None:
            return '{ownerid}/{dataset}/{revision}/{suffix}'.format(**meta, suffix=r, revision=revision)
        else:
            return '{ownerid}/{dataset}/{revision}'.format(**meta, revision=revision)

    def flow_id():
        return pipeline_id()

    def dataset_id():
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
                'out-file': '%s.zip' % (meta['dataset'])
            }
        }
        outputs.append(zip_output)

    datahub_step = ('assembler.update_metadata',
                    {
                        'ownerid': ownerid,
                        'owner': owner,
                        'findability': findability,
                        'flowid': flow_id(),
                        # 'stats': {
                        #     'rowcount': 0,
                        #     'bytes': 0,
                        # },
                        'modified': update_time,
                        'id': dataset_id()
                    })

    def planner_pipelines():
        planner_gen = planner(input,
                              spec.get('processing', []),
                              outputs,
                              **config)
        datapackage_url = None
        while True:
            inner_pipeline_id, pipeline_steps, dependencies, title, content_type = planner_gen.send(datapackage_url)
            inner_pipeline_id = pipeline_id(inner_pipeline_id)
            inner_pipeline_ids.append(inner_pipeline_id)

            pid_without_revision = inner_pipeline_id.replace('/{}/'.format(revision), '/')
            datapackage_url = s3_path(pid_without_revision, 'datapackage.json')
            urls.append(datapackage_url)

            path_without_revision = inner_pipeline_id.replace(
                '/{}/'.format(revision), '/')
            pipeline_steps.insert(0, datahub_step)
            pipeline_steps.extend(dump_steps(path_without_revision, content_type=content_type))
            dependencies = [dict(pipeline='./'+pipeline_id(r)) for r in dependencies]

            pipeline = {
                'pipeline': steps(*pipeline_steps),
                'dependencies': dependencies,
                'title': title
            }
            yield inner_pipeline_id, pipeline

    yield from planner_pipelines()

    dependencies = [dict(pipeline='./'+pid) for pid in inner_pipeline_ids]
    final_steps = [
        ('load_metadata',
         {
             'url': input['url']
         }),
        datahub_step,
        ('assembler.load_modified_resources',
         {
             'urls': urls
         }),
        ('assembler.sample',),
    ]
    final_steps.extend(dump_steps(ownerid, dataset, revision, final=True, content_type='application/json'))
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
        'title': 'Creating Package'
    }
    # print('yielding', pipeline_id(), pipeline)
    yield pipeline_id(), pipeline


def plan(revision, spec, **config):
    return list(_plan(revision, spec, **config))
