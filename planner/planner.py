# -*- coding: utf-8 -*-
import os

from datapackage_pipelines.generators import steps

from .nodes.planner import planner
from .utilities import dump_steps


def _plan(revision, spec, **config):
    """Plan a flow according to spec"""
    meta = spec['meta']

    flow_id = '{ownerid}/{dataset}/{revision}'.format(**meta, revision=revision)
    dataset_id = '{ownerid}/{dataset}'.format(**meta)

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
                        'flowid': flow_id,
                        # 'stats': {
                        #     'rowcount': 0,
                        #     'bytes': 0,
                        # },
                        'modified': update_time,
                        'id': dataset_id
                    })

    def planner_pipelines():
        planner_gen = planner(input,
                              flow_id,
                              spec.get('processing', []),
                              outputs,
                              **config)
        inner_pipeline_id = None
        while True:
            inner_pipeline_id, pipeline_steps, dependencies, title, content_type = planner_gen.send(inner_pipeline_id)
            inner_pipeline_ids.append(inner_pipeline_id)

            pid_without_revision = inner_pipeline_id.replace('/{}/'.format(revision), '/')

            pipeline_steps.insert(0, datahub_step)
            pipeline_steps.extend(dump_steps(pid_without_revision, content_type=content_type))
            dependencies = [dict(pipeline='./'+d) for d in dependencies]

            pipeline = {
                'pipeline': steps(*pipeline_steps),
                'dependencies': dependencies,
                'title': title
            }
            yield inner_pipeline_id, pipeline
            inner_pipeline_id = 'dependency://./'+inner_pipeline_id

    yield from planner_pipelines()

    dependencies = [dict(pipeline='./'+pid) for pid in inner_pipeline_ids]
    datapackage_descriptor = input['parameters']['descriptor']
    final_steps = [
        ('add_metadata', dict(
            (k, v)
            for k, v in datapackage_descriptor.items()
            if k != 'resources'
        )),
        datahub_step,
        ('assembler.load_modified_resources',
         {
             'urls': dependencies
         }),
    ]
    final_steps.extend(dump_steps(flow_id, content_type='application/json', final=True))
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
    yield flow_id, pipeline


def plan(revision, spec, **config):
    return list(_plan(revision, spec, **config))
