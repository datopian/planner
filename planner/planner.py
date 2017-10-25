# -*- coding: utf-8 -*-
from datapackage_pipelines.generators import steps

from .nodes.planner import planner
from .utilities import s3_path, dump_steps


def _plan(revision, spec, **config):
    """Plan a flow according to spec"""
    meta = spec['meta']

    def pipeline_id(r=None):
        if r is not None:
            return '{ownerid}/{dataset}:{suffix}'.format(**meta, suffix=r)
        else:
            return '{ownerid}/{dataset}'.format(**meta)

    ownerid = meta['ownerid']
    owner = meta.get('owner')
    findability = meta.get('findability', 'published')
    update_time = meta.get('update_time')
    schedule = spec.get('schedule', {})

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
                'out-file': '%s.zip' % meta.get('dataset', 'datahub')
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
            inner_pipeline_id, pipeline_steps, dependencies = planner_gen.send(datapackage_url)
            inner_pipeline_id = pipeline_id(inner_pipeline_id)
            inner_pipeline_ids.append(inner_pipeline_id)

            datapackage_url = s3_path(inner_pipeline_id, 'datapackage.json')
            urls.append(datapackage_url)

            pipeline_steps.extend(dump_steps(inner_pipeline_id))
            dependencies = [dict(pipeline=pipeline_id(r)) for r in dependencies]

            pipeline = {
                'pipeline': steps(*pipeline_steps),
                'dependencies': dependencies,
                'schedule': schedule
            }
            yield inner_pipeline_id, pipeline

    yield from planner_pipelines()

    dependencies = [dict(pipeline=pid) for pid in inner_pipeline_ids]
    # print(dependencies)
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
             'id': pipeline_id()
         }),
        ('assembler.load_modified_resources',
         {
             'urls': urls
         }),
        ('assembler.sample',),
    ]
    final_steps.extend(dump_steps(pipeline_id(), 'latest'))
    final_steps.append(('assembler.add_indexing_resource', {
        'flow-id': pipeline_id()
    }))
    final_steps.append(
        ('elasticsearch.dump.to_index',
         {
             'indexes': {
                 'datahub': [
                     {
                         'resource-name': '__datasets',
                         'doc-type': 'dataset'
                     }
                 ],
                 'events': [
                    {
                        'resource-name': '__events',
                        'doc-type': 'event'
                     }
                 ]
             }
         })
    )
    pipeline = {
        'update_time': update_time,
        'dependencies': dependencies,
        'pipeline': steps(*final_steps)
    }
    # print('yielding', pipeline_id(), pipeline)
    yield pipeline_id(), pipeline


def plan(revision, spec, **config):
    return list(_plan(revision, spec, **config))
