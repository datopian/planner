import os
from typing import List  # noqa
from typing import Tuple  # noqa
from datapackage import Resource  # noqa

import datapackage
from copy import deepcopy

import logging # noqa

from .node_collector import collect_artifacts
from .base_processing_node import ProcessingArtifact
from ..utilities import s3_path


def planner(datapackage_input, prefix, processing, outputs, allowed_types=None):
    parameters = datapackage_input.get('parameters')
    datapackage_url = datapackage_input['url']
    datapackage_descriptor = parameters.get('descriptor')

    resource_info = []
    dp = datapackage.DataPackage(datapackage_descriptor)
    for resource in dp.resources:  # type: Resource
        resource_info.append(deepcopy(resource.descriptor))

    for descriptor in resource_info:
        path = descriptor['path']
        if isinstance(path, list):
            path = path[0]
        if path.startswith('http'):
            url = path
        else:
            url = os.path.join(os.path.dirname(datapackage_url), path)
        descriptor['url'] = url
        descriptor['path'] = path

    # print('PLAN resource_info', resource_info)

    # Add types for all resources
    resource_mapping = parameters.get('resource-mapping', {})
    original_info = []
    for descriptor in resource_info:
        path = descriptor['path']
        name = descriptor['name']

        # Extract format from original path
        base, extension = os.path.splitext(descriptor['url'])
        extension = extension[1:]
        if extension and 'format' not in descriptor:
            descriptor['format'] = extension

        # Map original path if needed
        mapping = resource_mapping.get(path, resource_mapping.get(name))
        if mapping is not None:
            descriptor['url'] = mapping

        format = descriptor.get('format')

        # Augment url with format hint
        if format is not None:
            if not descriptor['url'].endswith(format):
                descriptor['url'] += '#.{}'.format(format)

        is_geojson = (
            (descriptor.get('format') == 'geojson') or
            (descriptor['url'].endswith('.geojson'))
        )

        # Hacky way to handle geojson files atm
        if is_geojson:
            schema = descriptor.get('schema')
            if schema is not None:
                del descriptor['schema']
                descriptor['geojsonSchema'] = schema

        descriptor['path'] = os.path.join('archive', '{}.{}'.format(name, extension))
        descriptor['datahub'] = {
            'type': 'original'
        }
        if 'schema' in descriptor:
            # keep original resource without processing steps for zip
            original_descriptor = deepcopy(descriptor)
            original_descriptor['name'] += '_original'
            original_info.append(original_descriptor)
            descriptor['datahub'] = {
                'type': 'source/tabular'
            }
    resource_info.extend(original_info)

    # Processing on resources
    processed_resources = set(p['input'] for p in processing)

    updated_resource_info = []
    for ri in resource_info:
        if ri['name'] not in processed_resources:
            updated_resource_info.append(ri)
            continue

        if ri['datahub']['type'] != 'source/tabular':
            updated_resource_info.append(ri)

        for p in processing:
            if p['input'] == ri['name']:
                # Make sure we have original resource if no schema provided
                ri['name'] += '_original'
                ri_ = deepcopy(ri)
                if 'tabulator' in p:
                    ri_.update(p['tabulator'])
                if 'schema' in p:
                    ri_['schema'] = p['schema']
                # Insert dpp in resource info for a while
                if 'dpp' in p:
                    ri_['dpp'] = [
                        (i['run'], i.get('parameters', {})) for i in p['dpp']
                    ]
                ri_['name'] = p['output']
                ri_['datahub']['type'] = 'source/tabular'
                updated_resource_info.append(ri_)

    resource_info = dict(
        (ri['name'], ri)
        for ri in updated_resource_info
    )

    # Create processing artifacts
    artifacts = [
        ProcessingArtifact(ri['datahub']['type'],
                           ri['name'],
                           [], [],
                           ri.pop('dpp', []),  # pop dpp form resource_info here
                           False)
        for ri in resource_info.values()
    ]
    for derived_artifact in collect_artifacts(artifacts, outputs, allowed_types):
        pipeline_steps: List[Tuple] = [
            ('add_metadata', dict(
                (k, v)
                for k, v in datapackage_descriptor.items()
                if k != 'resources'
            )),
        ]

        required_artifact_pipeline_steps = []
        needs_streaming = False
        stream_params = {}
        for required_artifact in derived_artifact.required_streamed_artifacts:
            ri = resource_info[required_artifact.resource_name]
            if 'resource' in ri:
                pipeline_steps.append(
                    ('assembler.load_private_resource', {
                        'url': s3_path(ri['url']),
                        'resource': ri['resource'],
                        'stream': True,
                        'limit-rows': required_artifact.limit_streamed_rows
                    })
                )
            else:
                if ri.get('signed') is None:
                    ri['signed'] = True
                    ri['url'] = s3_path(ri['url'])
                pipeline_steps.append(('add_resource', ri))
                needs_streaming = True
                if required_artifact.limit_streamed_rows:
                    stream_params['limit-rows'] = required_artifact.limit_streamed_rows
            required_artifact_pipeline_steps.extend(required_artifact.pipeline_steps)

        if needs_streaming:
            pipeline_steps.extend([
                ('stream_remote_resources', stream_params)
            ])

        pipeline_steps.extend(required_artifact_pipeline_steps)

        for required_artifact in derived_artifact.required_other_artifacts:
            ri = resource_info[required_artifact.resource_name]
            if 'resource' in ri:
                pipeline_steps.append(
                    ('assembler.load_private_resource', {
                        'url': ri['url'],
                        'resource': ri['resource'],
                        'stream': False
                    })
                )
            else:
                pipeline_steps.append(
                    ('add_resource', ri)
                )

        pipeline_steps.extend(derived_artifact.pipeline_steps)
        dependencies = [prefix + '/' + ra.resource_name
                        for ra in (derived_artifact.required_streamed_artifacts +
                                   derived_artifact.required_other_artifacts)
                        if ra.datahub_type not in ('source/tabular', 'original')]

        datapackage_url = yield (prefix + '/' + derived_artifact.resource_name,
                                 pipeline_steps, dependencies,
                                 derived_artifact.title, derived_artifact.content_type)

        resource_info[derived_artifact.resource_name] = {
            'resource': derived_artifact.resource_name,
            'url': datapackage_url
        }
