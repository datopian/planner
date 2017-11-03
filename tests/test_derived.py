# -*- coding: utf-8 -*-
import pytest

import planner

from .utilities import get_flow_sink
from .config import SPECS, CONFIGS


@pytest.mark.parametrize('spec', SPECS)
@pytest.mark.parametrize('config', CONFIGS)
def test_partial(spec, config):
    """Tests for a derived resource to exist"""
    flow = planner.plan(1, spec, **config)

    formats = []
    fmt = res_name = None
    for pipeline_id, pipeline_details in flow:
        for step in pipeline_details['pipeline']:
            if step['run'] == 'assembler.update_resource':
                params = step['parameters']['update']
                datahub_type = params['datahub']['type']
                if datahub_type == 'derived/preview':
                    continue
                fmt = params['format']
                assert params['datahub']['type'] == 'derived/' + fmt
                assert len(params['datahub']['derivedFrom']) == 1
                res_name = params['name']
            elif step['run'].endswith('to_s3'):
                params = step['parameters']
                formats.append((params['path'], res_name, fmt))

    expected_num = len(config['allowed_types']) if spec['meta']['dataset'] == \
        'empty' else len(config['allowed_types']) + 1
    assert expected_num == len(formats)
