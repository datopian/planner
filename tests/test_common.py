# -*- coding: utf-8 -*-
import pytest

import planner
from .utilities import get_flow_sink

from .config import SPECS, CONFIGS


@pytest.mark.parametrize('spec', SPECS)
@pytest.mark.parametrize('config', CONFIGS)
def test_connected(spec, config):
    """Tests that the generated grapיh is connected"""
    flow = planner.plan(1, spec, **config)
    connected_set = set()
    added = True
    while added:
        added = False
        for pipeline_id, pipeline in flow:
            if pipeline_id not in connected_set:
                deps = [x['pipeline'] for x in pipeline.get('dependencies')]
                print(pipeline_id, deps)
                if all(dep in connected_set for dep in deps):
                    connected_set.add(pipeline_id)
                    added = True
    assert len(connected_set) == len([pipeline_id for pipeline_id, _ in flow])


@pytest.mark.parametrize('spec', SPECS)
@pytest.mark.parametrize('config', CONFIGS)
def test_different_pipeline_ids(spec, config):
    """Tests that the generated grapיh has different pipline ids"""
    flow = planner.plan(1, spec, **config)
    pipeline_ids = set(pipeline_id for pipeline_id, pipeline in flow)
    assert len(pipeline_ids) == len(flow)


@pytest.mark.parametrize('spec', SPECS)
@pytest.mark.parametrize('config', CONFIGS)
def test_only_one_sink(spec, config):
    """Tests that the generated grapיh has only one sink"""
    flow = planner.plan(1, spec, **config)
    assert get_flow_sink(flow) is not None
