def get_flow_sink(flow):
    dependents = set()
    for pipeline_id, pipeline in flow:
        deps = [x['pipeline'] for x in pipeline.get('dependencies')]
        dependents.update(deps)
    if len(dependents) == len(flow) - 1:
        return [(pipeline_id, pipeline) for pipeline_id, pipeline in flow if pipeline_id not in dependents][0]
    return None
