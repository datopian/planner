from typing import List


class ProcessingArtifact():
    def __init__(self,
                 datahub_type,
                 resource_name,
                 required_streamed_artifacts: List['ProcessingArtifact'],
                 required_other_artifacts: List['ProcessingArtifact'],
                 pipeline_steps,
                 streamable,
                 title='',
                 limit_streamed_rows=None,
                 content_type='application/octet-stream'):
        self.datahub_type = datahub_type
        self.resource_name = resource_name
        self.required_streamed_artifacts = required_streamed_artifacts
        self.required_other_artifacts = required_other_artifacts
        self.pipeline_steps = pipeline_steps
        self.streamable = streamable
        self.title = title
        self.limit_streamed_rows = limit_streamed_rows
        self.content_type = content_type


class BaseProcessingNode():
    def __init__(self,
                 available_artifacts: List[ProcessingArtifact],
                 outputs):
        self.available_artifacts = available_artifacts[:]
        self.outputs = outputs

    def get_artifacts(self):
        raise NotImplemented() # noqa
