from .base_processing_node import BaseProcessingNode, ProcessingArtifact


class ReportProcessingNode(BaseProcessingNode):
    def __init__(self, available_artifacts, outputs):
        super(ReportProcessingNode, self).__init__(available_artifacts, outputs)
        self.outputs = outputs
        self.fmt = 'json'

    def get_artifacts(self):
        datahub_type = 'derived/report'
        resource_name = 'validation_report'
        tabular_artifacts = [
            artifact for artifact in self.available_artifacts
            if artifact.datahub_type == 'source/tabular'
        ]
        output = ProcessingArtifact(
            datahub_type, resource_name,
            [], tabular_artifacts,
            [('assembler.validate_resource', {})],
            False)
        yield output
