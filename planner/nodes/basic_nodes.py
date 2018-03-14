from mimetypes import guess_type

from .base_processing_node import BaseProcessingNode, ProcessingArtifact


class DerivedFormatProcessingNode(BaseProcessingNode):
    def __init__(self, available_artifacts, fmt):
        super(DerivedFormatProcessingNode, self).__init__(available_artifacts, [])
        self.fmt = fmt

    def get_artifacts(self):
        for artifact in self.available_artifacts:
            if artifact.datahub_type == 'source/tabular':
                datahub_type = 'derived/{}'.format(self.fmt)
                resource_name = artifact.resource_name + '_{}'.format(self.fmt)
                file_path = 'data/{}.{}'.format(resource_name, self.fmt)
                content_type, _ = guess_type(file_path)
                output = ProcessingArtifact(
                    datahub_type, resource_name,
                    [artifact], [],
                    [('assembler.update_resource',
                      {
                          'name': artifact.resource_name,
                          'update': {
                              'name': resource_name,
                              'format': self.fmt,
                              'path': file_path,
                              'datahub': {
                                'type': datahub_type,
                                'derivedFrom': [
                                    artifact.resource_name
                                ]
                              }
                          }
                      })],
                    True,
                    'Creating %s' % self.fmt.upper(),
                    content_type=content_type
                )
                yield output


class DerivedCSVProcessingNode(DerivedFormatProcessingNode):
    def __init__(self, available_artifacts, _):
        super(DerivedCSVProcessingNode, self).__init__(available_artifacts, 'csv')


class DerivedJSONProcessingNode(DerivedFormatProcessingNode):
    def __init__(self, available_artifacts, _):
        super(DerivedJSONProcessingNode, self).__init__(available_artifacts, 'json')


class OriginalProcessingNode(BaseProcessingNode):
    def __init__(self, available_artifacts, outputs):
        super(OriginalProcessingNode, self).__init__(available_artifacts, outputs)

    def get_artifacts(self):
        for artifact in self.available_artifacts:
            if artifact.datahub_type == 'original' or artifact.datahub_type == 'source/tabular':
                resource_name = artifact.resource_name
                # Avoid duplication: if resource name ends with original
                # and type is original -> is identical to source/tabular
                if resource_name.endswith('_original') and artifact.datahub_type == 'original':
                    continue
                output = ProcessingArtifact(
                    artifact.datahub_type, artifact.resource_name,
                    [], [artifact],
                    [('assembler.update_resource',
                      {
                          'name': resource_name,
                          'update': {
                              'name': resource_name,
                              'datahub': {
                                'type': artifact.datahub_type
                              }
                          }
                      })],
                    False,
                    'Copying source data'
                )
                yield output
