import os
import random

from .base_processing_node import BaseProcessingNode, ProcessingArtifact


class OutputToZipProcessingNode(BaseProcessingNode):
    def __init__(self, available_artifacts, outputs):
        super(OutputToZipProcessingNode, self).__init__(available_artifacts, outputs)
        self.outputs = outputs
        self.fmt = 'zip'

    def get_artifacts(self):
        zip_params = [out for out in self.outputs if out['kind'] == 'zip']
        if len(zip_params):
            out_file = zip_params[0]['parameters']['out-file']
            # add random number to avoid collisions
            tmp_zip = os.path.join('/tmp', '%s.%s' % (
                random.randrange(1000), out_file))
            datahub_type = 'derived/{}'.format(self.fmt)
            resource_name = out_file.replace('.', '_')
            # Exclude source/tabular as in zip it's duplicate of derived/csv
            artifacts = [
                a for a in self.available_artifacts if a.datahub_type != 'source/tabular'
            ]
            output = ProcessingArtifact(
                datahub_type, resource_name,
                [], artifacts,
                [('assembler.extract_readme', {}),
                 ('assembler.remove_hash', {}),
                 ('dump.to_zip', {
                    'out-file': tmp_zip,
                    'force-format': False,
                    'handle-non-tabular': True,
                    'pretty-descriptor': True
                    }),
                 ('assembler.clear_resources', {}),
                 ('add_resource', {
                    'url': tmp_zip,
                    'name': resource_name,
                    'format': self.fmt,
                    'path': 'data/{}.zip'.format(resource_name),
                    'datahub': {
                      'type': "derived/zip",
                    },
                    'description': 'Compressed versions of dataset. Includes normalized CSV and JSON data with original data and datapackage.json.' #noqa
                 })],
                False, 'Creating ZIP', content_type='application/zip'
            )
            yield output
