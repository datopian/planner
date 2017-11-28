from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, \
    NonTabularProcessingNode
from .view_nodes import DerivedPreviewProcessingNode
from .output_nodes import OutputToZipProcessingNode
from .report_nodes import ReportProcessingNode

import itertools

ORDERED_NODE_CLASSES = [
    ReportProcessingNode,
    DerivedCSVProcessingNode,
    DerivedPreviewProcessingNode,
    DerivedJSONProcessingNode,
    OutputToZipProcessingNode,
    NonTabularProcessingNode,
]


def collect_artifacts(artifacts, outputs, allowed_types=None):
    for cls in ORDERED_NODE_CLASSES:
        node = cls(artifacts, outputs)
        ret = list(node.get_artifacts())
        if allowed_types is not None:
            ret = filter(lambda a: a.datahub_type in allowed_types, ret)
        # Need to make copy of iter as it is consumed in artifacts.extend()
        for_artifacts, for_use = itertools.tee(ret)
        artifacts.extend(for_artifacts)
        yield from for_use
