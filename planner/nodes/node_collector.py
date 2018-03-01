from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, \
    NonTabularProcessingNode
from .view_nodes import DerivedPreviewProcessingNode
from .output_nodes import OutputToZipProcessingNode
from .report_nodes import ReportProcessingNode


ORDERED_NODE_CLASSES = [
    ReportProcessingNode,
    DerivedCSVProcessingNode,
    DerivedJSONProcessingNode,
    OutputToZipProcessingNode,
    DerivedPreviewProcessingNode,
    NonTabularProcessingNode,
]


def collect_artifacts(artifacts, outputs, allowed_types=None):
    for cls in ORDERED_NODE_CLASSES:
        node = cls(artifacts, outputs)
        ret = list(node.get_artifacts())
        if allowed_types is not None:
            ret = list(filter(lambda a: a.datahub_type in allowed_types, ret))
        artifacts.extend(ret)
        yield from ret
