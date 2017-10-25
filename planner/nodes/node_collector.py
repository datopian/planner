from .basic_nodes import DerivedCSVProcessingNode, DerivedJSONProcessingNode, \
    NonTabularProcessingNode
from .view_nodes import DerivedPreviewProcessingNode
from .output_nodes import OutputToZipProcessingNode

ORDERED_NODE_CLASSES = [
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
        artifacts.extend(ret)
        yield from ret
