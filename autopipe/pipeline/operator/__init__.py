from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from autopipe.pipeline.operator.get_op import get_operator
from autopipe.pipeline.operator.default_ops import (
    CleanModelDemo,
)
from autopipe.pipeline.operator.examples import (
    Operation1,
    Operation2,
    Operation3,
    Operation4,
)
from autopipe.pipeline.operator.gpu_op import MinerUExtract

__all__ = [
    "BaseOperation",
    "register_operator",
    "get_operator",
    "Operation1",
    "Operation2",
    "Operation3",
    "Operation4",
    "CleanModelDemo",
    "MinerUExtract",
]
