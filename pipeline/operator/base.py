from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional


class BaseOperator(ABC):
    """Base class for data processing operators with enforced naming."""

    # 强制子类定义类属性 (通过元类检查)
    operator_name: str
    operator_type: str
    default_engine: str

    def __init__(self, params_dict: Optional[dict] = None):
        self.params_dict = params_dict or {}

    def process_row(self, row: Any) -> Any:
        """Process single row. Subclasses MUST implement if not overriding process_partition."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} must implement either "
            "'process_row' or 'process_partition' method"
        )

    def process_iter(self, iterator: Iterable[Any]) -> Iterable[Any]:
        """Process entire partition. Defaults to iterating over process_row."""
        return (self.process_row(row) for row in iterator)