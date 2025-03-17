from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional


class BaseOperation(ABC):
    # 强制子类定义类属性 (通过元类检查)
    operator_name: str
    operator_type: str

    def __init__(self, op_name: str, params_dict: dict = None):
        self.op_name = op_name
        self.params_dict = params_dict or {}

    @abstractmethod
    def process(self, data: dict) -> dict:
        pass

    @abstractmethod
    def resource_load(self):
        pass
