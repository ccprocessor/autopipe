from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from typing import Iterator, Dict, Any, Iterable, Optional, Type


@register_operator
class Operation1(BaseOperation):
    """示例算子1"""

    operator_name = "op1"
    operator_type = "default"

    def resource_load(self):
        """加载资源"""
        print("load resource for op1")

    def process(self, data: dict) -> dict:
        """处理数据"""
        data["op1"] = "processed"
        return data


@register_operator
class Operation2(BaseOperation):
    """示例算子2"""

    operator_name = "op2"
    operator_type = "default"

    def resource_load(self):
        """加载资源"""
        print("load resource for op2")

    def process(self, data: dict) -> dict:
        """处理数据"""
        data["op2"] = 100
        return data


@register_operator
class Operation3(BaseOperation):
    """示例算子3"""

    operator_name = "op3"
    operator_type = "default"

    def resource_load(self):
        """加载资源"""
        print("load resource for op3")

    def process(self, data: dict) -> dict:
        """处理数据"""
        data["op3"] = "processed"
        return data


@register_operator
class Operation4(BaseOperation):
    """示例算子4"""

    operator_name = "op4"
    operator_type = "default"

    def resource_load(self):
        """加载资源"""
        print("load resource for op4")

    def process(self, data: dict) -> dict:
        """处理数据"""
        data["op4"] = 100
        return data


@register_operator
class Operation5(BaseOperation):
    """示例算子4"""

    operator_name = "op_stream_1"
    operator_type = "default"

    def resource_load(self):
        """加载资源"""
        print("load resource for op_stream_1")

    def process(self, data: dict) -> dict:
        """处理数据"""
        data["op_stream_1"] = 100
        return data

    def handle(
        self,
        _iter: Iterable[dict],
        input_file: str,
        output_file: str = "",
    ) -> Iterable:
        for d in _iter:
            d = self.process(d)
            yield d
