from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from typing import Iterator, Dict, Any, Iterable, Optional, Type
from xinghe.s3 import (
    S3DocWriter,
    head_s3_object_with_retry,
    is_s3_path,
    put_s3_object_with_retry,
    read_s3_rows,
)
from xinghe.utils.json_util import json_loads
from xinghe.dp.base import SkipTask


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


class combine_label(BaseOperation):
    """示例算子6"""

    operator_name = "combine_label  "
    operator_type = "default"

    def process(self, data: dict) -> dict:
        """处理单条数据"""
        pass

    def handle(
        data_iter: Iterable[dict], combing_cols, input_file, output_file
    ):  # 明确声明接收可迭代的字典流
        if not input_file:
            raise SkipTask("missing [input_file]")
        if not is_s3_path(input_file):
            raise SkipTask(f"invalid input_file [{input_file}]")
        if output_file and not is_s3_path(output_file):
            raise SkipTask(f"invalid output_file [{output_file}]")

        input_head = head_s3_object_with_retry(input_file)
        if not input_head:
            raise SkipTask(f"file [{input_file}] not found")

        use_stream = 2 << 30

        input_file_rows = read_s3_rows(input_file, use_stream)

        for d1, d2 in zip(data_iter, input_file_rows):
            d2 = json_loads(d2.value)
            d2.update(d1)
            yield d2
