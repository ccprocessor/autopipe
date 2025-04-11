from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from typing import Iterator, Dict, Any, Iterable, Optional, Type
from xinghe.ml.actor import ModelActor


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


@register_operator
class ModelOperation(BaseOperation, ModelActor):
    # 必须定义 BaseOperation 的抽象属性
    operator_name = "model_operator"
    operator_type = "default"

    def __init__(
        self,
        # BaseOperation 参数
        params_dict: dict = None,
        # ModelActor 参数
        model_cls: Type = None,
        model_cls_kwargs: dict = None,
        batch_size: int = 512,
        num_workers: int = 0,
        timeout: int = 0,
        dataloader_kwargs: dict = None,
        verbose: bool = False,
    ):
        # 初始化 BaseOperation
        BaseOperation.__init__(self, params_dict)

        # 初始化 ModelActor
        ModelActor.__init__(
            self,
            model_cls=model_cls,
            model_cls_kwargs=model_cls_kwargs or {},
            batch_size=batch_size,
            num_workers=num_workers,
            timeout=timeout,
            dataloader_kwargs=dataloader_kwargs or {},
            verbose=verbose,
        )

    # 实现 BaseOperation 的抽象方法
    def process(self, data: dict) -> dict:
        # 调用 ModelActor 的处理逻辑
        results = list(self.handle([data]))  # 假设 handle 支持单条数据处理
        return results[0] if results else {}

    def resource_load(self):
        # 加载模型资源（可复用 ModelActor 的初始化逻辑）
        pass
