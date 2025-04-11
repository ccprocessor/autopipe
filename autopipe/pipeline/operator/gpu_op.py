from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from typing import Iterator, Dict, Any, Iterable, Optional, Type
from xinghe.ml.actor import ModelActor


@register_operator
class ModelOperation(BaseOperation, ModelActor):
    # 必须定义 BaseOperation 的抽象属性
    operator_name = "model_operator"
    operator_type = "gpu_model"

    def __init__(
        self,
        # BaseOperation 参数
        params_dict: dict = None,
    ):
        from loguru import logger

        logger.info("start model operation init")
        # 初始化 BaseOperation
        BaseOperation.__init__(self, params_dict)

    # 实现 BaseOperation 的抽象方法
    def process(self, data: dict) -> dict:
        pass

    def resource_load(self):
        pass
