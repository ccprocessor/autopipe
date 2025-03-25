from typing import Type, Dict, Optional, Any
from autopipe.pipeline.operator.registry import _REGISTRY
from default_ops import *


def get_operator(name: str, params_dict: Optional[Dict[str, Any]] = None) -> Any:
    """通过 operator_name 获取已注册的类并实例化"""

    if name not in _REGISTRY:
        raise KeyError(f"Operator '{name}' not found. Available: {list(_REGISTRY.keys())}")

    operator_cls = _REGISTRY[name]
    return operator_cls(params_dict=params_dict)


def list_registered_operators() -> Dict[str, str]:
    """返回所有已注册Operator的 {name: class_name}"""
    return {name: cls.__name__ for name, cls in _REGISTRY.items()}
