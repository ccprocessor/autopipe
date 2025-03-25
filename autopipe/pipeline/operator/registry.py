from typing import Type, Dict, Optional, Any

# 全局注册表（私有变量，外部不可直接修改）
_REGISTRY: Dict[str, Type] = {}


def register_operator(cls: Type) -> Type:
    """类装饰器：将继承自BaseOperator的子类注册到全局注册表"""

    # 验证是否为有效Operator类
    if not hasattr(cls, 'operator_name'):
        raise TypeError(f"Class {cls.__name__} must define 'operator_name' class attribute")
    if not hasattr(cls, 'operator_type'):
        raise TypeError(f"Class {cls.__name__} must define 'operator_type' class attribute")

    name = cls.operator_name
    if name in _REGISTRY:
        raise KeyError(f"Operator name '{name}' already registered by {_REGISTRY[name].__name__}")

    _REGISTRY[name] = cls
    return cls
