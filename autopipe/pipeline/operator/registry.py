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


def get_operator(name: str, params_dict: Optional[Dict[str, Any]] = None) -> Any:
    """通过 operator_name 获取已注册的类并实例化"""

    if name not in _REGISTRY:
        raise KeyError(f"Operator '{name}' not found. Available: {list(_REGISTRY.keys())}")

    operator_cls = _REGISTRY[name]
    return operator_cls(params_dict=params_dict)


def list_registered_operators() -> Dict[str, str]:
    """返回所有已注册Operator的 {name: class_name}"""
    return {name: cls.__name__ for name, cls in _REGISTRY.items()}