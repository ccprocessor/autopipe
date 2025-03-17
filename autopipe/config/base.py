import json
import os
from types import SimpleNamespace
from typing import Any, Dict, Optional


class ConfigLoader:
    """智能 JSON 配置文件加载器"""

    def __init__(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"配置文件未找到: {file_path}")
        self.file_path = file_path
        self._data = self._load_config()

        # 转换为对象属性访问（关键修改）
        converted_obj = self._convert_to_object(self._data)
        self.__dict__.update(converted_obj.__dict__)  # 使用 __dict__ 属性

    def _load_config(self) -> Dict[str, Any]:
        """加载并解析 JSON 文件"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"配置文件格式错误: {str(e)}")

    def _convert_to_object(self, data: Any) -> Any:
        """递归转换字典为对象属性（安全版本）"""
        if isinstance(data, dict):
            # 处理字典：仅转换 value 中的 dict/list，其他类型保持原样
            return SimpleNamespace(**{
                k: self._convert_to_object(v)
                for k, v in data.items()
            })
        elif isinstance(data, list):
            # 处理列表：仅转换元素中的 dict/list，其他类型保持原样
            return [
                self._convert_to_object(item)
                if isinstance(item, (dict, list))
                else item
                for item in data
            ]
        else:
            # 非 dict/list 类型直接返回
            return data

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """安全获取配置参数（支持点分嵌套访问）"""
        keys = key.split('.')
        val = self._data
        try:
            for k in keys:
                val = val[k]
            return val
        except (KeyError, TypeError):
            return default

    def reload(self) -> None:
        """热重载配置文件"""
        self.__dict__.clear()
        self._data = self._load_config()
        self.__dict__.update(self._convert_to_object(self._data))

    def __repr__(self) -> str:
        return f"<ConfigLoader file='{self.file_path}'>"


if __name__ == "__main__":
    try:
        # 初始化配置加载器
        config = ConfigLoader("C:/Users/chenhaoling/PycharmProjects/autopipe/examples/test_config.json")

        # 属性式访问（支持智能提示）
        print("数据库配置:", config.meta_storage)
        print("字段:", dir(config))

    except Exception as e:
        print(f"配置加载失败: {str(e)}")
