from abc import ABC, abstractmethod
import os
import boto3
from urllib.parse import urlparse
import gzip


class IOBase(ABC):
    """抽象基类，定义统一接口"""

    @abstractmethod
    def read_file(self, path: str) -> str:
        pass

    @abstractmethod
    def write_file(self, path: str, content: str):
        pass

    @abstractmethod
    def list_dir(self, path: str) -> list:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def delete(self, path: str):
        pass


class LocalIO(IOBase):
    """本地文件系统实现"""

    def read_file(self, path: str) -> str:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def write_file(self, path: str, content: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

    def list_dir(self, path: str) -> list:
        return os.listdir(path)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def delete(self, path: str):
        if os.path.isdir(path):
            os.rmdir(path)
        else:
            os.remove(path)

    def read_stream(self, path: str):
        # 自动处理 gzip
        if path.endswith('.gz'):
            return gzip.open(path, 'rt', encoding='utf-8')
        return open(path, 'r', encoding='utf-8')

    def write_stream(self, path: str):
        # 自动创建目录
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        # 自动处理 gzip
        if path.endswith('.gz'):
            return gzip.open(path, 'wt', encoding='utf-8')
        return open(path, 'w', encoding='utf-8')


class S3IO(IOBase):
    """S3存储实现"""
    """待实现，结合xinghe"""

    def read_file(self, path: str) -> str:
        pass

    def write_file(self, path: str, content: str):
        pass

    def list_dir(self, path: str) -> list:
        pass

    def exists(self, path: str) -> bool:
        pass

    def delete(self, path: str):
        pass


class IO:
    """工厂类，根据类型返回对应IO实例"""

    def __new__(cls, io_type: str = 'local', **kwargs):
        if io_type == 'local':
            return LocalIO(**kwargs)
        elif io_type == 's3':
            return S3IO(**kwargs)
        else:
            raise ValueError(f"Unsupported IO type: {io_type}")
