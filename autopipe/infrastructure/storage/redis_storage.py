from autopipe.infrastructure.storage.base_storage import (
    StorageBase,
    StorageConfig,
)
from xinghe.utils.json_util import json_dumps, json_loads
from typing import Dict, List, Optional, Any


class RedisStorageConfig(StorageConfig):
    host: str
    port: int
    password: str


class RedisStorage(StorageBase):
    def __init__(self, config: RedisStorageConfig):
        super().__init__(config)
        import redis

        self.client = redis.Redis(
            host=config.host, port=config.port, password=config.password
        )

    def register_pipeline(self, pipeline_id: str, pipeline_meta: dict) -> bool:
        pipeline_meta_str = json_dumps(pipeline_meta, ensure_ascii=False)
        return self.client.set(pipeline_id, pipeline_meta_str)

    def register_step(self, step_id: str, step_meta: dict) -> bool:
        step_meta_str = json_dumps(step_meta, ensure_ascii=False)
        return self.client.set(step_id, step_meta_str)

    def register_step_progress(
        self,
        step_id: str,
    ) -> bool:
        return self.client.set(step_id + "_progress", 0)

    def set_step_state(self, step_id: str, state: str) -> bool:
        step_meta = self.client.get(step_id)
        if step_meta:
            step_meta = json_loads(step_meta)
            step_meta["state"] = state
            step_meta_str = json_dumps(step_meta, ensure_ascii=False)
            return self.client.set(step_id, step_meta_str)
        return False

    def get_step_progress(self, step_id: str) -> int:
        return self.client.scard(step_id + "_progress")

    def update_step_progress(self, step_id: str, file_path: str) -> int:
        """向步骤进度集合添加文件路径"""
        # 添加文件路径到集合（自动去重）
        return self.client.sadd(step_id + "_progress", file_path)

    def get_step_state(self, step_id: str) -> Optional[str]:
        step_meta = self.client.get(step_id)
        if step_meta:
            step_meta = json_loads(step_meta)
            return step_meta["state"]
        return None

    def get_step_field(self, step_id: str, field: str) -> Optional[Any]:
        step_meta = self.client.get(step_id)
        if step_meta:
            step_meta = json_loads(step_meta)
            return step_meta[field]
        return None

    def update_step_field(self, step_id: str, field: str, value: Any) -> bool:
        step_meta = self.client.get(step_id)
        if step_meta:
            step_meta = json_loads(step_meta)
            step_meta[field] = value
            step_meta_str = json_dumps(step_meta, ensure_ascii=False)
            return self.client.set(step_id, step_meta_str)
        return False

    def get_pipeline_field(self, pipeline_id: str, field: str) -> Optional[Any]:
        pipeline_meta = self.client.get(pipeline_id)
        if pipeline_meta:
            pipeline_meta = json_loads(pipeline_meta)
            return pipeline_meta[field]
        return None

    def update_pipeline_field(self, pipeline_id: str, field: str, value: Any) -> bool:
        pipeline_meta = self.client.get(pipeline_id)
        if pipeline_meta:
            pipeline_meta = json_loads(pipeline_meta)
            pipeline_meta[field] = value
            pipeline_meta_str = json_dumps(pipeline_meta, ensure_ascii=False)
            return self.client.set(pipeline_id, pipeline_meta_str)
        return False

    def get_pipeline_meta(self, pipeline_id: str) -> Optional[Dict]:
        pipeline_meta = self.client.get(pipeline_id)
        if pipeline_meta:
            return json_loads(pipeline_meta)
        return None

    def get_step_meta(self, step_id: str) -> Optional[Dict]:
        step_meta = self.client.get(step_id)
        if step_meta:
            return json_loads(step_meta)
        return None
