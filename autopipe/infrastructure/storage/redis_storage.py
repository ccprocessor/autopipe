from autopipe.infrastructure.storage.base_storage import (
    StorageBase,
    StorageConfig,
)


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
