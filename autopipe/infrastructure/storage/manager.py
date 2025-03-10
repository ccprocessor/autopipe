from infrastructure.storage.base_storage import StorageConfig, StorageMode
from infrastructure.storage.redis_storage import RedisStorage


def get_storage(config: StorageConfig):
    match config.mode:
        case StorageMode.redis:
            return RedisStorage(config=config)
        case _:
            raise NotImplementedError(f"Storage mode {config.mode} is not supported")
