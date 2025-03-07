from enum import Enum


class StageState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    STOPPED = "stopped"


class EngineType(Enum):
    SPARK = "spark"
    RAY = "ray"
    LOCAL = "local"  # 用于单机
