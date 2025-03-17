from abc import ABC, abstractmethod
from enum import Enum

from pydantic import BaseModel

from autopipe.pipeline.constants import StageState


class StorageMode(Enum):
    redis = "redis"


class StorageConfig(BaseModel):
    mode: StorageMode


class StorageBase(ABC):
    def __init__(self, config: StorageConfig):
        self.mode = config.mode

    @abstractmethod
    def register_step(self, stage_id: str, stage_meta: dict) -> bool:
        pass

    @abstractmethod
    def register_step_progress(self, stage_id: str,) -> bool:
        pass

    # @abstractmethod
    # def register_pipeline(self, pipeline_id: str, pipeline: dict) -> str:
    #     pass
    #
    # @abstractmethod
    # def list_pipelines(self) -> list:
    #     pass
    #
    # @abstractmethod
    # def get_pipeline(self, pipeline_id: str) -> dict:
    #     pass
    #
    # @abstractmethod
    # def get_pipeline_state(self, pipeline_id: str) -> str:
    #     pass
    #
    # @abstractmethod
    # def update_pipeline(self, pipeline_id: str, pipeline: dict) -> dict:
    #     pass
    #
    # @abstractmethod
    # def update_pipeline_state(self, pipeline_id: str, state: str) -> str:
    #     pass
    #
    # @abstractmethod
    # def delete_pipeline(self, pipeline_id: str) -> None:
    #     pass
    #
    # @abstractmethod
    # def get_stage_state(self, pipeline_id: str, stage_id: str) -> StageState:
    #     pass
    #
    # @abstractmethod
    # def get_stage_progress(self, pipeline_id: str, stage_id: str) -> dict:
    #     pass
    #
    # @abstractmethod
    # def update_stage_state(
    #     self, pipeline_id: str, stage_id: str, state: StageState
    # ) -> StageState:
    #     pass
    #
    # @abstractmethod
    # def update_stage_progress(
    #     self, pipeline_id: str, stage_id: str, progress: dict
    # ) -> dict:
    #     pass
