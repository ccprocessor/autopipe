import os.path
import time
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict, List, Optional, Type
from autopipe.infrastructure.storage import get_storage
from autopipe.infrastructure.io.base import IO
from xinghe.io.kafka import KafkaWriter
from xinghe.s3 import list_s3_objects
from itertools import tee


class EngineType:
    RAY_GPU_STREAM = "ray-gpu-stream"
    SPARK_CPU_BATCH = "spark-cpu-batch"
    SPARK_CPU_STREAM = "spark-cpu-stream"
    LOCAL_CPU_BATCH = "local-cpu-batch"
    LOCAL_GPU_BATCH = "local-gpu-batch"


class StepState:
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    SUCCESS = "success"
    FAILED = "failed"


class TriggerEvent:
    JOB_FINISHED = "job_finished"
    FILE_SUCCESS = "file_success"


# PipelineStep 元类
class StepMeta(ABCMeta):
    """自动注册子类的元类"""

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if hasattr(cls, "engine_type"):
            Step.register_subclass(cls)


class Step(ABC, metaclass=StepMeta):
    _registry: Dict[EngineType, Type["Step"]] = {}
    valid_engine_types = [
        value for key, value in vars(EngineType).items() if not key.startswith("__")
    ]

    @classmethod
    def register_subclass(cls, subclass: Type["Step"]):
        """注册子类到工厂"""
        engine_type = getattr(subclass, "engine_type", None)
        if engine_type is None:
            raise TypeError(
                f"{subclass.__name__} must define 'engine_type' class attribute"
            )

        if engine_type not in cls.valid_engine_types:
            raise TypeError(
                f"{subclass.__name__}.engine_type must be one of {cls.valid_engine_types}"
            )

        cls._registry[engine_type] = subclass

    @classmethod
    def create(
        cls,
        pipeline_id: str,
        step_order: int,
        trigger_event: str,
        engine_type: EngineType,
        engine_config: Dict,
        operators: List,
        meta_config,
        **kwargs,
    ) -> "Step":
        """工厂方法创建具体step实例"""
        if engine_type not in cls._registry:
            raise ValueError(
                f"Unsupported engine type: {engine_type}. Available: {list(cls._registry.keys())}"
            )

        subclass = cls._registry[engine_type]
        return subclass(
            pipeline_id=pipeline_id,
            step_order=step_order,
            trigger_event=trigger_event,
            engine_type=engine_type,
            engine_config=engine_config,
            operators=operators,
            meta_config=meta_config,
            **kwargs,
        )

    def __init__(
        self,
        pipeline_id: str,
        step_order: int,
        trigger_event: str,
        engine_type: EngineType,
        engine_config: Dict,
        operators: List,
        meta_config,
        is_last_step: bool = False,
    ):
        # 核心属性初始化
        self.pipeline_id = pipeline_id
        self.step_order = step_order
        self.step_id = f"{pipeline_id}_step_{step_order}"
        self.trigger_event = trigger_event

        # 引擎相关配置
        if engine_type not in self.valid_engine_types:
            raise ValueError("Invalid engine type")
        self.engine_type = engine_type
        self.engine_config = engine_config

        # 算子配置
        self.operators = operators

        # 状态管理
        # self.state = StepState.PENDING
        # self.input_count = 0
        self.is_last_step = is_last_step
        self.meta_config = meta_config
        self.storage = self._get_storage(meta_config)
        self._check_interval = 3

        # 运行配置
        self.input_path = None
        self.output_path = self.create_output_path()
        self.input_queue = None
        self.output_queue = self.create_output_queue()
        self.input_count = None

        # 输入输出
        self.io = IO()

    def _get_storage(self, meta_config):
        """获取存储实例"""
        return get_storage(meta_config)

    @abstractmethod
    def meta_registry(self):
        pass

    def run(self):
        try:
            """执行完整阶段流程"""
            self.set_state(StepState.PENDING)
            while not self.check_requirements():
                time.sleep(self._check_interval)
                print(f"{self.step_id} is waiting for requirements to be met...")
                if self.get_upstream_step_state() == StepState.FAILED:
                    print(f"{self.step_id} upstream step failed, stopping...")
                    self.set_state(StepState.STOPPED)
                    return

            if self.engine_type in (
                EngineType.LOCAL_CPU_BATCH,
                EngineType.SPARK_CPU_BATCH,
            ):
                self.input_path = self.get_input_path()
                self.storage.update_step_field(
                    self.step_id, "input_path", self.input_path
                )
                print(f"{self.step_id} input path: {self.input_path}")

            if self.engine_type in (EngineType.SPARK_CPU_STREAM,):
                self.input_path = self.get_input_path()
                print(f"{self.step_id} input path: {self.input_path}")
                self.storage.update_step_field(
                    self.step_id, "input_path", self.input_path
                )

                self.input_queue = self.get_input_queue()
                print(f"{self.step_id} input queue: {self.input_queue}")
                self.storage.update_step_field(
                    self.step_id, "input_queue", self.input_queue
                )

            print(f"{self.step_id} run")
            self.set_state(StepState.RUNNING)

            if self.engine_type in (
                EngineType.LOCAL_CPU_BATCH,
                EngineType.SPARK_CPU_BATCH,
            ):
                self.process()
                print(f"{self.step_id} success")
                self.set_state(StepState.SUCCESS)

            elif self.engine_type in (EngineType.SPARK_CPU_STREAM,):
                self.process()
                print(f"{self.step_id} completed")

        except Exception as e:
            print(f"{self.step_id} failed: {str(e)}")
            self.set_state(StepState.FAILED)
            raise e

    @abstractmethod
    def process(self):
        """处理上游数据的总体方法，流批不同。读写的具体流程也在这里定义。"""

        pass

    @staticmethod
    def process_row(data: dict, ops: list):
        for op in ops:
            data = op.process(data)
        return data

    def check_requirements(self) -> bool:
        """检查运行条件"""
        if self.step_order == 1:
            return True
        elif self.trigger_event == TriggerEvent.JOB_FINISHED:
            upstream_step_state = self.storage.get_step_state(
                self.get_upstream_step_id()
            )
            if upstream_step_state == StepState.SUCCESS:
                return True
            else:
                return False
        elif self.trigger_event == TriggerEvent.FILE_SUCCESS:
            upstream_step_progress = self.get_upstream_step_progress()
            if upstream_step_progress > 0:
                return True
            else:
                return False

    def get_upstream_step_id(self) -> str:
        up_order = self.step_order - 1
        return f"{self.pipeline_id}_step_{up_order}" if up_order > 0 else None

    def get_input_path(self):
        """获取输入的路径"""
        if (
            self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH)
            and self.step_order > 1
        ):
            input_path = self.storage.get_step_field(
                self.get_upstream_step_id(), "output_path"
            )
            return input_path
        elif (
            self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH)
            and self.step_order == 1
        ):
            input_path = self.storage.get_pipeline_field(self.pipeline_id, "input_path")
            return input_path
        # stream step的话，上游也是stream step时，input queue即为上游的output queue，input count为上游step的input count
        # 上游是batch step时，需要初始化一个队列（input queue），input path是上游step的output path
        elif self.engine_type in (EngineType.SPARK_CPU_STREAM,) and self.step_order > 1:
            input_path = self.storage.get_step_field(
                self.get_upstream_step_id(), "output_path"
            )
            return input_path
        elif (
            self.engine_type in (EngineType.SPARK_CPU_STREAM,) and self.step_order == 1
        ):
            input_path = self.storage.get_pipeline_field(self.pipeline_id, "input_path")
            return input_path
        else:
            return None

    def get_input_queue(self):
        if self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH):
            return None
        elif (
            self.engine_type in (EngineType.SPARK_CPU_STREAM,) and self.step_order == 1
        ):
            input_queue = self.init_input_queue()
            return input_queue
        elif self.engine_type in (EngineType.SPARK_CPU_STREAM,) and self.step_order > 1:
            input_queue = self.storage.get_step_field(
                self.get_upstream_step_id(), "output_queue"
            )
            upstream_input_count = self.storage.get_step_field(
                self.get_upstream_step_id(), "input_count"
            )
            self.storage.update_step_field(
                self.step_id, "input_count", upstream_input_count
            )
            return input_queue

    def create_output_path(self):
        """创建输出路径"""
        if self.engine_type in (
            EngineType.LOCAL_CPU_BATCH,
            EngineType.SPARK_CPU_BATCH,
            EngineType.SPARK_CPU_STREAM,
        ):
            pipeline_output_path = self.storage.get_pipeline_field(
                self.pipeline_id, "output_path"
            )
            if self.is_last_step:
                output_path = os.path.join(pipeline_output_path, "final_output")
            else:
                output_path = os.path.join(pipeline_output_path, self.step_id)
            return output_path
        else:
            return None

    def create_output_queue(self):
        """创建输出队列"""
        import time

        timestamp = int(time.time())
        output_queue = (
            f"kafka://{self.pipeline_id}_step_{self.step_order}_output_{timestamp}"
        )
        return output_queue

    def init_input_queue(self):
        """创建输入队列"""
        import time

        timestamp = int(time.time())
        input_queue = f"kafka://{self.pipeline_id}_step_0_output_{timestamp}"
        kafka_writer = KafkaWriter(input_queue)
        pipeline_input_path = self.storage.get_pipeline_field(
            self.pipeline_id, "input_path"
        )

        iter1, iter2 = tee(
            (
                fp
                for fp in list_s3_objects(pipeline_input_path)
                if fp.endswith((".jsonl", ".jsonl.gz"))
            ),
            2,
        )

        input_count = sum(1 for _ in iter1)
        self.storage.update_step_field(self.step_id, "input_count", input_count)

        for fp in iter2:
            kafka_writer.write({"id": fp, "file_path": fp})
            kafka_writer.flush()

        return input_queue

    # 状态管理方法
    def set_state(self, state: StepState):
        self.storage.set_step_state(self.step_id, state)
        print(f"Step {self.step_id} state changed to {state}")

    def get_upstream_step_progress(self) -> Optional[int]:
        """获取处理进度"""
        up_step_id = self.get_upstream_step_id()
        if up_step_id:
            up_step_progress = self.storage.get_step_progress(up_step_id)
            return up_step_progress
        else:
            return None

    def get_upstream_step_state(self) -> Optional[str]:
        """获取处理进度"""
        up_step_id = self.get_upstream_step_id()
        if up_step_id:
            up_step_progress = self.storage.get_step_state(up_step_id)
            return up_step_progress
        else:
            return None

    def stop(self):
        self.set_state(StepState.STOPPED)

    def resume(self):
        self.set_state(StepState.RUNNING)
