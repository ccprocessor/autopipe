import os.path
import time
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict, List, Optional, Type, Iterable
from enum import Enum
from autopipe.config.base import ConfigLoader
from autopipe.infrastructure.storage import get_storage
from autopipe.infrastructure.io.base import IO
from xinghe.dp.spark import SparkExecutor
from xinghe.spark import read_any_path, write_any_path
import json


class EngineType(Enum):
    RAY_GPU_STREAM = "ray-gpu-stream"
    SPARK_CPU_BATCH = "spark-cpu-batch"
    SPARK_CPU_STREAM = "spark-cpu-stream"
    LOCAL_CPU_BATCH = "local-cpu-batch"
    LOCAL_GPU_BATCH = "local-gpu-batch"


class StepState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    SUCCESS = "success"
    FAILED = "failed"


class TriggerEvent(Enum):
    JOB_FINISHED = "job_finished"
    FILE_SUCCESS = "file_success"


# PipelineStep 元类
class StepMeta(ABCMeta):
    """自动注册子类的元类"""

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if hasattr(cls, 'engine_type'):
            PipelineStep.register_subclass(cls)


class PipelineStep(ABC, metaclass=StepMeta):
    _registry: Dict[EngineType, Type['PipelineStep']] = {}

    @classmethod
    def register_subclass(cls, subclass: Type['PipelineStep']):
        """注册子类到工厂"""
        engine_type = getattr(subclass, 'engine_type', None)
        if engine_type is None:
            raise TypeError(f"{subclass.__name__} must define 'engine_type' class attribute")
        if not isinstance(engine_type, EngineType):
            raise TypeError(f"{subclass.__name__}.engine_type must be EngineType enum")
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
            **kwargs
    ) -> 'PipelineStep':
        """工厂方法创建具体step实例"""
        if engine_type not in cls._registry:
            raise ValueError(f"Unsupported engine type: {engine_type}. Available: {list(cls._registry.keys())}")

        subclass = cls._registry[engine_type]
        return subclass(
            pipeline_id=pipeline_id,
            step_order=step_order,
            trigger_event=trigger_event,
            engine_type=engine_type,
            engine_config=engine_config,
            operators=operators,
            meta_config=meta_config,
            **kwargs
        )

    def __init__(self,
                 pipeline_id: str,
                 step_order: int,
                 trigger_event: str,
                 engine_type: EngineType,
                 engine_config: Dict,
                 operators: List,
                 meta_config,
                 is_last_step: bool = False):
        # 核心属性初始化
        self.pipeline_id = pipeline_id
        self.step_order = step_order
        self.step_id = f"{pipeline_id}_step_{step_order}"
        self.trigger_event = trigger_event

        # 引擎相关配置
        if not isinstance(engine_type, EngineType):
            raise ValueError("Invalid engine type")
        self.engine_type = engine_type
        self.engine_config = engine_config

        # 算子配置
        self.operators = operators

        # 状态管理
        # self.state = StepState.PENDING
        # self.input_count = 0
        self.is_last_step = is_last_step
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
                if self.get_upstream_step_state() == StepState.FAILED.value:
                    print(f"{self.step_id} upstream step failed, stopping...")
                    self.set_state(StepState.STOPPED)
                    return
            if self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH):
                self.input_path = self.get_input()
                print(f"{self.step_id} input path: {self.input_path}")
            print(f"{self.step_id} run")
            self.set_state(StepState.RUNNING)
            self.process()
            print(f"{self.step_id} success")
            self.set_state(StepState.SUCCESS)
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
        elif self.trigger_event == TriggerEvent.JOB_FINISHED.value:
            upstream_step_state = self.storage.get_step_state(self.get_upstream_step_id())
            if upstream_step_state == StepState.SUCCESS.value:
                return True
            else:
                return False
        elif self.trigger_event == TriggerEvent.FILE_SUCCESS.value:
            upstream_step_progress = self.get_upstream_step_progress()
            if upstream_step_progress > 0:
                return True
            else:
                return False

    def get_upstream_step_id(self) -> str:
        up_order = self.step_order - 1
        return f"{self.pipeline_id}_step_{up_order}" if up_order > 0 else None

    def get_input(self):
        """获取输入"""
        if self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH) and self.step_order > 1:
            input_path = self.storage.get_step_field(self.get_upstream_step_id(), "output_path")
            return input_path
        elif self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH) and self.step_order == 1:
            input_path = self.storage.get_pipeline_field(self.pipeline_id, "input_path")
            return input_path
        else:
            return None

    def create_output_path(self):
        """创建输出路径"""
        if self.engine_type in (EngineType.LOCAL_CPU_BATCH, EngineType.SPARK_CPU_BATCH):
            pipeline_output_path = self.storage.get_pipeline_field(self.pipeline_id, "output_path")
            if self.is_last_step:
                output_path = os.path.join(pipeline_output_path, "final_output")
            else:
                output_path = os.path.join(pipeline_output_path, self.step_id)
            return output_path
        else:
            return None

    def create_output_queue(self):
        """创建输出队列"""
        output_queue = f"kafka://{self.pipeline_id}_step_{self.step_order}_output"
        return output_queue

    # 状态管理方法
    def set_state(self, state: StepState):
        self.storage.set_step_state(self.step_id, state.value)
        print(f"Step {self.step_id} state changed to {state.value}")

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


class LocalCpuBatchStep(PipelineStep):
    engine_type = EngineType.LOCAL_CPU_BATCH

    def meta_registry(self):
        meta_dict = {
            'id': self.step_id,
            'engine_type': self.engine_type.value,
            'trigger_event': self.trigger_event,
            'input_path': self.input_path,
            'input_queue': self.input_queue,
            'input_count': self.input_count,
            'output_path': self.output_path,
            'output_queue': self.output_queue,
            'state': None,
            'operators': self.operators
        }
        self.storage.register_step(self.step_id, meta_dict)
        self.storage.register_step_progress(self.step_id)

    def process(self):
        ops = [get_operator(op['name'], op['params']) for op in self.operators]

        for op in ops:
            op.resource_load()

        if not self.io.exists(self.input_path):
            raise FileNotFoundError(f"输入目录不存在: {self.input_path}")

        if not self.io.exists(self.output_path):
            print(f"创建输出目录: {self.output_path}")
            os.mkdir(self.output_path)

        # 遍历文件
        for filename in self.io.list_dir(self.input_path):
            print(filename)
            if not filename.endswith(('.jsonl', '.jsonl.gz')):
                continue

            # 构建完整路径（可根据不同IO实现调整路径逻辑）
            input_path = os.path.join(self.input_path, filename)
            output_path = os.path.join(self.output_path, filename)

            # 处理单个文件
            with self.io.read_stream(input_path) as fin, self.io.write_stream(output_path) as fout:
                # print(f"execute {input_path}")
                for line in fin:
                    line = line.strip()
                    # print(line)
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        # 执行操作链

                        data = LocalCpuBatchStep.process_row(data, ops)
                        fout.write(json.dumps(data) + '\n')
                    except Exception as e:
                        print(f"处理失败: {input_path} | 错误: {e}")


class SparkCPUBatchStep(PipelineStep):
    engine_type = EngineType.SPARK_CPU_BATCH

    def meta_registry(self):
        meta_dict = {
            'id': self.step_id,
            'engine_type': self.engine_type.value,
            'trigger_event': self.trigger_event,
            'input_path': self.input_path,
            'input_queue': self.input_queue,
            'input_count': self.input_count,
            'output_path': self.output_path,
            'output_queue': self.output_queue,
            'state': None,
            'operators': self.operators
        }
        self.storage.register_step(self.step_id, meta_dict)
        self.storage.register_step_progress(self.step_id)

    def process(self):
        ops = [get_operator(op['name'], op['params']) for op in self.operators]

        for op in ops:
            op.resource_load()

        # if not self.io.exists(self.input_path):
        #     raise FileNotFoundError(f"输入目录不存在: {self.input_path}")

        # 初始化 SparkExecutor
        executor = SparkExecutor(appName=self.step_id, config=self.engine_config)

        # 定义处理函数
        def _process(_iter):
            for d in _iter:
                d = SparkCPUBatchStep.process_row(d, ops)
                yield d
                # d["op5"] = "test"
                # yield d

        # 定义处理函数
        pipeline = [
            {
                "fn": read_any_path,
                "kwargs": {
                    "path": self.input_path,
                }
            },
            {
                "fn": _process,
            },
            {
                "fn": write_any_path,
                "kwargs": {
                    "path": self.output_path,
                }
            },
        ]

        # 执行任务
        executor.run(pipeline)


# code for test

from multiprocessing import Process


def run_step_in_process(
        pipeline_id: str,
        step_order: int,
        trigger_event: str,
        engine_type: EngineType,
        engine_config: Dict,
        operators: List,
        meta_config,
        is_last_step: bool,
):
    """子进程任务：创建并运行 PipelineStep 实例"""
    try:
        # 每个进程独立创建实例
        step = PipelineStep.create(
            pipeline_id=pipeline_id,
            step_order=step_order,
            trigger_event=trigger_event,
            engine_type=engine_type,
            engine_config=engine_config,
            operators=operators,
            meta_config=meta_config,
            is_last_step=is_last_step,
        )
        step.meta_registry()  # 元数据注册
        step.run()  # 运行任务
    except Exception as e:
        print(f"outcheck: Step {pipeline_id}_step_{step_order} failed: {str(e)}")
        raise e


from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator, get_operator


@register_operator
class Operation1(BaseOperation):
    operator_name = "op1"
    operator_type = "default"

    def resource_load(self):
        print("load resource for op1")

    def process(self, data: dict) -> dict:
        data["op1"] = "processed"
        return data


@register_operator
class Operation2(BaseOperation):
    operator_name = "op2"
    operator_type = "default"

    def resource_load(self):
        print("load resource for op2")

    def process(self, data: dict) -> dict:
        data["op2"] = 100
        return data


@register_operator
class Operation3(BaseOperation):
    operator_name = "op3"
    operator_type = "default"

    def resource_load(self):
        print("load resource for op3")

    def process(self, data: dict) -> dict:
        data["op3"] = "processed"
        return data


@register_operator
class Operation4(BaseOperation):
    operator_name = "op4"
    operator_type = "default"

    def resource_load(self):
        print("load resource for op4")

    def process(self, data: dict) -> dict:
        data["op4"] = 100
        return data


if __name__ == "__main__":

    config = ConfigLoader("C:/Users/chenhaoling/PycharmProjects/autopipe/examples/test_config.json")
    redis_client = get_storage(config.meta_storage)

    pipeline_meta = {
        "pipeline_id": "test_pipeline_202503111212_abcd",
        "pipeline_name": "test_pipeline",
        "pipeline_description": "test_pipeline_202503111212_abcd",
        "pipeline_config": "test_pipeline_202503111212_abcd",
        "pipeline_status": "running",
        "input_path": "D:/downloads/samples/",
        "output_path": "D:/downloads/output/",
        "steps": ["test_pipeline_202503111212_abcd_step_1", "test_pipeline_202503111212_abcd_step_2"]
    }

    redis_client.register_pipeline(pipeline_id="test_pipeline_202503111212_abcd", pipeline_meta=pipeline_meta)

    steps_config = [
        {
            "pipeline_id": "test_pipeline_202503111212_abcd",
            "step_order": 1,
            "trigger_event": TriggerEvent.JOB_FINISHED.value,
            "engine_type": EngineType.LOCAL_CPU_BATCH,
            "engine_config": {"memory": "4g"},
            "operators": [{"name": "op1", "params": {}}, {"name": "op2", "params": {}}],
            "meta_config": config.meta_storage,
            "is_last_step": False
        },
        {
            "pipeline_id": "test_pipeline_202503111212_abcd",
            "step_order": 2,
            "trigger_event": TriggerEvent.JOB_FINISHED.value,
            "engine_type": EngineType.LOCAL_CPU_BATCH,
            "engine_config": {"gpu_id": 0},
            "operators": [{"name": "op3", "params": {}}, {"name": "op4", "params": {}}],
            "meta_config": config.meta_storage,
            "is_last_step": True
        }
    ]

    processes = []
    for config in steps_config:
        proc = Process(target=run_step_in_process, kwargs=config)
        processes.append(proc)
        proc.start()

    # 等待所有子进程完成
    for proc in processes:
        proc.join()
