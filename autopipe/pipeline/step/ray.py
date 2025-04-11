from xinghe.dp.ray import RayTaskExecutor
from xinghe.dp.task import KafkaTasks
from xinghe.spark import read_any_path, write_any_path
from xinghe.s3 import (
    S3DocWriter,
    head_s3_object_with_retry,
    is_s3_path,
    read_s3_rows,
)
from xinghe.dp.base import SkipTask
from xinghe.utils.json_util import json_loads
from xinghe.ops.file import FileReader, FileWriter

from autopipe.infrastructure.storage import get_storage
from autopipe.pipeline.step.base import EngineType, StepState
from autopipe.pipeline.operator.get_op import get_operator
from autopipe.pipeline.step.base import Step, InputType
from typing import Iterator, Dict, Any, Iterable
from loguru import logger
import logging
import ray

import time
import threading

SIZE_2G = 2 << 30


def read_func(
    dummy_iter: Iterator, use_stream: int, input_file: str
) -> Iterator[Dict[str, Any]]:
    if not input_file:
        raise SkipTask("missing [input_file]")
    if not is_s3_path(input_file):
        raise SkipTask(f"invalid input_file [{input_file}]")

    input_head = head_s3_object_with_retry(input_file)
    if not input_head:
        raise SkipTask(f"file [{input_file}] not found")

    use_stream = use_stream

    input_file_rows = read_s3_rows(input_file, use_stream)

    for d in input_file_rows:
        d = json_loads(d.value)
        yield d


def write_func(
    data_iter: Iterator,
    # use_stream: int,
    step_id,
    output_path,
    output_queue,
    meta_config,
    input_file: str,
) -> Iterator[Dict[str, Any]]:
    if not input_file:
        raise SkipTask("missing [input_file]")
    if not is_s3_path(input_file):
        raise SkipTask(f"invalid input_file [{input_file}]")

    input_head = head_s3_object_with_retry(input_file)
    if not input_head:
        raise SkipTask(f"file [{input_file}] not found")

    # use_stream = use_stream

    output_file = output_path + "/" + input_file.split("/")[-1]
    print(f"output_file: {output_file}")
    writer = S3DocWriter(output_file) if output_file else None

    file_meta_client = get_storage(meta_config)

    for d in data_iter:
        writer.write(d)

    writer.flush()
    file_meta_client.update_step_progress(step_id, output_file)
    input_count = file_meta_client.get_step_field(step_id, "input_count")
    step_progress = file_meta_client.get_step_progress(step_id)
    print(f"input_count: {input_count}, step_progress: {step_progress}")

    if input_count == step_progress:
        file_meta_client.set_step_state(step_id, StepState.SUCCESS)

    return []


class RayGPUStreamStep(Step):
    engine_type = EngineType.RAY_GPU_STREAM

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor = None

    def meta_registry(self):
        meta_dict = {
            "id": self.step_id,
            "engine_type": self.engine_type,
            "trigger_event": self.trigger_event,
            "input_path": self.input_path,
            "input_queue": self.input_queue,
            "input_count": self.input_count,
            "output_path": self.output_path,
            "output_queue": self.output_queue,
            "state": None,
            "operators": self.operators,
        }
        self.storage.register_step(self.step_id, meta_dict)
        # self.storage.register_step_progress(self.step_id)

    def construct_sequence(self, ops):
        read_stream = self.engine_config.get("read_stream", 2 << 30)

        sequence = [
            {
                "fn": read_func,
                "kwargs": {
                    "use_stream": read_stream,
                },
                "ray_remote_args": {
                    "memory": read_stream,
                    "num_cpus": 1,
                },
            }
        ]

        for op in ops:
            logger.info("start ray op construction")
            import torch
            from autopipe.pipeline.operator.gpu_op import ModelOperation

            seq_dict = {
                "fn": get_operator(op["name"], op["params"]),
                "kwargs": op["params"].get("kwargs", {}),
                "ray_remote_args": op["params"].get("ray_remote_args", {}),
            }

            if seq_dict["fn"].operator_type == "gpu_model":
                from xinghe.ml.actor import ModelActor

                seq_dict["fn"] = ModelActor
                logger.info(f"start model operation {seq_dict['kwargs']['model_cls']}")

                model_cls_str = seq_dict["kwargs"]["model_cls"]

                from xinghe.ml.model import (
                    BertTwoClassModel,
                    TemplateLmDeployTextGenModel,
                    XlmrMultiLabelProbModel,
                )

                logger.info(f"model_cls_str: {model_cls_str}")

                model_cls = globals()[model_cls_str]
                seq_dict["kwargs"]["model_cls"] = model_cls

                logger.info(f"test model operation {seq_dict['kwargs']['model_cls']}")

                seq_dict["kwargs"]["model_cls_kwargs"]["device"] = torch.device("cuda")

            sequence.append(seq_dict)

        sequence.append(
            {
                "fn": write_func,
                "kwargs": {
                    "step_id": self.step_id,
                    "output_path": self.output_path,
                    "output_queue": self.output_queue,
                    "meta_config": self.meta_config,
                },
                "ray_remote_args": {
                    "memory": (2 << 30),
                    "num_cpus": 1,
                },
            },
        )
        return sequence

    def process(self):
        self.stop_event = threading.Event()

        # file_compression = self.engine_config.get("output_compression", None)

        ray_address = self.engine_config.get("address", None)
        if not ray_address:
            raise Exception("ray address is required")

        ray_runtime_env = self.engine_config.get("runtime_env", {})
        log_to_driver = self.engine_config.get("log_to_driver", False)

        ray.init(
            address=ray_address,
            runtime_env=ray_runtime_env,
            log_to_driver=log_to_driver,
            logging_level=logging.INFO,
        )

        def _safe_shutdown(executor, step_id, storage, stop_event):
            """安全关闭Spark Streaming的轮询检查"""
            while True:
                time.sleep(6)  # 每6s检查一次
                step_state = storage.get_step_state(step_id)
                # print(f"daemon check: {step_id} state: {step_state}")

                if step_state == StepState.SUCCESS:
                    logger.info(f"daemon check: {step_id} success")
                    executor._clean_actor_pools()  # 停止SparkContext
                    logger.info(f"daemon stop {step_id} clean ray actor pools")
                    stop_event.set()
                    break

        sequence = self.construct_sequence(self.operators)

        logger.info(self.input_queue)
        logger.info(self.output_queue)
        logger.info("output_path: " + self.output_path)

        # 创建executor
        parallelism = self.engine_config.get("parallelism", 20)
        self.executor = RayTaskExecutor(parallelism=parallelism)

        # 启动独立线程监控进度
        shutdown_thread = threading.Thread(
            target=_safe_shutdown,
            args=(self.executor, self.step_id, self.storage, self.stop_event),
        )
        shutdown_thread.daemon = True
        shutdown_thread.start()

        # 启动SparkExecutor
        tasks = KafkaTasks(self.input_queue, self.step_id)
        # self.executor.run(tasks, sequence)

        # 启动执行线程（将 executor.run 放入独立线程）
        executor_thread = threading.Thread(
            target=self.executor.run, args=(tasks, sequence)
        )
        executor_thread.start()

        # 主线程阻塞，等待事件触发
        self.stop_event.wait()  # 直到 stop_event.set() 被调用

        # 清理资源
        executor_thread.join(timeout=10)  # 等待执行线程结束
        if executor_thread.is_alive():
            logger.warning("Executor thread did not exit gracefully, forcing cleanup")

        logger.info("Process terminated after step succeeded.")

    def stop(self):
        """停止 Spark 任务并更新状态"""
        # 调用父类方法更新状态
        super().stop()

        # 停止 Spark 任务
        if self.executor:
            try:
                self.executor._clean_actor_pools()
                logger.info(f"{self.step_id} Ray 任务已终止")
            except Exception as e:
                logger.error(f"{self.step_id} 停止 Ray 任务失败: {e}")
