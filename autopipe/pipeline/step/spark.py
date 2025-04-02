from xinghe.dp.spark import SparkExecutor
from xinghe.spark import read_any_path, write_any_path
from xinghe.s3 import (
    S3DocWriter,
    head_s3_object_with_retry,
    is_s3_path,
    read_s3_rows,
)
from xinghe.utils.json_util import json_loads
from autopipe.infrastructure.storage import get_storage
from autopipe.pipeline.step.base import EngineType, StepState
from autopipe.pipeline.operator.get_op import get_operator
from autopipe.pipeline.step.base import Step, InputType
from loguru import logger

import time
import threading

SIZE_2G = 2 << 30


class SparkCPUBatchStep(Step):
    engine_type = EngineType.SPARK_CPU_BATCH

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

    def process(self):
        ops = [get_operator(op["name"], op["params"]) for op in self.operators]

        for op in ops:
            op.resource_load()

        if not self.io.exists(self.input_path):
            raise FileNotFoundError(f"输入目录不存在: {self.input_path}")

        # 初始化 SparkExecutor
        self.executor = SparkExecutor(appName=self.step_id, config=self.engine_config)

        # 定义处理函数
        def _process(_iter):
            for d in _iter:
                d = SparkCPUBatchStep.process_row(d, ops)
                yield d

        # 定义处理函数
        pipeline = [
            {
                "fn": read_any_path,
                "kwargs": {
                    "path": self.input_path,
                },
            },
            {
                "fn": _process,
            },
            {
                "fn": write_any_path,
                "kwargs": {
                    "path": self.output_path,
                },
            },
        ]

        # 执行任务
        self.executor.run(pipeline)

    def stop(self):
        """停止 Spark 任务并更新状态"""
        # 调用父类方法更新状态
        super().stop()

        # 停止 Spark 任务
        if self.executor:
            try:
                self.executor.stop()
                logger.info(f"{self.step_id} Spark 任务已终止")
            except Exception as e:
                logger.error(f"{self.step_id} 停止 Spark 任务失败: {e}")


class SparkCPUStreamStep(Step):
    engine_type = EngineType.SPARK_CPU_STREAM

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

    def process(self):
        file_compression = self.engine_config.get("output_compression", None)

        def _safe_shutdown(executor, step_id, storage):
            """安全关闭Spark Streaming的轮询检查"""
            while True:
                time.sleep(6)  # 每6s检查一次
                step_state = storage.get_step_state(step_id)
                # print(f"daemon check: {step_id} state: {step_state}")

                if step_state == StepState.SUCCESS:
                    logger.info(f"daemon check: {step_id} success")
                    executor.spark.stop()  # 停止SparkContext
                    logger.info(f"daemon stop {step_id} spark context")
                    break

                # wait for stop
                # if step_state == StepState.STOPPED:
                #     logger.info(f"daemon check: {step_id} has been stopped")
                #     executor.spark.stop()  # 停止SparkContext
                #     logger.info(f"daemon stop {step_id} spark context")
                #     break

        def _process(_iter, step_id, meta_config, output_path, operators):
            from loguru import logger

            use_stream = SIZE_2G
            ops = [get_operator(op["name"], op["params"]) for op in operators]

            for op in ops:
                op.resource_load()

            for d in _iter:
                file_meta_client = get_storage(meta_config)

                input_file_path = d["file_path"]
                input_track_id = d["track_id"]

                if not is_s3_path(input_file_path):
                    logger.info(f"{input_file_path} is not s3 path")
                    continue

                input_head = head_s3_object_with_retry(input_file_path)
                if not input_head:
                    logger.info(f"{input_file_path} is not exist")
                    continue

                if self.input_type == InputType.DATA:
                    file_name = input_file_path.split("/")[-1]
                    output_file_path = f"{output_path}/{file_name}"

                elif self.input_type == InputType.INDEX:
                    output_file_path = (
                        f"{output_path}/{input_track_id}.jsonl" + f".{file_compression}"
                        if file_compression
                        else f"{output_path}/{input_track_id}.jsonl"
                    )

                output_head = head_s3_object_with_retry(output_file_path)

                if output_head:
                    logger.info(f"{output_file_path} is exist")
                    continue

                writer = S3DocWriter(output_file_path)

                for row in read_s3_rows(input_file_path, use_stream):
                    try:
                        row_dict = json_loads(row.value)

                        new_row = SparkCPUStreamStep.process_row(row_dict, ops)
                        # new_row = add_test(row)
                        writer.write(new_row)
                    except Exception as e:
                        logger.error(
                            f"""处理失败: {input_file_path} | {row_dict.get("track_id")} | 错误: {e}"""
                        )

                writer.flush()
                file_meta_client.update_step_progress(step_id, output_file_path)
                input_count = file_meta_client.get_step_field(step_id, "input_count")
                step_progress = file_meta_client.get_step_progress(step_id)

                if input_count == step_progress:
                    file_meta_client.set_step_state(step_id, StepState.SUCCESS)

                d["file_path"] = output_file_path
                yield d

        if not self.is_last_step:
            pipeline = [
                {
                    "fn": read_any_path,
                    "kwargs": {
                        "path": self.input_queue,
                    },
                },
                {
                    "fn": _process,
                    "kwargs": {
                        "step_id": self.step_id,
                        "meta_config": self.meta_config,
                        "output_path": self.output_path,
                        "input_count": self.input_count,
                        "operators": self.operators,
                    },
                },
                {
                    "fn": write_any_path,
                    "kwargs": {
                        "path": self.output_queue,
                    },
                },
            ]
        else:
            pipeline = [
                {
                    "fn": read_any_path,
                    "kwargs": {
                        "path": self.input_queue,
                    },
                },
                {
                    "fn": _process,
                    "kwargs": {
                        "step_id": self.step_id,
                        "meta_config": self.meta_config,
                        "output_path": self.output_path,
                        "operators": self.operators,
                    },
                },
            ]

        logger.info(self.input_queue)
        logger.info(self.output_queue)
        logger.info("output_path: " + self.output_path)

        # 创建executor
        self.executor = SparkExecutor(appName=self.step_id, config=self.engine_config)

        # 启动独立线程监控进度
        shutdown_thread = threading.Thread(
            target=_safe_shutdown, args=(self.executor, self.step_id, self.storage)
        )
        shutdown_thread.daemon = True
        shutdown_thread.start()

        # 启动SparkExecutor
        self.executor.run(pipeline)

    def stop(self):
        """停止 Spark 任务并更新状态"""
        # 调用父类方法更新状态
        super().stop()

        # 停止 Spark 任务
        if self.executor:
            try:
                self.executor.stop()
                logger.info(f"{self.step_id} Spark 任务已终止")
            except Exception as e:
                logger.error(f"{self.step_id} 停止 Spark 任务失败: {e}")
