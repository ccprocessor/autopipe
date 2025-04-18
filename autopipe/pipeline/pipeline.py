from typing import Dict, List, Union
from datetime import datetime
import string
import random
from autopipe.pipeline.step.base import Step
from autopipe.config.base import ConfigLoader
import time
from multiprocessing import Process
from autopipe.pipeline.step.base import StepState
from autopipe.infrastructure.storage import get_storage
from loguru import logger


class PipelineState:
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    SUCCESS = "success"
    FAILED = "failed"


def human_readable_id():
    date_part = datetime.now().strftime("%y%m%d")
    char_pool = string.ascii_uppercase + string.digits
    random_part = "".join(random.choices(char_pool, k=6))
    return f"{date_part}_{random_part}"


class Pipeline:
    """协调数据处理流程的核心类"""

    def __init__(self, config: Union[Dict, str], pipeline_id: str = None):
        if isinstance(config, str):
            config = ConfigLoader(config)
            self.config = config
        else:
            self.config = config

        # storage
        self.meta_config = config.meta_storage
        self.storage = get_storage(self.meta_config)

        if pipeline_id:
            # 恢复已有流程
            existing_meta = self.storage.get_pipeline_meta(pipeline_id)
            if not existing_meta:
                raise ValueError(f"Pipeline {pipeline_id} does not exist")
                self._init_from_existing_meta(existing_meta)
        else:
            # 创建新流程
            self.pipeline_meta = config.get("pipeline_meta", {})
            self.pipeline_name = self.pipeline_meta.get("pipeline_name")
            self.pipeline_mode = self.pipeline_meta.get("pipeline_mode")
            self.pipeline_id = f"{self.pipeline_name}_{human_readable_id()}"
            self._init_new_pipeline()

        self.processes: List[Process] = []
        self._check_interval = 3  # 状态检查间隔（秒）

    def _init_new_pipeline(self):
        """初始化新流程"""
        # dataset
        self.dataset_config = self.config.get("dataset", {})
        self.input_path = self.dataset_config.get("input_path")
        self.input_type = self.dataset_config.get("input_type")
        self.output_path = self.dataset_config.get("output_path")

        # steps
        self.steps_config = self.config.get("steps", [])
        self.steps: List[Step] = []

        # register metadata
        self.register_metadata()

        # construct steps
        self.construct_steps()

        logger.info(f"new pipeline constructed: {self.pipeline_id}")

    def _init_from_existing_meta(self, existing_meta: Dict):
        """从元数据恢复流程"""
        self.pipeline_id = existing_meta["pipeline_id"]
        self.pipeline_name = existing_meta["pipeline_name"]
        self.input_path = existing_meta["input_path"]
        self.input_type = existing_meta["input_type"]
        self.output_path = existing_meta["output_path"]
        # 恢复所有步骤
        step_ids = existing_meta.get("steps", [])
        self.steps = []
        for step_id in step_ids:
            step_meta = self.storage.get_step_meta(step_id)
            step = Step.create_from_meta(step_meta, self.meta_config)
            self.steps.append(step)

        logger.info(f"pipeline {self.pipeline_id} restored from meta")

    def construct_steps(self):
        """构造pipeline的所有steps"""
        steps_num = len(self.steps_config)
        for step_order, step_config in enumerate(self.steps_config, start=1):
            step = Step.create(
                pipeline_id=self.pipeline_id,
                step_order=step_order,
                trigger_event=step_config.get("trigger_event"),
                engine_type=step_config.get("engine_type"),
                engine_config=step_config.get("engine_config"),
                operators=step_config.get("operators"),
                meta_config=self.meta_config,
                is_last_step=step_order == steps_num,
            )
            step.meta_registry()
            self.steps.append(step)

        step_id_list = [step.step_id for step in self.steps]
        self.storage.update_pipeline_field(self.pipeline_id, "steps", step_id_list)

    def register_metadata(self):
        """注册pipeline的元数据"""

        pipeline_meta_dict = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_state": None,
            "input_path": self.input_path,
            "input_type": self.input_type,
            "output_path": self.output_path,
        }

        self.storage.register_pipeline(self.pipeline_id, pipeline_meta_dict)

    def run(self):
        """运行所有Step"""
        self.storage.update_pipeline_field(
            self.pipeline_id, "pipeline_state", PipelineState.RUNNING
        )
        self.processes = []

        # 启动所有Step进程
        for step in self.steps:
            proc = Process(target=self._run_step, args=(step,))
            self.processes.append(proc)
            proc.start()

        # 启动状态监控进程
        monitor_proc = Process(target=self._monitor_steps)
        self.processes.append(monitor_proc)
        monitor_proc.start()

        # 等待所有进程完成
        for proc in self.processes:
            proc.join()
        logger.info(f"Pipeline {self.pipeline_id} running completed")

    def _run_step(self, step: Step):
        """运行单个Step"""
        try:
            step.run()
            logger.info(f"Step {step.step_id} completed")
        except Exception as e:
            logger.info(f"Step {step.step_id} failed: {str(e)}")
            self.storage.update_pipeline_field(
                self.pipeline_id, "pipeline_state", PipelineState.FAILED
            )
            raise e

    def _monitor_steps(self):
        """监控所有Step的状态"""
        while (
            self.storage.get_pipeline_field(self.pipeline_id, "pipeline_state")
            == PipelineState.RUNNING
        ):
            all_success = True
            any_failed = False

            for step in self.steps:
                state = self.storage.get_step_state(step.step_id)
                if state == StepState.FAILED:
                    any_failed = True
                    all_success = False
                    break
                elif state != StepState.SUCCESS:
                    all_success = False

            if any_failed:
                logger.info("Pipeline failed: one or more steps failed")
                self.storage.update_pipeline_field(
                    self.pipeline_id, "pipeline_state", "failed"
                )
                break
            elif all_success:
                logger.info("Pipeline completed successfully")
                self.storage.update_pipeline_field(
                    self.pipeline_id, "pipeline_state", "success"
                )
                break

            time.sleep(self._check_interval)
        logger.info("Pipeline completed")

    def stop(self):
        """停止所有进程（包括步骤和监控进程）"""
        logger.info(f"Stopping pipeline {self.pipeline_id}...")

        # 停止所有步骤
        for step in self.steps:
            try:
                step.stop()
            except Exception as e:
                logger.error(f"Failed to stop step {step.step_id}: {e}")

        # 终止所有进程（包括监控进程）
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()  # 发送 SIGTERM
                logger.debug(f"Terminated process {proc.pid}")

        # 强制清理残留进程
        for proc in self.processes:
            if proc.is_alive():
                proc.kill()  # 发送 SIGKILL
                logger.warning(f"Killed process {proc.pid}")

        # 更新状态
        self.storage.update_pipeline_field(
            self.pipeline_id, "pipeline_state", PipelineState.STOPPED
        )

    def resume(self):
        """恢复所有Step"""
        for step in self.steps:
            step.resume()

    def get_status(self) -> Dict:
        """获取Pipeline状态"""
        status = {
            "pipeline_id": self.pipeline_id,
            "steps": [],
        }

        for step in self.steps:
            step_state = self.storage.get_step_state(step.step_id)
            step_progress = self.storage.get_step_progress(step.step_id)
            status["steps"].append(
                {
                    "step_id": step.step_id,
                    "state": step_state,
                    "progress": step_progress,
                }
            )

        return status
