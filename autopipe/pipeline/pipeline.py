from typing import Dict, List
from datetime import datetime
import string
import random
from autopipe.pipeline.pipeline_step.base import PipelineStep
import time
from multiprocessing import Process
from autopipe.pipeline.pipeline_step.base import StepState
from autopipe.infrastructure.storage import get_storage


def human_readable_id():
    date_part = datetime.now().strftime("%y%m%d")
    char_pool = string.ascii_uppercase + string.digits
    random_part = "".join(random.choices(char_pool, k=6))
    return f"{date_part}_{random_part}"


class Pipeline:
    """协调数据处理流程的核心类"""

    def __init__(self, config: Dict, pipeline_id: str = None):
        self.config = config
        self.pipeline_meta = self.config.get("pipeline_meta")
        self.pipeline_name = self.pipeline_meta.get("pipeline_name")
        self.pipeline_mode = self.pipeline_meta.get("pipeline_mode")

        if not pipeline_id:
            self.pipeline_id = f"{self.pipeline_name}_{human_readable_id()}"
        else:
            self.pipeline_id = pipeline_id
        print(f"pipeline_id: {self.pipeline_id}")

        # storage
        self.meta_config = config.get("meta_storage")
        self.storage = get_storage(self.meta_config)

        # dataset
        self.dataset_config = config.get("dataset")
        self.input_path = self.dataset_config.get("input_path")
        self.input_type = self.dataset_config.get("input_type")
        self.output_path = self.dataset_config.get("output_path")

        # steps
        self.steps_config = config.get("steps")
        self.steps: List[PipelineStep] = []
        self.processes: List[Process] = []
        self._check_interval = 3  # 状态检查间隔（秒）
        self._is_running = False

        self.construct_steps()
        self.register_metadata()

    def construct_steps(self):
        """构造pipeline的所有steps"""
        steps_num = len(self.steps_config)
        for step_order, step_config in enumerate(self.steps_config, start=1):
            step = PipelineStep.create(
                pipeline_id=self.pipeline_id,
                step_order=step_config.get("step_order"),
                trigger_event=step_config.get("trigger_event"),
                engine_type=step_config.get("engine_type"),
                engine_config=step_config.get("engine_config"),
                operators=step_config.get("operators"),
                meta_config=self.meta_config,
                is_last_step=step_order == steps_num,
            )
            step.meta_registry()
            self.steps.append(step)

    def register_metadata(self):
        """注册pipeline的元数据"""

        pipeline_meta_dict = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_state": None,
            "input_path": self.input_path,
            "output_path": self.output_path,
        }
        step_id_list = [step.step_id for step in self.steps]
        pipeline_meta_dict["steps"] = step_id_list

        self.storage.register_pipeline(self.pipeline_id, pipeline_meta_dict)

    def run(self):
        """运行所有Step"""
        self._is_running = True
        self.processes = []

        # 启动所有Step进程
        for step in self.steps:
            proc = Process(target=self._run_step, args=(step,))
            self.processes.append(proc)
            proc.start()

        # 启动状态监控进程
        monitor_proc = Process(target=self._monitor_steps)
        monitor_proc.start()

        # 等待所有进程完成
        for proc in self.processes:
            proc.join()
        monitor_proc.join()

    def _run_step(self, step: PipelineStep):
        """运行单个Step"""
        try:
            step.meta_registry()
            step.run()
        except Exception as e:
            print(f"Step {step.step_id} failed: {str(e)}")
            self._is_running = False
            raise e

    def _monitor_steps(self):
        """监控所有Step的状态"""
        while self._is_running:
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
                print("Pipeline failed: one or more steps failed")
                self._is_running = False
                break
            elif all_success:
                print("Pipeline completed successfully")
                self._is_running = False
                break

            time.sleep(self._check_interval)

    def stop(self):
        """停止所有Step"""
        self._is_running = False
        for step in self.steps:
            step.stop()

        # 等待所有进程结束
        for proc in self.processes:
            proc.join()

    def resume(self):
        """恢复所有Step"""
        self._is_running = True
        for step in self.steps:
            step.resume()

    def get_status(self) -> Dict:
        """获取Pipeline状态"""
        status = {
            "pipeline_id": self.pipeline_id,
            "is_running": self._is_running,
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
