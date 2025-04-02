import os.path
from autopipe.pipeline.step.base import Step, EngineType
from autopipe.pipeline.operator.get_op import get_operator
from xinghe.utils.json_util import json_loads, json_dumps
from loguru import logger


class LocalCpuBatchStep(Step):
    engine_type = EngineType.LOCAL_CPU_BATCH

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_requested = False  # 停止标志位

    def stop(self):
        """停止本地批处理任务"""
        super().stop()  # 调用父类方法更新状态为 STOPPED
        self._stop_requested = True
        logger.info(f"{self.step_id} 已接收停止信号，正在终止处理...")

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

        if not self.io.exists(self.output_path):
            logger.info(f"创建输出目录: {self.output_path}")
            os.mkdir(self.output_path)

        # 遍历文件
        for filename in self.io.list_dir(self.input_path):
            if self._stop_requested:
                logger.warning(f"{self.step_id} 处理已终止")
                return  # 直接退出方法

            logger.info(filename)
            if not filename.endswith((".jsonl", ".jsonl.gz")):
                continue

            # 构建完整路径（可根据不同IO实现调整路径逻辑）
            input_path = os.path.join(self.input_path, filename)
            output_path = os.path.join(self.output_path, filename)

            # 处理单个文件
            with (
                self.io.read_stream(input_path) as fin,
                self.io.write_stream(output_path) as fout,
            ):
                # print(f"execute {input_path}")
                for line in fin:
                    if self._stop_requested:
                        logger.info(f"{self.step_id} 正在终止当前文件处理...")
                        break  # 终止当前文件处理

                    line = line.strip()
                    # print(line)
                    if not line:
                        continue

                    try:
                        data = json_loads(line)
                        # 执行操作链

                        data = LocalCpuBatchStep.process_row(data, ops)
                        fout.write(json_dumps(data) + "\n")
                    except Exception as e:
                        logger.error(f"处理失败: {input_path} | 错误: {e}")

                else:
                    continue  # 只有 for 循环正常结束（未被 break）才会执行
                break  # 若 for 循环被 break 中断，则执行此处
