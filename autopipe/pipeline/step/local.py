import os.path
from autopipe.pipeline.step.base import Step, EngineType
from autopipe.pipeline.operator.get_op import get_operator
from xinghe.utils.json_util import json_loads, json_dumps
from loguru import logger


class LocalCpuBatchStep(Step):
    engine_type = EngineType.LOCAL_CPU_BATCH

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
