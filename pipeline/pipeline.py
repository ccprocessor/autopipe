from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pipeline.dataset.base import Dataset
from pipeline.stage.base import BaseStage


# from config


class Pipeline:
    """协调数据处理流程的核心类"""

    def __init__(self, config_path: str):
        """
        Args:
            config: 包含输入路径、输出路径、资源参数和阶段定义的配置
        """
        self.config = Config(config_path)
        self.dataset = self._init_dataset()
        self.stages = self._build_stages()

    def _init_dataset(self) -> Dataset:
        """根据配置初始化数据集"""
        dataset = Dataset(self.config.input_path)
        dataset.set_output_path(self.config.output_path)
        return dataset

    def _build_stages(self) -> List[BaseStage]:
        """根据配置构建所有阶段实例"""
        pass

    def _get_stage_class(self, engine_type: str) -> Type[BaseStage]:
        """根据引擎类型获取对应的 Stage 类（可扩展）"""
        # 示例映射关系，实际根据项目结构调整
        pass

    def run(self, start_from: int = 0) -> None:
        """
        执行流水线中的阶段

        Args:
            start_from: 从指定索引的阶段开始执行（用于断点续跑）
        """
        for stage in self.stages[start_from:]:
            if stage.state != "completed":
                print(f"Running stage {stage.index} ({stage.stage_type})")
                stage.run()

                # 失败时停止流水线
                if stage.state == "failed":
                    raise RuntimeError(
                        f"Stage {stage.index} failed: {stage.dataset.get_stage_info(stage.index).get('error')}"
                    )

    def get_current_progress(self) -> Dict[int, str]:
        """获取当前各阶段状态"""
        return {
            stage.index: stage.state
            for stage in self.stages
        }
