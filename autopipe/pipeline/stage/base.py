from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod
from pipeline.dataset.base import Dataset


class BaseStage(ABC):
    """Stage 基类，定义数据处理阶段的标准流程"""

    # 子类必须定义的参数
    stage_engine: str  # 执行引擎类型，如 "spark"
    stage_type: str  # 阶段类型，如 "batch" 或 "stream"

    def __init_subclass__(cls, ** kwargs):
        """强制子类定义必要属性"""
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "stage_engine"):
            raise TypeError(f"Subclass {cls.__name__} must define 'stage_engine'")
        if not hasattr(cls, "stage_type"):
            raise TypeError(f"Subclass {cls.__name__} must define 'stage_type'")

    def __init__(
            self,
            dataset: Dataset,
            operators: List[Any],
            engine_resources: Dict[str, Any],
            index: int
    ):
        """
        Args:
            dataset: 数据集对象，用于管理输入输出路径和状态
            operators: 本阶段要执行的操作器列表
            engine_resources: 引擎资源配置（如 Spark 并行度、Kafka 地址）
            index: 当前阶段的序号
        """
        self.dataset = dataset
        self.operators = operators
        self.engine_resources = engine_resources
        self.index = index
        self.state: str = "pending"

        # 初始化时注册阶段信息
        self.dataset.update_stage_info({
            "index": self.index,
            "stage_type": self.stage_type,
            "state": self.state,
            "input_path": None,
            "output_path": None
        }, index=self.index)

    def run(self) -> None:
        """执行阶段处理流程"""
        try:
            # 状态更新为运行中
            self._update_state("running")

            # 1. 获取输入数据
            input_data = self._get_input()

            # 2. 应用操作器处理数据
            processed_data = self._process_data(input_data)

            # 3. 确定输出方式
            next_stage_type = self._get_next_stage_type()
            output_path = self._generate_output_path()

            # 4. 写入输出
            self._write_output(processed_data, output_path, next_stage_type)

            # 状态更新为已完成
            self._update_state("completed", output_path=output_path)

        except Exception as e:
            self._update_state("failed", error=str(e))
            raise

    def _get_input(self) -> Any:
        """获取上一个阶段的输出作为输入"""
        if self.index == 0:
            # 第一个阶段从数据集初始路径读取
            return self._load_data(self.dataset.input_path)
        else:
            # 获取前序阶段输出路径
            prev_stage_info = self.dataset.get_stage_info(self.index - 1)
            return self._load_data(prev_stage_info["output_path"])

    def _process_data(self, data: Any) -> Any:
        """应用所有操作器处理数据"""
        for operator in self.operators:
            if hasattr(operator, "process_partition"):
                data = operator.process_partition(data)
            else:
                raise ValueError(f"Invalid operator: {operator.__class__.__name__}")
        return data

    def _get_next_stage_type(self) -> Optional[str]:
        """获取下一个阶段的类型"""
        next_index = self.index + 1
        next_stage_info = self.dataset.get_stage_info(next_index)
        return next_stage_info.get("stage_type") if next_stage_info else None

    def _write_output(self, data: Any, output_path: str, next_stage_type: Optional[str]) -> None:
        """根据下阶段类型决定输出方式"""
        # 所有类型都写入 S3
        self._write_to_s3(data, output_path)

        # 如果下阶段是流处理，额外写入 Kafka
        if next_stage_type == "stream":
            self._write_to_kafka(output_path)

    def _update_state(self, state: str, ** kwargs) -> None:
        """更新阶段状态到数据集"""
        update_data = {"state": state}
        if "output_path" in kwargs:
            update_data["output_path"] = kwargs["output_path"]
        if "error" in kwargs:
            update_data["error"] = kwargs["error"]

        self.dataset.update_stage_info(update_data, index=self.index)
        self.state = state

    # 以下为需要子类实现的引擎相关方法
    @abstractmethod
    def _load_data(self, path: str) -> Any:
        """加载数据（引擎相关实现）"""

    @abstractmethod
    def _write_to_s3(self, data: Any, path: str) -> None:
        """写入 S3（引擎相关实现）"""

    @abstractmethod
    def _write_to_kafka(self, path: str) -> None:
        """向 Kafka 写入文件地址（引擎相关实现）"""

    def _generate_output_path(self) -> str:
        """生成输出路径（可被子类覆盖）"""
        return f"{self.dataset.output_path}/stage_{self.index}"
