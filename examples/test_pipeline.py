from multiprocessing import Process
from autopipe.config.base import ConfigLoader
from autopipe.infrastructure.storage import get_storage
from autopipe.pipeline.base import EngineType
from autopipe.pipeline.base import TriggerEvent
from autopipe.pipeline.base import PipelineStep
from autopipe.pipeline.operator.examples import (
    Operation1,
    Operation2,
    Operation3,
    Operation4,
)  # 导入示例算子


def run_step_in_process(
    pipeline_id: str,
    step_order: int,
    trigger_event: str,
    engine_type: EngineType,
    engine_config: dict,
    operators: list,
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


if __name__ == "__main__":
    config = ConfigLoader(
        "C:/Users/chenhaoling/PycharmProjects/autopipe/examples/test_config.json"
    )
    redis_client = get_storage(config.meta_storage)

    pipeline_meta = {
        "pipeline_id": "test_pipeline_202503111212_abcd",
        "pipeline_name": "test_pipeline",
        "pipeline_description": "test_pipeline_202503111212_abcd",
        "pipeline_config": "test_pipeline_202503111212_abcd",
        "pipeline_status": "running",
        "input_path": "D:/downloads/samples/",
        "output_path": "D:/downloads/output/",
        "steps": [
            "test_pipeline_202503111212_abcd_step_1",
            "test_pipeline_202503111212_abcd_step_2",
        ],
    }

    redis_client.register_pipeline(
        pipeline_id="test_pipeline_202503111212_abcd", pipeline_meta=pipeline_meta
    )

    steps_config = [
        {
            "pipeline_id": "test_pipeline_202503111212_abcd",
            "step_order": 1,
            "trigger_event": TriggerEvent.JOB_FINISHED,
            "engine_type": EngineType.LOCAL_CPU_BATCH,
            "engine_config": {"memory": "4g"},
            "operators": [{"name": "op1", "params": {}}, {"name": "op2", "params": {}}],
            "meta_config": config.meta_storage,
            "is_last_step": False,
        },
        {
            "pipeline_id": "test_pipeline_202503111212_abcd",
            "step_order": 2,
            "trigger_event": TriggerEvent.JOB_FINISHED,
            "engine_type": EngineType.LOCAL_CPU_BATCH,
            "engine_config": {"gpu_id": 0},
            "operators": [{"name": "op3", "params": {}}, {"name": "op4", "params": {}}],
            "meta_config": config.meta_storage,
            "is_last_step": True,
        },
    ]

    processes = []
    for config in steps_config:
        proc = Process(target=run_step_in_process, kwargs=config)
        processes.append(proc)
        proc.start()

    # 等待所有子进程完成
    for proc in processes:
        proc.join()
