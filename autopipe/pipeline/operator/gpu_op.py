from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
from typing import Iterator, Dict, Any, Iterable, Optional, Type, List
from xinghe.ml.actor import ModelActor
from xinghe.s3 import (
    S3DocWriter,
    head_s3_object_with_retry,
    is_s3_path,
    put_s3_object_with_retry,
    read_s3_rows,
    read_s3_object_bytes,
)
from xinghe.utils.json_util import json_loads
from xinghe.dp.base import SkipTask

from typing import Iterable
from xinghe.ops.pdf import PdfProcessor, PageRange, S3Writer
from xinghe.ops.file import RecordHandler
from xinghe.dp.ray import RayTaskExecutor
from xinghe.s3.path import split_s3_path
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
from magic_pdf.data.data_reader_writer import S3DataReader, S3DataWriter

import os


@register_operator
class ModelOperation(BaseOperation, ModelActor):
    # 必须定义 BaseOperation 的抽象属性
    operator_name = "model_operator"
    operator_type = "gpu_model"

    def __init__(
        self,
        # BaseOperation 参数
        params_dict: dict = None,
    ):
        from loguru import logger

        logger.info("start model operation init")
        # 初始化 BaseOperation
        BaseOperation.__init__(self, params_dict)

    # 实现 BaseOperation 的抽象方法
    def process(self, data: dict) -> dict:
        pass

    def resource_load(self):
        pass


def extract_pdf_content(
    d: dict, output_ak: str, output_sk: str, output_endpoint: str, output_path: str
) -> Iterable:
    output_bucket, output_prefix = split_s3_path(output_path)
    s3_img_prefix = os.path.join(output_prefix, "images")

    image_writer = image_writer = S3DataWriter(
        s3_img_prefix, output_bucket, output_ak, output_sk, output_endpoint
    )

    path = d["path"]
    track_id = d["track_id"]

    image_writer = S3Writer(path)

    pdf_bytes = None
    try:
        pdf_bytes = read_s3_object_bytes(path)
    except Exception as e:
        d["_error"] = "read_error"
        d["_error_msg"] = str(e)
        return d

    if pdf_bytes is None:
        d["_error"] = "read_error"
        d["_error_msg"] = "pdf_bytes is None"
        return d

    ds = PymuDocDataset(pdf_bytes)

    if ds.classify() == SupportedPdfParseMethod.OCR:
        infer_result = ds.apply(doc_analyze, ocr=True)

        ## pipeline
        pipe_result = infer_result.pipe_ocr_mode(image_writer)

    else:
        infer_result = ds.apply(doc_analyze, ocr=False)

        ## pipeline
        pipe_result = infer_result.pipe_txt_mode(image_writer)

    pipe_results = []
    pipe_results.append(pipe_result)

    content_list_content = pipe_result.get_content_list(s3_img_prefix)

    d["content_list"] = content_list_content
    return d


@register_operator
class MinerUExtract(BaseOperation):
    """示例算子6"""

    operator_name = "mineru_extract"
    operator_type = "gpu_model"

    def resource_load(self):
        """加载资源"""
        pass

    def process(self, data: dict) -> dict:
        """处理单条数据"""
        pass

    def handle(
        self, data_iter: Iterable[dict], input_file, output_path
    ) -> Iterable:  # 明确声明接收可迭代的字典流
        if not input_file:
            raise SkipTask("missing [input_file]")
        if not is_s3_path(input_file):
            raise SkipTask(f"invalid input_file [{input_file}]")

        input_head = head_s3_object_with_retry(input_file)
        if not input_head:
            raise SkipTask(f"file [{input_file}] not found")

        for d in data_iter:
            # d = json_loads(d.value)
            yield extract_pdf_content(d, output_path)
            

