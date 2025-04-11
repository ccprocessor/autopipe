from autopipe.pipeline.operator.base import BaseOperation
from autopipe.pipeline.operator.registry import register_operator
import ctypes
import pickle
import os
import math
from collections import Counter
from typing import Callable, Dict, List
import pandas as pd


@register_operator
class CleanModelDemo(BaseOperation):
    operator_name = "clean_model_demo"
    operator_type = "default"
    clean_model = None

    def resource_load(self):
        ctypes.cdll.LoadLibrary("libgomp.so.1")
        self.clean_model = load_clean_model(
            "/share/chenhaojiong/notebooks/common_clean_2/pipline-dev/models/lgb_model_0925.pkl"
        )

    def predict_with_feature(self, feature_dict: dict) -> float:
        feature_df = pd.json_normalize(feature_dict)
        pred = self.clean_model.predict(feature_df)[0]

        return pred

    def pred_clean_prob_fast(self, content: str) -> float:
        # 信息熵
        entropy = stats_entropy(content)

        if entropy <= 1:
            return 0

        features_dict = {
            # "content_len": content_len,
            "lines_num": 0,
            "words_num": 0,
            "word_compression": 0,
            "content_word_len": 0,
            "content_word_frac": 0,
            "num_len": 0,
            "num_frac": 0,
            "space_len": 0,
            "space_frac": 0,
            "punc_len": 0,
            "punc_frac": 0,
            "emoji_len": 0,
            "emoji_frac": 0,
            "punc_end_sentence_num": 0,
            "punc_end_sentence_mean_len": 0,
            "stop_word_num": 0,
            "stop_word_frac": 0,
            # "ellipsis_line_num": ellipsis_line_num,
            "ellipsis_line_frac": 0,
            "enter_frac": 0,
            "max_continue_space_num": 0,
            "html_semi_entity_count": 0,
            "html_semi_entity_frac": 0,
            "url_char_frac": 0,
            "special_char_len": 0,
            "special_char_frac": 0,
            "unique_words_frac": 0,
            "entropy": 0,
            # "js_line_frac": js_line_frac,
            "average_words_per_line": 0,
            "bulletpoint_line_frac": 0,
            "dup_top_2gram": 0,
            # "dup_top_3gram": dup_top_3gram,
            "dup_top_4gram": 0,
            # "dup_5gram": dup_5gram,
            # "dup_6gram": dup_6gram,
            # "dup_7gram": dup_7gram,
            # "dup_8gram": dup_8gram,
            # "dup_9gram": dup_9gram,
            "dup_10gram": 0,
            "std_dev_unicode_value": 0,
            "mean_diff_unicode_value": 0,
        }

        prob = self.predict_with_feature(features_dict)
        return prob

    def process(self, data: dict) -> dict:
        content_list = data.get("content_list")
        content = get_concat_content(content_list)
        data["clean_prob"] = self.pred_clean_prob_fast(content)

        return data


def load_clean_model(model_path: str):
    # from assert
    if model_path.startswith("assets/"):
        pickle_path = get_asset(model_path)
    # absolute path
    else:
        pickle_path = model_path

    with open(pickle_path, "rb") as file:
        clean_model = pickle.load(file)
    print(f"clean_model: {pickle_path}")

    return clean_model


def get_asset(path: str):
    path = path.lstrip("/")
    if path.startswith("assets/"):
        path = path[len("assets/") :]
    assets_dir = get_assets_dir()
    return os.path.join(assets_dir, path)


def get_assets_dir():
    if os.path.isdir("assets"):
        return "assets"
    base_dir = os.getenv("BASE_DIR")
    if base_dir:
        assets_dir = os.path.join(base_dir, "assets")
        if os.path.isdir(assets_dir):
            return assets_dir
    raise Exception("assets dir not found.")


def stats_entropy(content: str) -> float:
    """
    计算文本的熵值（信息熵）
    """

    # 计算每个字符的频数
    freq = Counter(content)
    total_chars = len(content)

    # 计算熵
    entropy = 0.0
    for count in freq.values():
        p_x = div_zero(count, total_chars)
        entropy -= p_x * math.log2(p_x)

    return entropy


def div_zero(a, b):
    """
    避免除零错误
    Args:
        a: float 分子
        b: float 分母
    Returns:
        result: float 除法结果
    """
    if b == 0 and a == 0:
        result = float("nan")
    elif b == 0:
        result = float("inf")
    else:
        result = a / b
    return result


ALL_ITEM_SET = frozenset(
    [
        "text",
        "quote",
        "list",
        "code",
        "table",
        "image",
        "audio",
        "video",
        "hr",
        "equation",
    ]
)
NLP_INCLUDE_SET = frozenset(["text", "quote", "list", "table"])

NLP_EXCLUDE_SET = ALL_ITEM_SET - NLP_INCLUDE_SET

TEXT_INCLUDE_SET = frozenset(["text", "quote", "list", "table", "code", "equation"])

TEXT_EXCLUDE_SET = ALL_ITEM_SET - TEXT_INCLUDE_SET


def get_concat_content(content_list: List[Dict]) -> str:
    content_block_list = []
    for content_item in content_list:
        if content_item["type"] in TEXT_EXCLUDE_SET:
            continue
        content_block_list.append(get_content_item_md(content_item))
    return "\n\n".join(content_block_list)


def get_content_item_md(content_item: dict) -> str:
    return content_item.get("md") or content_item.get("text") or ""
