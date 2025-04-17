from xinghe.ml.model import BertModel
import torch
from typing import Iterator, Dict, Any, Iterable, Optional, Type, List
from autopipe.pipeline.operator.registry import register_operator

class XlmrRegressionModel(BertModel):
    pad_token_id=1  # copied from BertModel and modified

    def __init__(self, config, device=None):
        super().__init__(config, device)
        self.prob_key = self.get_output_key("prob")

    def load_tokenizer(self):
        import logging

        from transformers import XLMRobertaTokenizerFast

        # sometimes, we use this tokenizer to get tail_tokens, so we suppress following warning:
        # `Token indices sequence length is longer than the specified maximum sequence length...`
        logging.getLogger("transformers.tokenization_utils_base").setLevel(logging.ERROR)
        return XLMRobertaTokenizerFast.from_pretrained(self.model_path)

    
    def load_model(self):
        from transformers import XLMRobertaForSequenceClassification
        model = XLMRobertaForSequenceClassification.from_pretrained(self.model_path)
        return self._config_bert_model(model)

    def inference(self, batch:dict) -> List[dict]:
        batch={k: t.to(self.device) for k, t in batch.items()}
        with torch.no_grad():
            outputs=self.model(**batch)
            logits=outputs.logits
            pos_probs = logits.detach().cpu().numpy().clip(min=0, max=1)

            return [{self.prob_key: round(float(p), 6)} for p in pos_probs]
