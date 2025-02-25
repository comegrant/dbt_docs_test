from dataclasses import dataclass, field

import numpy as np

from embeddings.interface import Embedder
from embeddings.lazy_imports import (
    sentence_transformers,
)


@dataclass
class SentenceTransformer(Embedder):
    model_name: str
    trust_remote_code: bool = field(default=False)

    def model_version(self) -> str:
        return self.model_name

    def load_model(self) -> sentence_transformers.SentenceTransformer:
        return sentence_transformers.SentenceTransformer(self.model_name, trust_remote_code=self.trust_remote_code)

    async def embed_numpy(self, data: np.ndarray) -> np.ndarray:
        model = self.load_model()
        return model.encode(sentences=data.tolist(), convert_to_numpy=True)
