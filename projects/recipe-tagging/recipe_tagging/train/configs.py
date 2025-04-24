from pydantic import BaseModel


class TfidfConfig(BaseModel):
    ngram_range: tuple[int, int] = (1, 1)
    max_df: float = 0.57
    analyzer: str = "word"
    binary: bool = False
    token_pattern: str = r"\w+"
    sublinear_tf: bool = False


class HyperparameterRangeConfig(BaseModel):
    C: tuple[float, float] = (0.1, 1000)
    gamma: tuple[float, float] = (0.0001, 1.0)


class BayesianOptConfig(BaseModel):
    init_points: int = 3
    n_iter: int = 20


class SVCConfig(BaseModel):
    kernel: str = "rbf"
    class_weight: str = "balanced"
    probability: bool = True


class CrossValidationConfig(BaseModel):
    n_splits: int = 3
    scoring: str = "f1_micro"


class ModelConfig(BaseModel):
    tfidf: TfidfConfig = TfidfConfig()
    hyperparam: HyperparameterRangeConfig = HyperparameterRangeConfig()
    bayes_opt: BayesianOptConfig = BayesianOptConfig()
    svc: SVCConfig = SVCConfig()
    cv: CrossValidationConfig = CrossValidationConfig()
