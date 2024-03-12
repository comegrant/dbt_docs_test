import pandas as pd


class Model:
    def __init__(self, data: pd.DataFrame, config: dict):
        self.data = data
        self.config = config

    def fit(self) -> bool:
        # May not be necessary
        return True

    def predict(self) -> pd.DataFrame:
        return pd.DataFrame()
