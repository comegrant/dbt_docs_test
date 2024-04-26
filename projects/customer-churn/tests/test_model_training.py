import logging

import pytest
from constants.companies import company_amk, company_gl, company_lmk, company_rt
from customer_churn.data.preprocess import Preprocessor
from customer_churn.models.train import train_model_locally

from .helpers import generate_training_data

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "company_code",
    [
        company_lmk.company_code,
        company_gl.company_code,
        company_amk.company_code,
        company_rt.company_code,
    ],
)
def test_model_training(company_code: str) -> None:
    df = generate_training_data()
    df_x_train, df_y_train, df_x_val, df_y_val = Preprocessor().prep_training_data(
        df,
    )
    train_model_locally(
        data={
            "x_train": df_x_train,
            "y_train": df_y_train,
            "x_val": df_x_val,
            "y_val": df_y_val,
        },
        company_code=company_code,
    )
