import logging

import pytest
from customer_churn.models.train import train_model_locally
from lmkgroup_ds_utils.constants import Companies

from .helpers import generate_training_data

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "company_code",
    [
        Companies.GL.code,
        Companies.LMK.code,
        Companies.AMK.code,
        Companies.RN.code,
    ],
)
def test_model_training(company_code: str) -> None:
    df = generate_training_data()
    train_model_locally(df, company_code=company_code)
